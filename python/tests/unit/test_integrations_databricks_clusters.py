"""Tests for Databricks cluster discovery."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from databricks_agent_notebooks.integrations.databricks.clusters import (
    Cluster,
    ClusterError,
    SdkClusterService,
    _build_workspace_client,
    _fuzzy_candidate_suffix,
    parse_spark_version_line,
    resolve_cluster_databricks_line,
)


_FIRST_CLUSTER = {
    "cluster_id": "1003-184738-wkj97rxa",
    "cluster_name": "rnd-alpha",
    "state": "RUNNING",
    "spark_version": "16.4.x-scala2.12",
}

_SECOND_CLUSTER = {
    "cluster_id": "2004-295849-xyz12abc",
    "cluster_name": "rnd-beta",
    "state": "TERMINATED",
    "spark_version": "15.3.x-scala2.12",
}


class FakeClock:
    def __init__(self, *values: float):
        self._values = list(values) or [0.0]
        self._index = 0

    def __call__(self) -> float:
        if self._index >= len(self._values):
            return self._values[-1]
        value = self._values[self._index]
        self._index += 1
        return value


class FakeApiClient:
    def __init__(
        self,
        pages: dict[str | None, dict],
        *,
        workspace_id: str | None = None,
        expose_private_cfg: bool = True,
    ):
        self._pages = pages
        self.calls: list[tuple[str, str, dict[str, object]]] = []
        self.headers: list[dict[str, str]] = []
        if expose_private_cfg:
            self._cfg = SimpleNamespace(workspace_id=workspace_id)

    def do(self, method: str, path: str, *, query: dict | None = None, headers: dict | None = None) -> dict:
        request_query = dict(query or {})
        self.calls.append((method, path, request_query))
        self.headers.append(dict(headers or {}))
        page_token = request_query.get("page_token")
        if page_token not in self._pages:
            raise AssertionError(f"unexpected page token {page_token!r}")
        return self._pages[page_token]


class BlockingFakeApiClient(FakeApiClient):
    def __init__(self, pages: dict[str | None, dict], *, release_event: threading.Event):
        super().__init__(pages)
        self.release_event = release_event
        self.started = threading.Event()

    def do(self, method: str, path: str, *, query: dict | None = None, headers: dict | None = None) -> dict:
        request_query = dict(query or {})
        self.calls.append((method, path, request_query))
        self.headers.append(dict(headers or {}))
        self.started.set()
        if not self.release_event.wait(timeout=1.0):
            raise AssertionError("test did not release the blocking fake API client")
        page_token = request_query.get("page_token")
        if page_token not in self._pages:
            raise AssertionError(f"unexpected page token {page_token!r}")
        return self._pages[page_token]


class FakeClustersAPI:
    def __init__(
        self,
        api_client: FakeApiClient,
        *,
        get_result: object | None = None,
        get_error: Exception | None = None,
        expose_private_api: bool = True,
    ):
        if expose_private_api:
            self._api = api_client
        self._get_result = get_result
        self._get_error = get_error
        self.get_calls: list[str] = []

    def get(self, *, cluster_id: str) -> object:
        self.get_calls.append(cluster_id)
        if self._get_error is not None:
            raise self._get_error
        if self._get_result is None:
            raise AssertionError("unexpected clusters.get call without a configured result")
        return self._get_result


class BlockingFakeClustersAPI(FakeClustersAPI):
    def __init__(
        self,
        api_client: FakeApiClient,
        *,
        get_result: object | None = None,
        get_error: Exception | None = None,
        release_event: threading.Event,
    ):
        super().__init__(api_client, get_result=get_result, get_error=get_error)
        self.release_event = release_event
        self.started = threading.Event()

    def get(self, *, cluster_id: str) -> object:
        self.get_calls.append(cluster_id)
        self.started.set()
        if not self.release_event.wait(timeout=1.0):
            raise AssertionError("test did not release the blocking fake clusters API")
        if self._get_error is not None:
            raise self._get_error
        if self._get_result is None:
            raise AssertionError("unexpected clusters.get call without a configured result")
        return self._get_result


class BlockingClientFactory:
    def __init__(self, client: FakeWorkspaceClient, *, release_event: threading.Event):
        self._client = client
        self.release_event = release_event
        self.started = threading.Event()
        self.calls: list[str] = []

    def __call__(self, profile: str) -> FakeWorkspaceClient:
        self.calls.append(profile)
        self.started.set()
        if not self.release_event.wait(timeout=1.0):
            raise AssertionError("test did not release the blocking client factory")
        return self._client


class FakeWorkspaceClient:
    def __init__(
        self,
        *,
        pages: dict[str | None, dict] | None = None,
        get_result: object | None = None,
        get_error: Exception | None = None,
        workspace_id: str | None = None,
        expose_private_api: bool = True,
        expose_private_cfg: bool = True,
    ):
        self.api_client = FakeApiClient(
            pages or {},
            workspace_id=workspace_id,
            expose_private_cfg=expose_private_cfg,
        )
        self.config = SimpleNamespace(workspace_id=workspace_id)
        self.clusters = FakeClustersAPI(
            self.api_client,
            get_result=SimpleNamespace(**get_result) if isinstance(get_result, dict) else get_result,
            get_error=get_error,
            expose_private_api=expose_private_api,
        )


class FakeEnum:
    def __init__(self, value: str):
        self.value = value


def _make_service(
    client: FakeWorkspaceClient,
    *,
    clock: Callable[[], float] | None = None,
    resolve_timeout_seconds: float = 30.0,
    list_timeout_seconds: float = 30.0,
) -> SdkClusterService:
    return SdkClusterService(
        client_factory=lambda profile: client,
        clock=clock or FakeClock(0.0),
        resolve_timeout_seconds=resolve_timeout_seconds,
        list_timeout_seconds=list_timeout_seconds,
    )


def _run_in_background(call: Callable[[], object]) -> tuple[threading.Thread, threading.Event, dict[str, object]]:
    done = threading.Event()
    outcome: dict[str, object] = {}

    def target() -> None:
        try:
            outcome["value"] = call()
        except BaseException as exc:  # pragma: no cover - asserted by tests
            outcome["error"] = exc
        finally:
            done.set()

    thread = threading.Thread(target=target)
    thread.start()
    return thread, done, outcome


def test_resolve_cluster_id_uses_clusters_get_and_returns_cluster_metadata() -> None:
    client = FakeWorkspaceClient(get_result=_FIRST_CLUSTER)
    service = _make_service(client)

    cluster = service.resolve_cluster("1003-184738-wkj97rxa", "dev")

    assert client.clusters.get_calls == ["1003-184738-wkj97rxa"]
    assert client.api_client.calls == []
    assert cluster == Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")


def test_resolve_cluster_id_times_out_while_building_client() -> None:
    release_event = threading.Event()
    factory = BlockingClientFactory(FakeWorkspaceClient(get_result=_FIRST_CLUSTER), release_event=release_event)
    service = SdkClusterService(
        client_factory=factory,
        clock=time.monotonic,
        resolve_timeout_seconds=0.05,
    )

    thread, done, outcome = _run_in_background(lambda: service.resolve_cluster("1003-184738-wkj97rxa", "dev"))

    assert factory.started.wait(timeout=0.5)
    try:
        assert done.wait(timeout=0.5), "resolve_cluster did not return within the caller timeout budget"
    finally:
        release_event.set()
        thread.join(timeout=1.0)

    assert isinstance(outcome.get("error"), ClusterError)
    assert "cluster ID lookup did not resolve within 0.1 seconds" in str(outcome["error"])
    assert factory.calls == ["dev"]


def test_resolve_cluster_id_times_out_while_waiting_for_direct_lookup() -> None:
    release_event = threading.Event()
    client = FakeWorkspaceClient(pages={})
    client.clusters = BlockingFakeClustersAPI(
        client.api_client,
        get_result=SimpleNamespace(**_FIRST_CLUSTER),
        release_event=release_event,
    )
    service = _make_service(client, clock=time.monotonic, resolve_timeout_seconds=0.05)

    thread, done, outcome = _run_in_background(lambda: service.resolve_cluster("1003-184738-wkj97rxa", "dev"))

    assert client.clusters.started.wait(timeout=0.5)
    try:
        assert done.wait(timeout=0.5), "resolve_cluster did not return within the caller timeout budget"
    finally:
        release_event.set()
        thread.join(timeout=1.0)

    assert isinstance(outcome.get("error"), ClusterError)
    assert "cluster ID lookup did not resolve within 0.1 seconds" in str(outcome["error"])
    assert client.clusters.get_calls == ["1003-184738-wkj97rxa"]
    assert client.api_client.calls == []


def test_resolve_cluster_id_surfaces_direct_lookup_errors() -> None:
    client = FakeWorkspaceClient(get_error=RuntimeError("cluster not found"))
    service = _make_service(client)

    with pytest.raises(ClusterError, match="cluster not found"):
        service.resolve_cluster("1003-184738-wkj97rxa", "dev")


def test_resolve_cluster_name_returns_exact_match_from_first_page() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_FIRST_CLUSTER, _SECOND_CLUSTER], "next_page_token": "unused-page"},
        }
    )
    service = _make_service(client)

    cluster = service.resolve_cluster("rnd-alpha", "dev")

    assert cluster.cluster_id == "1003-184738-wkj97rxa"
    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]


def test_resolve_cluster_name_times_out_while_building_client() -> None:
    release_event = threading.Event()
    factory = BlockingClientFactory(
        FakeWorkspaceClient(pages={None: {"clusters": [_FIRST_CLUSTER]}}),
        release_event=release_event,
    )
    service = SdkClusterService(
        client_factory=factory,
        clock=time.monotonic,
        resolve_timeout_seconds=0.05,
    )

    thread, done, outcome = _run_in_background(lambda: service.resolve_cluster("rnd-alpha", "dev"))

    assert factory.started.wait(timeout=0.5)
    try:
        assert done.wait(timeout=0.5), "resolve_cluster did not return within the caller timeout budget"
    finally:
        release_event.set()
        thread.join(timeout=1.0)

    assert isinstance(outcome.get("error"), ClusterError)
    assert "did not resolve within 0.1 seconds" in str(outcome["error"])
    assert factory.calls == ["dev"]


def test_iter_clusters_adds_workspace_header_when_sdk_config_exposes_workspace_id() -> None:
    client = FakeWorkspaceClient(
        pages={None: {"clusters": [_FIRST_CLUSTER]}},
        workspace_id="1234567890",
    )
    service = _make_service(client)

    pages = list(service.iter_clusters("dev"))

    assert pages == [[Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")]]
    assert client.api_client.headers == [{"Accept": "application/json", "X-Databricks-Org-Id": "1234567890"}]


def test_iter_clusters_uses_public_workspace_client_surfaces_when_private_sdk_fields_are_absent() -> None:
    client = FakeWorkspaceClient(
        pages={None: {"clusters": [_FIRST_CLUSTER]}},
        workspace_id="1234567890",
        expose_private_api=False,
        expose_private_cfg=False,
    )
    service = _make_service(client)

    pages = list(service.iter_clusters("dev"))

    assert pages == [[Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")]]
    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]
    assert client.api_client.headers == [{"Accept": "application/json", "X-Databricks-Org-Id": "1234567890"}]


def test_resolve_cluster_name_returns_exact_match_from_later_page() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_SECOND_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": [_FIRST_CLUSTER]},
        }
    )
    service = _make_service(client)

    cluster = service.resolve_cluster("rnd-alpha", "dev")

    assert cluster.cluster_id == "1003-184738-wkj97rxa"
    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
        ("GET", "/api/2.1/clusters/list", {"page_size": 100, "page_token": "page-2"}),
    ]


def test_resolve_cluster_name_times_out_before_match() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_SECOND_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": [_FIRST_CLUSTER]},
        }
    )
    service = _make_service(client, clock=FakeClock(0.0, 0.0, 31.0))

    with pytest.raises(
        ClusterError,
        match="did not resolve within 30.0 seconds.*cluster ID for deterministic targeting",
    ):
        service.resolve_cluster("rnd-alpha", "dev")

    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]


def test_resolve_cluster_name_times_out_while_waiting_for_slow_page_fetch() -> None:
    release_event = threading.Event()
    blocking_api_client = BlockingFakeApiClient(
        {
            None: {
                "clusters": [
                    _SECOND_CLUSTER,
                    _FIRST_CLUSTER,
                ]
            }
        },
        release_event=release_event,
    )
    client = FakeWorkspaceClient(pages={})
    client.api_client = blocking_api_client
    client.clusters = FakeClustersAPI(blocking_api_client)
    service = _make_service(client, clock=time.monotonic, resolve_timeout_seconds=0.05)

    thread, done, outcome = _run_in_background(lambda: service.resolve_cluster("rnd-alpha", "dev"))

    assert blocking_api_client.started.wait(timeout=0.5)
    try:
        assert done.wait(timeout=0.5), "resolve_cluster did not return within the caller timeout budget"
    finally:
        release_event.set()
        thread.join(timeout=1.0)

    assert isinstance(outcome.get("error"), ClusterError)
    assert "did not resolve within 0.1 seconds" in str(outcome["error"])
    assert blocking_api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]


def test_iter_clusters_yields_pages_incrementally() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_FIRST_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": [_SECOND_CLUSTER]},
        }
    )
    service = _make_service(client)

    pages = service.iter_clusters("dev")
    first_page = next(pages)

    assert first_page == [Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")]
    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]

    second_page = next(pages)
    assert second_page == [Cluster("2004-295849-xyz12abc", "rnd-beta", "TERMINATED", "15.3.x-scala2.12")]
    assert len(client.api_client.calls) == 2


def test_iter_clusters_respects_global_deadline() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_FIRST_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": [_SECOND_CLUSTER]},
        }
    )
    service = _make_service(client, clock=FakeClock(0.0, 0.0, 31.0))

    pages = service.iter_clusters("dev")
    first_page = next(pages)
    assert first_page == [Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")]

    with pytest.raises(ClusterError, match="cluster listing did not complete within 30.0 seconds"):
        next(pages)

    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
    ]


def test_iter_clusters_empty_yields_empty_list() -> None:
    client = FakeWorkspaceClient(pages={None: {"clusters": []}})
    service = _make_service(client)

    pages = list(service.iter_clusters("dev"))

    assert pages == [[]]


def test_iter_clusters_client_build_timeout() -> None:
    release_event = threading.Event()
    factory = BlockingClientFactory(
        FakeWorkspaceClient(pages={None: {"clusters": [_FIRST_CLUSTER]}}),
        release_event=release_event,
    )
    service = SdkClusterService(
        client_factory=factory,
        clock=time.monotonic,
        list_timeout_seconds=0.05,
    )

    def consume():
        return list(service.iter_clusters("dev"))

    thread, done, outcome = _run_in_background(consume)

    assert factory.started.wait(timeout=0.5)
    try:
        assert done.wait(timeout=0.5), "iter_clusters did not return within the caller timeout budget"
    finally:
        release_event.set()
        thread.join(timeout=1.0)

    assert isinstance(outcome.get("error"), ClusterError)
    assert "cluster listing did not complete within 0.1 seconds" in str(outcome["error"])
    assert factory.calls == ["dev"]


def test_resolve_cluster_name_fails_after_visible_pages_exhaust() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_SECOND_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": []},
        }
    )
    service = _make_service(client)

    with pytest.raises(
        ClusterError,
        match="was not found in the visible cluster pages.*cluster ID for deterministic targeting",
    ):
        service.resolve_cluster("rnd-alpha", "dev")

    assert client.api_client.calls == [
        ("GET", "/api/2.1/clusters/list", {"page_size": 100}),
        ("GET", "/api/2.1/clusters/list", {"page_size": 100, "page_token": "page-2"}),
    ]


def test_resolve_cluster_name_duplicate_exact_names_returns_first_visible_match() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {
                "clusters": [
                    _SECOND_CLUSTER,
                    _FIRST_CLUSTER,
                    {
                        "cluster_id": "3005-010101-dupaaaaa",
                        "cluster_name": "rnd-alpha",
                        "state": "RUNNING",
                        "spark_version": "14.3.x-scala2.12",
                    },
                ]
            }
        }
    )
    service = _make_service(client)

    cluster = service.resolve_cluster("rnd-alpha", "dev")

    assert cluster.cluster_id == "1003-184738-wkj97rxa"


def test_resolve_cluster_name_requires_exact_match_without_substring_fallback() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {
                "clusters": [
                    {**_FIRST_CLUSTER, "cluster_name": "rnd-alpha [engineering]"},
                    {**_SECOND_CLUSTER, "cluster_name": "rnd-beta [adhoc]"},
                ]
            }
        }
    )
    service = _make_service(client)

    with pytest.raises(
        ClusterError,
        match="cluster name 'rnd-alpha'.*exact cluster-name matching is best-effort convenience only",
    ):
        service.resolve_cluster("rnd-alpha", "dev")


def test_resolve_cluster_id_unwraps_sdk_enum_values() -> None:
    client = FakeWorkspaceClient(
        get_result=SimpleNamespace(
            cluster_id="1003-184738-wkj97rxa",
            cluster_name="rnd-alpha",
            state=FakeEnum("RUNNING"),
            spark_version="16.4.x-scala2.12",
        )
    )
    service = _make_service(client)

    cluster = service.resolve_cluster("1003-184738-wkj97rxa", "dev")

    assert cluster == Cluster("1003-184738-wkj97rxa", "rnd-alpha", "RUNNING", "16.4.x-scala2.12")


def test_build_workspace_client_only_passes_profile_to_workspace_client() -> None:
    recorded_kwargs: dict[str, object] = {}

    def fake_workspace_client(**kwargs):
        recorded_kwargs.update(kwargs)
        return object()

    with patch("databricks.sdk.WorkspaceClient", side_effect=fake_workspace_client):
        _build_workspace_client("prod")

    assert recorded_kwargs == {"profile": "prod"}


@pytest.mark.parametrize(
    ("spark_version", "expected_line"),
    [
        ("16.4.x-scala2.12", "16.4"),
        ("15.3.x-photon-scala2.12", "15.3"),
        ("14.3.x-gpu-ml-scala2.12", "14.3"),
    ],
)
def test_parse_spark_version_line_extracts_major_minor(spark_version: str, expected_line: str) -> None:
    assert parse_spark_version_line(spark_version) == expected_line


def test_resolve_cluster_databricks_line_uses_cluster_metadata() -> None:
    cluster = Cluster(
        cluster_id="1003-184738-wkj97rxa",
        cluster_name="rnd-alpha",
        state="RUNNING",
        spark_version="16.4.x-scala2.12",
    )

    assert resolve_cluster_databricks_line(cluster) == "16.4"


def test_resolve_cluster_databricks_line_fails_for_unknown_metadata() -> None:
    cluster = Cluster(
        cluster_id="1003-184738-wkj97rxa",
        cluster_name="rnd-alpha",
        state="RUNNING",
        spark_version="UNKNOWN",
    )

    with pytest.raises(ClusterError, match="Unable to determine Databricks Runtime line"):
        resolve_cluster_databricks_line(cluster)


# ── Fuzzy cluster name suggestion tests ─────────────────────────────────


def test_resolve_name_exhaustion_includes_fuzzy_suggestions() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_FIRST_CLUSTER, _SECOND_CLUSTER]},
        }
    )
    service = _make_service(client)

    with pytest.raises(ClusterError) as exc_info:
        service.resolve_cluster("rnd-alphha", "dev")

    msg = str(exc_info.value)
    assert "was not found in the visible cluster pages" in msg
    assert "Similar clusters:" in msg
    assert '"rnd-alpha"' in msg
    assert "RUNNING" in msg
    assert "1003-184738-wkj97rxa" in msg


def test_resolve_name_timeout_includes_fuzzy_suggestions() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {"clusters": [_FIRST_CLUSTER, _SECOND_CLUSTER], "next_page_token": "page-2"},
            "page-2": {"clusters": []},
        }
    )
    service = _make_service(client, clock=FakeClock(0.0, 0.0, 31.0))

    with pytest.raises(ClusterError) as exc_info:
        service.resolve_cluster("rnd-alphha", "dev")

    msg = str(exc_info.value)
    assert "did not resolve within 30.0 seconds" in msg
    assert "Similar clusters:" in msg
    assert '"rnd-alpha"' in msg


def test_resolve_name_exhaustion_no_similar_clusters() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {
                "clusters": [
                    {**_FIRST_CLUSTER, "cluster_name": "production-etl-main"},
                    {**_SECOND_CLUSTER, "cluster_name": "analytics-warehouse"},
                ]
            },
        }
    )
    service = _make_service(client)

    with pytest.raises(ClusterError) as exc_info:
        service.resolve_cluster("zzz-totally-different", "dev")

    msg = str(exc_info.value)
    assert "was not found" in msg
    assert "Similar clusters:" not in msg


def test_resolve_name_deduplicates_same_name_clusters() -> None:
    client = FakeWorkspaceClient(
        pages={
            None: {
                "clusters": [
                    {**_FIRST_CLUSTER, "cluster_name": "shared-cluster"},
                    {**_SECOND_CLUSTER, "cluster_name": "shared-cluster"},
                ]
            },
        }
    )
    service = _make_service(client)

    with pytest.raises(ClusterError) as exc_info:
        service.resolve_cluster("shared-clster", "dev")

    msg = str(exc_info.value)
    assert "Similar clusters:" in msg
    assert msg.count('"shared-cluster"') == 1


def test_fuzzy_candidate_suffix_empty_for_no_clusters() -> None:
    assert _fuzzy_candidate_suffix("anything", {}) == ""
