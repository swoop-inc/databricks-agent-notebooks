"""Databricks cluster operations backed by the Python SDK and config-profile auth."""

from __future__ import annotations

import queue
import re
import threading
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

# Databricks cluster IDs match this pattern: MMDD-HHMMSS-xxxxxxxx
_CLUSTER_ID_RE = re.compile(r"^\d{4}-\d{6}-[a-z0-9]{8}$")
_CLUSTERS_LIST_PATH = "/api/2.1/clusters/list"
_CLUSTERS_PAGE_SIZE = 100
_DEFAULT_RESOLVE_TIMEOUT_SECONDS = 30.0
_DEFAULT_LIST_TIMEOUT_SECONDS = _DEFAULT_RESOLVE_TIMEOUT_SECONDS
_T = TypeVar("_T")


@dataclass(frozen=True)
class Cluster:
    """Immutable snapshot of a Databricks cluster's identity and state."""

    cluster_id: str
    cluster_name: str
    state: str
    spark_version: str


class ClusterError(Exception):
    """User-friendly cluster operation error."""


@dataclass(frozen=True)
class _ClusterPage:
    clusters: tuple[Any, ...]
    next_page_token: str | None


class ClusterService(Protocol):
    """Minimal interface for cluster discovery."""

    def iter_clusters(self, profile: str) -> Iterator[list[Cluster]]: ...

    def resolve_cluster(self, name: str, profile: str) -> Cluster: ...

    def check_available(self) -> bool: ...


def _build_workspace_client(profile: str) -> Any:
    try:
        from databricks.sdk import WorkspaceClient
    except ImportError as exc:
        raise ClusterError(
            "Databricks support is unavailable because the bundled databricks-sdk dependency could not be imported; "
            "reinstall databricks-agent-notebooks."
        ) from exc

    try:
        return WorkspaceClient(profile=profile)
    except Exception as exc:  # pragma: no cover - depends on SDK internals
        raise ClusterError(str(exc)) from exc


class SdkClusterService:
    """Implementation backed by the Databricks Python SDK and config-profile auth."""

    def __init__(
        self,
        *,
        client_factory: Callable[[str], Any] | None = None,
        clock: Callable[[], float] | None = None,
        resolve_timeout_seconds: float = _DEFAULT_RESOLVE_TIMEOUT_SECONDS,
        list_timeout_seconds: float = _DEFAULT_LIST_TIMEOUT_SECONDS,
    ) -> None:
        self._client_factory = client_factory or _build_workspace_client
        self._clock = clock or time.monotonic
        self._resolve_timeout_seconds = resolve_timeout_seconds
        self._list_timeout_seconds = list_timeout_seconds

    def get_cluster(self, cluster_id: str, profile: str) -> Cluster:
        """Fetch a single cluster by exact *cluster_id*."""
        deadline = self._clock() + self._resolve_timeout_seconds
        client = self._build_client(
            profile,
            timeout_seconds=self._resolve_timeout_seconds,
            timeout_context="cluster ID lookup",
        )
        try:
            return _parse_cluster(
                self._call_with_deadline(
                    lambda: client.clusters.get(cluster_id=cluster_id),
                    deadline=deadline,
                    context="cluster ID lookup",
                )
            )
        except ClusterError:
            raise
        except Exception as exc:
            raise ClusterError(str(exc)) from exc

    def iter_clusters(self, profile: str) -> Iterator[list[Cluster]]:
        """Yield clusters page-by-page with the standard list timeout."""
        deadline = self._clock() + self._list_timeout_seconds
        client = self._build_client(
            profile,
            timeout_seconds=self._list_timeout_seconds,
            timeout_context="cluster listing",
        )
        yield from self._iter_cluster_pages(client, deadline=deadline, timeout_context="cluster listing")

    def resolve_cluster(self, name: str, profile: str) -> Cluster:
        """Find a cluster by exact *name* or *cluster ID*."""
        if _CLUSTER_ID_RE.match(name):
            return self.get_cluster(name, profile)

        deadline = self._clock() + self._resolve_timeout_seconds
        client = self._build_client(
            profile,
            timeout_seconds=self._resolve_timeout_seconds,
            timeout_context="cluster name lookup",
        )

        seen_by_name: dict[str, Cluster] = {}

        try:
            for page in self._iter_cluster_pages(client, deadline=deadline):
                for cluster in page:
                    if cluster.cluster_name == name:
                        return cluster
                    seen_by_name.setdefault(cluster.cluster_name, cluster)
        except ClusterError as err:
            raise ClusterError(
                str(err) + _fuzzy_candidate_suffix(name, seen_by_name)
            ) from err

        raise ClusterError(
            f"cluster name {name!r} was not found in the visible cluster pages before pagination exhausted; "
            "exact cluster-name matching is best-effort convenience only. "
            "Pass the cluster ID for deterministic targeting."
            + _fuzzy_candidate_suffix(name, seen_by_name)
        )

    def check_available(self) -> bool:
        """Return ``True`` if the Databricks Python SDK can be imported."""
        try:
            import databricks.sdk  # noqa: F401
        except ImportError:
            return False
        return True

    def _iter_cluster_pages(
        self,
        client: Any,
        *,
        deadline: float | None = None,
        timeout_context: str = "cluster name lookup",
    ) -> Iterator[list[Cluster]]:
        page_token: str | None = None

        while True:
            remaining_seconds = self._remaining_seconds(deadline)
            if remaining_seconds is not None and remaining_seconds <= 0:
                raise self._timeout_error(timeout_context)
            response = self._fetch_cluster_page(
                client,
                page_token=page_token,
                timeout_seconds=remaining_seconds,
                timeout_context=timeout_context,
            )
            yield [_parse_cluster(raw_cluster) for raw_cluster in response.clusters]

            next_page_token = response.next_page_token
            if not next_page_token:
                return
            page_token = str(next_page_token)

    def _fetch_cluster_page(
        self,
        client: Any,
        *,
        page_token: str | None,
        timeout_seconds: float | None,
        timeout_context: str,
    ) -> _ClusterPage:
        if timeout_seconds is None:
            return self._request_cluster_page(client, page_token=page_token)
        if timeout_seconds <= 0:
            raise self._timeout_error(timeout_context)
        return self._call_with_timeout(
            lambda: self._request_cluster_page(client, page_token=page_token),
            timeout_seconds=timeout_seconds,
            context=timeout_context,
        )

    def _build_client(
        self,
        profile: str,
        *,
        timeout_seconds: float,
        timeout_context: str,
    ) -> Any:
        try:
            return self._call_with_timeout(
                lambda: self._client_factory(profile),
                timeout_seconds=timeout_seconds,
                context=timeout_context,
            )
        except ClusterError:
            raise
        except Exception as exc:
            raise ClusterError(str(exc)) from exc

    def _request_cluster_page(self, client: Any, *, page_token: str | None) -> _ClusterPage:
        try:
            from databricks.sdk.service.compute import ListClustersResponse
        except ImportError as exc:
            raise ClusterError(
                "Databricks support is unavailable because the bundled databricks-sdk dependency could not be imported; "
                "reinstall databricks-agent-notebooks."
            ) from exc

        query: dict[str, Any] = {"page_size": _CLUSTERS_PAGE_SIZE}
        if page_token is not None:
            query["page_token"] = page_token
        api_client = _clusters_api_client(client)
        headers = {"Accept": "application/json"}
        workspace_id = getattr(getattr(client, "config", None), "workspace_id", None)
        if workspace_id:
            headers["X-Databricks-Org-Id"] = workspace_id
        try:
            response = api_client.do(
                "GET",
                _CLUSTERS_LIST_PATH,
                query=query,
                headers=headers,
            )
        except ClusterError:
            raise
        except Exception as exc:
            raise ClusterError(str(exc)) from exc
        if not isinstance(response, dict):
            raise ClusterError("Databricks SDK returned an unexpected cluster list response")
        page = ListClustersResponse.from_dict(response)
        return _ClusterPage(
            clusters=tuple(page.clusters or ()),
            next_page_token=page.next_page_token,
        )

    def _call_with_timeout(
        self,
        call: Callable[[], _T],
        *,
        timeout_seconds: float,
        context: str,
    ) -> _T:
        if timeout_seconds <= 0:
            raise self._timeout_error(context)
        result_queue: queue.Queue[tuple[bool, object]] = queue.Queue(maxsize=1)

        def target() -> None:
            try:
                result_queue.put((True, call()))
            except BaseException as exc:  # pragma: no cover - exercised via queue result
                result_queue.put((False, exc))

        threading.Thread(target=target, daemon=True).start()

        try:
            success, value = result_queue.get(timeout=timeout_seconds)
        except queue.Empty as exc:
            raise self._timeout_error(context) from exc

        if success:
            return value  # type: ignore[return-value]
        raise value  # type: ignore[misc]

    def _call_with_deadline(
        self,
        call: Callable[[], _T],
        *,
        deadline: float | None,
        context: str,
    ) -> _T:
        remaining_seconds = self._remaining_seconds(deadline)
        if remaining_seconds is None:
            return call()
        return self._call_with_timeout(call, timeout_seconds=remaining_seconds, context=context)

    def _remaining_seconds(self, deadline: float | None) -> float | None:
        if deadline is None:
            return None
        return max(deadline - self._clock(), 0.0)

    def _timeout_error(self, context: str) -> ClusterError:
        timeout_seconds = self._timeout_seconds_for_context(context)
        if context == "cluster listing":
            return ClusterError(
                f"cluster listing did not complete within {timeout_seconds:.1f} seconds of paginated listing"
            )
        if context == "cluster ID lookup":
            return ClusterError(
                f"cluster ID lookup did not resolve within {timeout_seconds:.1f} seconds"
            )
        return ClusterError(
            "cluster name lookup did not resolve within "
            f"{timeout_seconds:.1f} seconds of paginated listing; "
            "exact cluster-name matching is best-effort convenience only. "
            "Pass the cluster ID for deterministic targeting."
        )

    def _timeout_seconds_for_context(self, context: str) -> float:
        if context == "cluster listing":
            return self._list_timeout_seconds
        return self._resolve_timeout_seconds


def default_service() -> ClusterService:
    """Return the standard SDK-backed cluster service."""
    return SdkClusterService()


def _parse_cluster(data: Any) -> Cluster:
    return Cluster(
        cluster_id=_stringify_required_field(data, "cluster_id"),
        cluster_name=_stringify_required_field(data, "cluster_name"),
        state=_stringify_optional_field(data, "state", default="UNKNOWN"),
        spark_version=_stringify_optional_field(data, "spark_version", default=""),
    )


def _stringify_required_field(data: Any, field: str) -> str:
    value = _read_field(data, field)
    if value is None or value == "":
        raise ClusterError(f"Databricks cluster metadata did not include {field!r}")
    return _stringify_field(value)


def _stringify_optional_field(data: Any, field: str, *, default: str) -> str:
    value = _read_field(data, field)
    if value is None:
        return default
    return _stringify_field(value)


def _read_field(data: Any, field: str) -> Any:
    if isinstance(data, dict):
        return data.get(field)
    return getattr(data, field, None)


def _stringify_field(value: Any) -> str:
    if hasattr(value, "value"):
        value = value.value
    return str(value)


def _fuzzy_candidate_suffix(
    name: str,
    seen_by_name: dict[str, Cluster],
    *,
    limit: int = 5,
    score_cutoff: float = 40.0,
) -> str:
    """Return a suffix with fuzzy-matched cluster suggestions, or ``""``."""
    if not seen_by_name:
        return ""
    try:
        from rapidfuzz import fuzz, process
    except ImportError:
        return ""
    names = list(seen_by_name.keys())
    matches = process.extract(name, names, scorer=fuzz.WRatio, limit=limit, score_cutoff=score_cutoff)
    if not matches:
        return ""
    lines = ["\n\nSimilar clusters:"]
    for matched_name, _score, _idx in matches:
        c = seen_by_name[matched_name]
        lines.append(f'  - "{c.cluster_name}" ({c.state}, ID: {c.cluster_id})')
    return "\n".join(lines)


def _clusters_api_client(client: Any) -> Any:
    api_client = getattr(client, "api_client", None)
    if api_client is None:
        raise ClusterError("Databricks SDK client did not expose a clusters API client")
    return api_client


def parse_spark_version_line(spark_version: str) -> str:
    """Extract the Databricks Runtime major.minor line from cluster metadata."""
    match = re.search(r"(?P<major>\d+)\.(?P<minor>\d+)", spark_version)
    if match is None:
        raise ClusterError(f"Unable to determine Databricks Runtime line from spark_version {spark_version!r}")
    return f"{int(match.group('major'))}.{int(match.group('minor'))}"


def resolve_cluster_databricks_line(cluster: Cluster) -> str:
    """Resolve the normalized Databricks Runtime line from cluster metadata."""
    spark_version = cluster.spark_version.strip()
    if not spark_version or spark_version.upper() == "UNKNOWN":
        raise ClusterError(
            f"Unable to determine Databricks Runtime line for cluster {cluster.cluster_id}: spark_version is unavailable"
        )
    return parse_spark_version_line(spark_version)
