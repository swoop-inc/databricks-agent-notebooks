# Code Review: Streaming Clusters CLI Output (02ca561)

## Summary

The change adds `iter_clusters()` to both the `ClusterService` Protocol and `SdkClusterService`, yielding clusters page-by-page under the same 30s global deadline that `list_clusters` used. `list_clusters()` is redefined as a thin wrapper over `iter_clusters()`. The CLI handler `_cmd_clusters()` is rewritten to stream rows with `flush=True` and deferred header printing.

**Overall assessment:** Well-executed change. The intent is clearly communicated in the commit message and the implementation matches it. The refactoring is clean -- the new `iter_clusters` extracts the deadline/client-setup concern that was previously buried inside `list_clusters`, and `list_clusters` becomes a trivial accumulator. The streaming CLI handler gives users immediate feedback and shows partial results on timeout.

## What Was Done Well

1. **Clean decomposition.** `iter_clusters` owns the deadline and client setup; `list_clusters` is a one-liner wrapper. No code duplication.

2. **Protocol updated in sync.** The `ClusterService` Protocol got `iter_clusters` at the same time as the concrete class, so downstream typing consumers stay in sync.

3. **Streaming correctness.** The CLI handler defers the header until the first cluster arrives, prints each row with `flush=True`, and catches `ClusterError` around the entire iteration so partial output survives a mid-stream timeout.

4. **Test coverage.** Four new `iter_clusters` tests (incremental yields, deadline enforcement, empty page, client-build timeout) plus three CLI tests (happy path with `iter_clusters`, empty result, mid-stream error with partial output). The mid-stream error test is particularly valuable -- it validates the core user-facing benefit of the change.

5. **Backward compatibility preserved.** `list_clusters` still works identically from the caller's perspective. Existing tests for `list_clusters` pass unchanged.

---

## Issues

### Important (should fix)

#### 1. CLI empty-page detection is fragile: an API page with zero clusters looks like "no clusters"

**Problem:** The `_cmd_clusters` handler uses `header_printed` as a proxy for "any clusters exist." But `_iter_cluster_pages` always yields at least one page even when `response.clusters` is empty (line 173 in `clusters.py`). The CLI iterates through clusters in each page, so an empty-page yield produces zero inner-loop iterations and `header_printed` stays `False`.

This means: if the API returns a page with `clusters: []` and `next_page_token: "page-2"`, then a second page with actual clusters, the second page's clusters print fine and the header is printed. So the behavior is *correct* -- but only by accident of the inner-loop structure. The real concern is a different edge case: the API returns a single page with zero clusters. In that case `iter_clusters` yields `[[]]` (confirmed by `test_iter_clusters_empty_yields_empty_list`), the CLI prints "No clusters found." to stderr, and returns 0. This is defensible behavior.

However, there is a subtle contract issue: `iter_clusters` always yields at least one page (even when empty), whereas a consumer might reasonably expect zero yields for zero clusters. The `list_clusters` wrapper handles this fine because `extend([])` is a no-op, but other future consumers of `iter_clusters` might be surprised.

**Verdict:** This is not a bug today. But it is a documentation gap. The docstring for `iter_clusters` says "Yield clusters page-by-page" but does not state the invariant that at least one page is always yielded (even if empty).

**Suggested fix (docstring clarification):**

```python
def iter_clusters(self, profile: str) -> Iterator[list[Cluster]]:
    """Yield clusters page-by-page with the standard list timeout.

    Always yields at least one page. An empty workspace yields a single
    empty list.  Raises *ClusterError* on timeout or API failure.
    """
```

And the Protocol method:

```python
def iter_clusters(self, profile: str) -> Iterator[list[Cluster]]:
    """Yield clusters page-by-page. Always yields at least one page."""
    ...
```

---

#### 2. `list_clusters` docstring is now misleading about implementation

**Problem:** The `list_clusters` docstring reads "List all clusters visible to *profile*." This is fine as a contract description, but after the refactoring it no longer sets up its own deadline or client -- it delegates entirely to `iter_clusters`. If someone reads the source to understand the timeout behavior, they might think `list_clusters` has independent deadline logic. It does not; it inherits `iter_clusters`'s deadline.

**Suggested fix:** No change needed to the docstring text itself -- "List all clusters" is correct as an API contract. But consider adding a one-line note:

```python
def list_clusters(self, profile: str) -> list[Cluster]:
    """List all clusters visible to *profile*.

    Convenience wrapper over :meth:`iter_clusters` that accumulates all pages.
    """
```

This documents the delegation relationship for readers of the source.

---

### Suggestions (nice to have)

#### 3. `CliClusterService` alias is dead code

**Problem:** Line 320 defines `CliClusterService = SdkClusterService`, but nothing imports it (confirmed by grep). This predates the current change but is worth noting.

**Suggested fix:** Either remove the alias or add a comment explaining its purpose (e.g., if it's part of a public API surface for downstream consumers).

---

#### 4. No test for `iter_clusters` with multiple non-empty pages verifying exact yield sequence

**Problem:** `test_iter_clusters_yields_pages_incrementally` manually calls `next()` twice and verifies the content of each page. This is good. But there is no test that calls `list(service.iter_clusters("dev"))` on a multi-page scenario and checks that pages are returned in order as separate lists (i.e., `[[cluster_a], [cluster_b]]` rather than `[[cluster_a, cluster_b]]`). The existing `test_list_clusters_pages_incrementally_with_page_size_100` indirectly validates this through `list_clusters`, but a direct `iter_clusters` test would be slightly more explicit.

**Verdict:** Low priority. The manual `next()` test is actually stronger because it verifies lazy evaluation between pages (the second API call only happens after the first `next()` returns). This is a suggestion for completeness, not a gap.

---

#### 5. CLI header width is hard-coded and could truncate long cluster names

**Problem:** The CLI formats cluster names with `{c.cluster_name:<40}`. Cluster names longer than 40 characters will overflow into the STATE column, misaligning the output. This is a pre-existing issue (the format string existed before this change), but the streaming rewrite carries it forward.

**Suggested fix (if addressed in a future pass):**

```python
# Two-pass approach is incompatible with streaming, so either:
# (a) Accept the truncation and document it
# (b) Use a wider column (e.g., 60)
# (c) Use tab-separated output for machine parsing
```

Not actionable for this change -- just noting it.

---

#### 6. The `flush=True` is on the data rows but not on the header

**Problem:** Lines 328-329 print the header and separator without `flush=True`, but line 331 prints each data row with `flush=True`. In practice this does not matter because the very next statement after the header is a `print(..., flush=True)` which will flush the buffer. But for consistency:

```python
if not header_printed:
    print(f"{'NAME':<40} {'STATE':<12} {'ID'}", flush=True)
    print("-" * 80, flush=True)
    header_printed = True
print(f"{c.cluster_name:<40} {c.state:<12} {c.cluster_id}", flush=True)
```

This is purely cosmetic -- the first data row's flush will push the header out regardless.

---

## Protocol Backward Compatibility

Adding `iter_clusters` to the `ClusterService` Protocol is a **breaking change** for any class that structurally conforms to the old Protocol but does not implement `iter_clusters`. However:

1. No code outside of `clusters.py` implements `ClusterService` (confirmed by grep).
2. The CLI's `_cmd_clusters` now calls `iter_clusters` directly on the return value of `default_service()`, which always returns `SdkClusterService` -- so the Protocol expansion is safe.
3. `CliClusterService` is an alias for `SdkClusterService`, so it automatically has `iter_clusters`.

**Verdict:** No compatibility risk in the current codebase. If external consumers exist (published package), this would be a minor version bump. Within the repo, safe.

## Global Deadline Behavior

The deadline logic is correctly preserved:

- `iter_clusters` sets `deadline = self._clock() + self._list_timeout_seconds` once, then passes it through to `_iter_cluster_pages`, which checks `self._remaining_seconds(deadline)` before each page fetch.
- `list_clusters` delegates entirely to `iter_clusters`, inheriting its deadline. Previously `list_clusters` set its own deadline with the same formula -- the behavior is identical.
- The FakeClock-based tests (`test_iter_clusters_respects_global_deadline`) correctly simulate time advancing past the deadline between page fetches.

No issues found.

## Streaming Correctness

- `flush=True` ensures each row is visible to the terminal immediately, even if stdout is block-buffered (e.g., piped to another process).
- The `try/except ClusterError` wraps the entire `for page in service.iter_clusters(...)` loop, so a timeout mid-pagination prints the error to stderr and returns 1 -- but any rows already printed remain on stdout. This is confirmed by `test_clusters_command_error_mid_stream_shows_partial_output`.
- The deferred header means an immediate-failure scenario (first page fails) prints only the error, no orphaned header. Good.

No issues found.

## Files Reviewed

- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/src/databricks_agent_notebooks/integrations/databricks/clusters.py`
- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/src/databricks_agent_notebooks/cli.py`
- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/tests/unit/test_cli.py`
- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/tests/unit/test_integrations_databricks_clusters.py`
- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/src/databricks_agent_notebooks/runtime/connect.py` (dependency check -- not affected)
- `/Users/sim/.config/superpowers/worktrees/databricks-agent-notebooks/codex/agent-notebook-standalone/python/src/databricks_agent_notebooks/integrations/databricks/__init__.py` (no re-exports affected)
