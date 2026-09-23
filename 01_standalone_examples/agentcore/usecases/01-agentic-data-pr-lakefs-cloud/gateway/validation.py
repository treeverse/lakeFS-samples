"""Server-side input validation for every curated capability.

These guards are authoritative. The agent's system prompt is advisory only; the
permitted repository, branches, and prefixes are always derived from the run
scope (server state), never from model-supplied arguments. Any request that
targets the wrong branch, escapes the allowed prefix, attempts path traversal,
exceeds the size limit, or uses an unsupported content type is rejected here
before it can reach lakeFS.
"""

from __future__ import annotations

from common.naming import RunScope

# Corpus objects are intentionally tiny; cap generously but firmly.
MAX_OBJECT_SIZE_BYTES = 256 * 1024  # 256 KiB
MAX_LIST_RESULTS = 200
DEFAULT_LIST_RESULTS = 100

ALLOWED_CONTENT_TYPES = frozenset(
    {
        "application/json",
        "text/json",
        "text/plain",
        "text/markdown",
        "text/x-markdown",
    }
)


class CapabilityError(Exception):
    """A structured, safe-to-return capability error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message

    def to_dict(self) -> dict[str, str]:
        return {"error": self.code, "message": self.message}


def reject_path_traversal(path: str) -> str:
    if not isinstance(path, str) or not path:
        raise CapabilityError("invalid_path", "path must be a non-empty string")
    if path.startswith("/"):
        raise CapabilityError("invalid_path", "path must not be absolute")
    if "\\" in path:
        raise CapabilityError("invalid_path", "backslashes are not allowed in paths")
    # Reject any traversal component, including url-ish encodings.
    lowered = path.lower()
    if ".." in path.split("/") or "%2e%2e" in lowered or "\x00" in path:
        raise CapabilityError("path_traversal", f"path traversal rejected: {path!r}")
    return path


def ensure_content_type(content_type: str | None) -> str:
    ct = (content_type or "").split(";")[0].strip().lower()
    if ct not in ALLOWED_CONTENT_TYPES:
        raise CapabilityError(
            "unsupported_content_type",
            f"content type {content_type!r} is not allowed; "
            f"allowed: {sorted(ALLOWED_CONTENT_TYPES)}",
        )
    return ct


def ensure_size(content: bytes | str) -> None:
    size = len(content.encode("utf-8") if isinstance(content, str) else content)
    if size > MAX_OBJECT_SIZE_BYTES:
        raise CapabilityError(
            "object_too_large",
            f"object is {size} bytes; limit is {MAX_OBJECT_SIZE_BYTES} bytes",
        )


def clamp_amount(amount: int | None) -> int:
    if not amount or amount <= 0:
        return DEFAULT_LIST_RESULTS
    return min(int(amount), MAX_LIST_RESULTS)


def ensure_read_allowed(scope: RunScope, branch: str, path: str) -> None:
    reject_path_traversal(path)
    if branch not in scope.readable_branches():
        raise CapabilityError(
            "branch_not_readable",
            f"branch {branch!r} is not readable in this run",
        )
    if not scope.is_read_allowed(branch, path):
        raise CapabilityError(
            "prefix_not_allowed",
            f"path {path!r} is outside the allowed read prefixes for {branch!r}",
        )


def ensure_write_allowed(scope: RunScope, branch: str, path: str) -> None:
    reject_path_traversal(path)
    if scope.is_protected_branch(branch):
        raise CapabilityError(
            "protected_branch",
            f"writes to protected branch {branch!r} are forbidden",
        )
    if branch != scope.writable_branch():
        raise CapabilityError(
            "branch_not_writable",
            f"the only writable branch is {scope.writable_branch()!r}, not {branch!r}",
        )
    if not scope.is_write_allowed(branch, path):
        raise CapabilityError(
            "prefix_not_allowed",
            f"path {path!r} is outside the allowed output prefix "
            f"{scope.output_prefix!r}",
        )


def ensure_list_prefix_allowed(scope: RunScope, branch: str, prefix: str) -> None:
    reject_path_traversal(prefix.rstrip("/") + "/x")  # traversal check only
    # Listing is restricted to the corpus prefix on the baseline branch.
    if branch not in scope.readable_branches():
        raise CapabilityError("branch_not_readable", f"branch {branch!r} not readable")
    allowed = scope.read_prefixes(branch)
    if not any(prefix.startswith(p) or p.startswith(prefix) for p in allowed):
        raise CapabilityError(
            "prefix_not_allowed",
            f"list prefix {prefix!r} not allowed for {branch!r}",
        )
