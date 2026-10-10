"""Minimal lakeFS Cloud REST client (requests-based, no lakeFS SDK dependency).

Covers exactly the operations this sample needs: auth check, repository listing,
branches, objects, commits, diff, and Pull Requests (Enterprise API). Endpoints
are relative to ``<endpoint>/api/v1`` and match the lakeFS OpenAPI spec.

All access to lakeFS data in this sample goes through this client and the normal
lakeFS Cloud API. It never touches the physical backing bucket, never requests
S3 credentials, and never uses lakeFS Mount, lakectl local, or Metadata Search.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import quote

import requests
from requests.auth import HTTPBasicAuth

API_PREFIX = "/api/v1"


class LakeFSError(RuntimeError):
    def __init__(self, message: str, status_code: int | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code


@dataclass
class ObjectStats:
    path: str
    size_bytes: int
    checksum: str
    content_type: str | None
    mtime: int | None = None
    metadata: dict[str, str] | None = None

    @classmethod
    def from_api(cls, d: dict[str, Any]) -> ObjectStats:
        return cls(
            path=d.get("path", ""),
            size_bytes=int(d.get("size_bytes") or 0),
            checksum=d.get("checksum", ""),
            content_type=d.get("content_type"),
            mtime=d.get("mtime"),
            metadata=d.get("metadata"),
        )


class LakeFSClient:
    def __init__(
        self,
        endpoint: str,
        access_key_id: str,
        secret_access_key: str,
        *,
        timeout: int = 30,
    ) -> None:
        self._base = endpoint.rstrip("/") + API_PREFIX
        self._timeout = timeout
        self._session = requests.Session()
        self._session.auth = HTTPBasicAuth(access_key_id, secret_access_key)
        self._endpoint = endpoint.rstrip("/")

    # --- low-level -------------------------------------------------------
    def _url(self, path: str) -> str:
        return self._base + path

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        files: Any | None = None,
        raw: bool = False,
    ) -> Any:
        try:
            resp = self._session.request(
                method,
                self._url(path),
                params=params,
                json=json_body,
                files=files,
                timeout=self._timeout,
            )
        except requests.RequestException as exc:  # network errors carry no secret
            raise LakeFSError(f"lakeFS request failed: {exc.__class__.__name__}") from None

        if resp.status_code >= 400:
            # Response bodies never contain our credentials; safe to surface.
            snippet = resp.text[:300]
            raise LakeFSError(
                f"lakeFS {method} {path} -> {resp.status_code}: {snippet}",
                status_code=resp.status_code,
            )
        if raw:
            return resp.content
        if not resp.content:
            return None
        return resp.json()

    @staticmethod
    def _enc(segment: str) -> str:
        # Branch/ref names contain slashes (agentcore-demo/<run>/workspace) that
        # must be percent-encoded as a single path segment.
        return quote(segment, safe="")

    # --- auth / repos ----------------------------------------------------
    def whoami(self) -> dict[str, Any]:
        return self._request("GET", "/user")

    def list_repositories(self, amount: int = 100) -> list[dict[str, Any]]:
        repos: list[dict[str, Any]] = []
        after = ""
        while True:
            page = self._request(
                "GET", "/repositories", params={"after": after, "amount": amount}
            )
            repos.extend(page.get("results", []))
            pag = page.get("pagination", {})
            if not pag.get("has_more"):
                break
            after = pag.get("next_offset", "")
        return repos

    def get_repository(self, repository: str) -> dict[str, Any]:
        return self._request("GET", f"/repositories/{self._enc(repository)}")

    # --- branches --------------------------------------------------------
    def get_branch(self, repository: str, branch: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/repositories/{self._enc(repository)}/branches/{self._enc(branch)}",
        )

    def branch_head(self, repository: str, branch: str) -> str:
        return self.get_branch(repository, branch)["commit_id"]

    def create_branch(
        self, repository: str, name: str, source: str, *, hidden: bool = False
    ) -> str:
        # createBranch returns the new commit id as a bare (non-JSON) string.
        raw = self._request(
            "POST",
            f"/repositories/{self._enc(repository)}/branches",
            json_body={"name": name, "source": source, "hidden": hidden},
            raw=True,
        )
        return raw.decode("utf-8").strip().strip('"')

    def delete_branch(self, repository: str, branch: str) -> None:
        self._request(
            "DELETE",
            f"/repositories/{self._enc(repository)}/branches/{self._enc(branch)}",
        )

    def branch_exists(self, repository: str, branch: str) -> bool:
        try:
            self.get_branch(repository, branch)
            return True
        except LakeFSError as exc:
            if exc.status_code == 404:
                return False
            raise

    # --- objects ---------------------------------------------------------
    def upload_object(
        self,
        repository: str,
        branch: str,
        path: str,
        content: bytes | str,
        content_type: str = "application/octet-stream",
    ) -> ObjectStats:
        if isinstance(content, str):
            content = content.encode("utf-8")
        files = {"content": (path.rsplit("/", 1)[-1], content, content_type)}
        stats = self._request(
            "POST",
            f"/repositories/{self._enc(repository)}/branches/{self._enc(branch)}/objects",
            params={"path": path},
            files=files,
        )
        return ObjectStats.from_api(stats)

    def get_object(self, repository: str, ref: str, path: str) -> bytes:
        return self._request(
            "GET",
            f"/repositories/{self._enc(repository)}/refs/{self._enc(ref)}/objects",
            params={"path": path},
            raw=True,
        )

    def stat_object(self, repository: str, ref: str, path: str) -> ObjectStats:
        d = self._request(
            "GET",
            f"/repositories/{self._enc(repository)}/refs/{self._enc(ref)}/objects/stat",
            params={"path": path, "user_metadata": "true"},
        )
        return ObjectStats.from_api(d)

    def list_objects(
        self,
        repository: str,
        ref: str,
        prefix: str = "",
        *,
        after: str = "",
        amount: int = 100,
        delimiter: str = "",
    ) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/repositories/{self._enc(repository)}/refs/{self._enc(ref)}/objects/ls",
            params={
                "prefix": prefix,
                "after": after,
                "amount": amount,
                "delimiter": delimiter,
                "user_metadata": "true",
            },
        )

    # --- commits / diff --------------------------------------------------
    def commit(
        self,
        repository: str,
        branch: str,
        message: str,
        metadata: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        body: dict[str, Any] = {"message": message}
        if metadata:
            body["metadata"] = {k: str(v) for k, v in metadata.items()}
        return self._request(
            "POST",
            f"/repositories/{self._enc(repository)}/branches/{self._enc(branch)}/commits",
            json_body=body,
        )

    def diff_refs(
        self,
        repository: str,
        left_ref: str,
        right_ref: str,
        *,
        prefix: str = "",
        amount: int = 1000,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        after = ""
        while True:
            page = self._request(
                "GET",
                f"/repositories/{self._enc(repository)}/refs/"
                f"{self._enc(left_ref)}/diff/{self._enc(right_ref)}",
                params={"prefix": prefix, "after": after, "amount": amount},
            )
            results.extend(page.get("results", []))
            pag = page.get("pagination", {})
            if not pag.get("has_more"):
                break
            after = pag.get("next_offset", "")
        return results

    # --- pull requests (Enterprise API) ---------------------------------
    def create_pull_request(
        self,
        repository: str,
        title: str,
        description: str,
        source_branch: str,
        destination_branch: str,
    ) -> dict[str, Any]:
        created = self._request(
            "POST",
            f"/repositories/{self._enc(repository)}/pulls",
            json_body={
                "title": title,
                "description": description,
                "source_branch": source_branch,
                "destination_branch": destination_branch,
            },
        )
        # Some deployments return only {"id": ...}; normalise to a full object.
        if isinstance(created, dict) and set(created.keys()) == {"id"}:
            return self.get_pull_request(repository, created["id"])
        return created

    def get_pull_request(self, repository: str, pull_id: str) -> dict[str, Any]:
        return self._request(
            "GET",
            f"/repositories/{self._enc(repository)}/pulls/{self._enc(str(pull_id))}",
        )

    def list_pull_requests(
        self, repository: str, status: str | None = None, amount: int = 100
    ) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"amount": amount}
        if status:
            params["status"] = status
        page = self._request(
            "GET", f"/repositories/{self._enc(repository)}/pulls", params=params
        )
        return page.get("results", [])

    def update_pull_request(
        self,
        repository: str,
        pull_id: str,
        *,
        status: str | None = None,
        title: str | None = None,
        description: str | None = None,
    ) -> None:
        body: dict[str, Any] = {}
        if status is not None:
            body["status"] = status
        if title is not None:
            body["title"] = title
        if description is not None:
            body["description"] = description
        self._request(
            "PATCH",
            f"/repositories/{self._enc(repository)}/pulls/{self._enc(str(pull_id))}",
            json_body=body,
        )

    def merge_pull_request(self, repository: str, pull_id: str) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/repositories/{self._enc(repository)}/pulls/{self._enc(str(pull_id))}/merge",
        )

    # --- helpers ---------------------------------------------------------
    def pull_request_web_url(self, repository: str, pull_id: str) -> str:
        return f"{self._endpoint}/repositories/{repository}/pulls/{pull_id}"
