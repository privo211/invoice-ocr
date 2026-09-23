"""Small resilient client with injectable transport and explicit failure semantics."""
from __future__ import annotations

import json
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Protocol

from .models import PayloadError, Post, User


class Transport(Protocol):
    def request(self, method: str, url: str, *, headers: dict[str, str], body: bytes | None,
                timeout: float) -> tuple[int, dict[str, str], bytes]: ...


class UrllibTransport:
    def request(self, method: str, url: str, *, headers: dict[str, str], body: bytes | None,
                timeout: float) -> tuple[int, dict[str, str], bytes]:
        req = urllib.request.Request(url, data=body, headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=timeout) as response:
                return response.status, dict(response.headers.items()), response.read()
        except urllib.error.HTTPError as exc:
            return exc.code, dict(exc.headers.items()), exc.read()


@dataclass(frozen=True, slots=True)
class ApiError(Exception):
    message: str
    status: int | None = None
    retryable: bool = False

    def __str__(self) -> str:
        suffix = f" (HTTP {self.status})" if self.status is not None else ""
        return self.message + suffix


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    attempts: int = 3
    base_delay: float = 0.05
    max_delay: float = 0.5

    def __post_init__(self) -> None:
        if self.attempts < 1 or self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("retry attempts must be positive and delays non-negative")


class IntegrationClient:
    """JSONPlaceholder adapter. Token support is optional bearer-header plumbing, not OAuth."""
    def __init__(self, base_url: str = "https://jsonplaceholder.typicode.com", *,
                 token: str | None = None, transport: Transport | None = None,
                 retry: RetryPolicy = RetryPolicy(), timeout: float = 5.0,
                 sleep: Callable[[float], None] = time.sleep) -> None:
        self.base_url = base_url.rstrip("/")
        self.token, self.transport, self.retry, self.timeout, self.sleep = token, transport or UrllibTransport(), retry, timeout, sleep

    def _request(self, method: str, path: str, *, query: dict[str, str] | None = None,
                 payload: dict[str, Any] | None = None, idempotency_key: str | None = None) -> Any:
        url = self.base_url + path
        if query:
            url += "?" + urllib.parse.urlencode(query)
        headers = {"Accept": "application/json", "User-Agent": "integration-lab/0.1"}
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        body = None
        if payload is not None:
            headers["Content-Type"] = "application/json"
            body = json.dumps(payload, separators=(",", ":")).encode()
        if idempotency_key:
            headers["Idempotency-Key"] = idempotency_key
        safe_to_retry = method in {"GET", "HEAD"} or bool(idempotency_key)
        for attempt in range(self.retry.attempts):
            try:
                status, _, raw = self.transport.request(method, url, headers=headers, body=body, timeout=self.timeout)
            except (TimeoutError, OSError, urllib.error.URLError) as exc:
                if attempt + 1 < self.retry.attempts and safe_to_retry:
                    self.sleep(min(self.retry.base_delay * (2 ** attempt), self.retry.max_delay))
                    continue
                raise ApiError(f"transport failure: {exc}", retryable=True) from exc
            if status == 429 or 500 <= status <= 599:
                if attempt + 1 < self.retry.attempts and safe_to_retry:
                    self.sleep(min(self.retry.base_delay * (2 ** attempt), self.retry.max_delay))
                    continue
                raise ApiError("upstream service unavailable", status, retryable=True)
            if not 200 <= status < 300:
                raise ApiError("upstream request rejected", status)
            if not raw:
                return None
            try:
                return json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise ApiError("upstream returned invalid JSON") from exc
        raise AssertionError("retry loop exhausted")

    def list_posts(self, *, page: int = 1, per_page: int = 10) -> list[Post]:
        if page < 1 or not 1 <= per_page <= 100:
            raise ValueError("page must be >= 1 and per_page between 1 and 100")
        data = self._request("GET", "/posts", query={"_page": str(page), "_limit": str(per_page)})
        if not isinstance(data, list):
            raise PayloadError("expected posts array")
        return [Post.from_payload(item) for item in data]

    def get_user(self, user_id: int) -> User:
        if user_id < 1:
            raise ValueError("user_id must be positive")
        data = self._request("GET", f"/users/{user_id}")
        return User.from_payload(data)

    def create_post(self, *, user_id: int, title: str, body: str, idempotency_key: str) -> Post:
        if not idempotency_key.strip():
            raise ValueError("idempotency_key is required for create_post")
        data = self._request("POST", "/posts", payload={"userId": user_id, "title": title, "body": body}, idempotency_key=idempotency_key)
        return Post.from_payload(data)
