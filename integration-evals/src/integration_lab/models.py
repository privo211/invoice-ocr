"""Normalized domain objects; upstream payloads never leak past this boundary."""
from dataclasses import dataclass
from typing import Any


class PayloadError(ValueError):
    """The upstream response was valid JSON but violated the expected contract."""


def _integer(payload: dict[str, Any], key: str) -> int:
    value = payload.get(key)
    if isinstance(value, bool) or not isinstance(value, int):
        raise PayloadError(f"expected integer field '{key}'")
    return value


def _string(payload: dict[str, Any], key: str) -> str:
    value = payload.get(key)
    if not isinstance(value, str):
        raise PayloadError(f"expected string field '{key}'")
    return value


@dataclass(frozen=True, slots=True)
class Post:
    id: int
    user_id: int
    title: str
    body: str

    @classmethod
    def from_payload(cls, payload: Any) -> "Post":
        if not isinstance(payload, dict):
            raise PayloadError("expected post object")
        return cls(_integer(payload, "id"), _integer(payload, "userId"),
                   _string(payload, "title"), _string(payload, "body"))


@dataclass(frozen=True, slots=True)
class User:
    id: int
    name: str
    username: str
    email: str

    @classmethod
    def from_payload(cls, payload: Any) -> "User":
        if not isinstance(payload, dict):
            raise PayloadError("expected user object")
        return cls(_integer(payload, "id"), _string(payload, "name"),
                   _string(payload, "username"), _string(payload, "email"))
