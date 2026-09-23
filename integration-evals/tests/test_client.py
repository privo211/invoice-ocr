import json

import pytest

from integration_lab.client import ApiError, IntegrationClient, RetryPolicy
from integration_lab.models import PayloadError


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, *, headers, body, timeout):
        self.calls.append((method, url, headers, body, timeout))
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def response(status, payload=None):
    return status, {}, b"" if payload is None else json.dumps(payload).encode()


def post(id=11):
    return {"id": id, "userId": 3, "title": "hello", "body": "world"}


def test_pagination_is_validated_and_normalized():
    transport = FakeTransport([response(200, [post()])])
    client = IntegrationClient("https://mock.invalid/", transport=transport, sleep=lambda _: None)
    posts = client.list_posts(page=2, per_page=7)
    assert posts[0].id == 11
    assert transport.calls[0][1] == "https://mock.invalid/posts?_page=2&_limit=7"
    with pytest.raises(ValueError):
        client.list_posts(page=0)


def test_bearer_token_is_sent_without_being_logged_or_modified():
    transport = FakeTransport([response(200, {"id": 4, "name": "A", "username": "a", "email": "a@example.test"})])
    client = IntegrationClient(transport=transport, token="test-token")
    assert client.get_user(4).name == "A"
    assert transport.calls[0][2]["Authorization"] == "Bearer test-token"


def test_transient_get_failure_retries_with_bounded_exponential_delay():
    transport = FakeTransport([response(503), response(429), response(200, [post()])])
    delays = []
    client = IntegrationClient(transport=transport, retry=RetryPolicy(3, .1, .15), sleep=delays.append)
    assert client.list_posts()
    assert len(transport.calls) == 3
    assert delays == [.1, .15]


def test_post_sends_stable_idempotency_key_and_json():
    transport = FakeTransport([response(201, post())])
    client = IntegrationClient(transport=transport)
    assert client.create_post(user_id=3, title="hello", body="world", idempotency_key="request-abc").id == 11
    method, _, headers, body, _ = transport.calls[0]
    assert method == "POST"
    assert headers["Idempotency-Key"] == "request-abc"
    assert json.loads(body) == {"userId": 3, "title": "hello", "body": "world"}


def test_client_refuses_write_without_idempotency_key():
    client = IntegrationClient(transport=FakeTransport([]))
    with pytest.raises(ValueError, match="idempotency_key"):
        client.create_post(user_id=1, title="x", body="y", idempotency_key=" ")


def test_non_retryable_client_error_fails_immediately():
    transport = FakeTransport([response(404, {"message": "not found"})])
    client = IntegrationClient(transport=transport, retry=RetryPolicy(4), sleep=lambda _: None)
    with pytest.raises(ApiError) as error:
        client.get_user(888)
    assert error.value.status == 404
    assert len(transport.calls) == 1


def test_invalid_contract_and_invalid_json_are_distinct_errors():
    bad_shape = FakeTransport([response(200, [{"id": "wrong"}])])
    with pytest.raises(PayloadError):
        IntegrationClient(transport=bad_shape).list_posts()
    bad_json = FakeTransport([(200, {}, b"not-json")])
    with pytest.raises(ApiError, match="invalid JSON"):
        IntegrationClient(transport=bad_json).list_posts()


def test_transport_timeout_retries_safe_request_then_succeeds():
    transport = FakeTransport([TimeoutError("slow"), response(200, [post()])])
    client = IntegrationClient(transport=transport, sleep=lambda _: None)
    assert len(client.list_posts()) == 1
    assert len(transport.calls) == 2
