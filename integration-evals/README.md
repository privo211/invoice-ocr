# Integration Evals Lab

A small, self-contained Python project demonstrating how to build and evaluate an external API integration boundary. It uses [JSONPlaceholder](https://jsonplaceholder.typicode.com), a public demo REST API, and only Python's standard library at runtime. The repo is intentionally a standalone learning/demo artifact: it does not connect to, import, or reproduce any employer or customer code.

## What it demonstrates

- **Typed normalization:** provider JSON is validated at the boundary and converted to immutable `Post` and `User` objects.
- **Request reliability:** bounded exponential backoff for safe GETs and explicitly idempotent writes; 429/5xx and transport errors are retried, while other 4xx responses fail immediately.
- **Repeat-safe write contract:** create requests require a caller-supplied idempotency key and send it as `Idempotency-Key`. JSONPlaceholder is a fake-write demo and does not promise durable idempotency; the behavior is verified with a local deterministic transport.
- **Auth seam:** optional `API_BEARER_TOKEN` is passed as an Authorization header for APIs that accept bearer tokens. This is header plumbing only; there is no OAuth exchange or credential storage.
- **Pagination:** page and page-size are validated and mapped to JSONPlaceholder query parameters.
- **Description-driven tool evals:** the deterministic router extracts selection cues from each tool description itself and exercises unsupported inputs, ambiguous requests, and explicit write intent. A baseline vs. cue-rich description ablation makes the contribution testable. This is not an LLM benchmark; fixed examples make regressions reproducible.

## Quick start

Requires Python 3.10+.

```bash
python -m venv .venv
. .venv/bin/activate
python -m pip install -e .
python -m pytest
```

No test needs network access. Try the live public demo endpoints when connected:

```bash
integration-lab posts --page 1 --per-page 3
integration-lab user 1
integration-lab route "Show posts page 2"
integration-lab eval
```

`API_BEARER_TOKEN` is optional and usually unnecessary for JSONPlaceholder. Never commit real credentials.

## Architecture

```text
CLI ──> IntegrationClient ──> Transport ──> JSONPlaceholder
              │
              ├── retry / status / JSON handling
              ├── pagination and request headers
              └── payload validation ──> immutable models

Evaluation cases ──> deterministic router ──> expected tool or clarify
```

`Transport` is a narrow injectable protocol. Production calls use `urllib`; tests substitute scripted responses to verify retries, request construction, error classification, and schema validation without network instability. The API adapter owns provider-specific paths and field names; downstream callers only see normalized dataclasses.

## Evaluation scope

`integration-lab eval` checks 9 fixed cases including list pagination, single-user lookup, explicit create intent, unsupported input, empty input, and conflicting intent. On this deliberately small hand-authored suite, terse baseline descriptions route 3/9 cases correctly (the three no-tool/clarification cases); descriptions with explicit `Intent cues:` route 9/9, a six-case improvement. The score is exact-match accuracy on these examples only and does not establish production tool-calling quality. The cue extraction is a transparent deterministic proxy for description quality, not an LLM or agent eval. A real agent rollout should add representative anonymized queries, adversarial cases, argument validation, tool execution outcomes, and human-reviewed acceptance thresholds.

## Deliberate limits

This demo has no persistent storage, OAuth flow, distributed rate limiter, circuit breaker, server-side idempotency store, or production telemetry. JSONPlaceholder may change availability and treats writes as simulated responses. Those gaps are explicit so a reviewer can see where adapter patterns end and production infrastructure begins.
