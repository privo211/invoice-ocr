"""A tiny deterministic harness for testing whether tool descriptions aid routing.

The router reads the comma-separated ``Intent cues:`` field from each tool's
natural-language description. This is a transparent proxy for description-led
selection, not a simulation or benchmark of an LLM.
"""
from dataclasses import dataclass
import re


@dataclass(frozen=True, slots=True)
class Tool:
    name: str
    description: str


# Description set under evaluation: intent cues are specific and separated by
# tool family, with selection boundaries and required arguments also stated.
TOOLS = (
    Tool("list_posts", "List posts from the demo API. Supports page and per_page. Use for collection reads and pagination. Intent cues: list posts, browse posts, next page, show posts, posts page."),
    Tool("get_user", "Fetch one user by numeric user_id. Use for a profile or details about one specific user. Intent cues: user profile, get user, user details, about user, user info."),
    Tool("create_post", "Create a post with title and body. Requires an idempotency_key. Select only for explicit requests to publish or create; do not use for browsing. Intent cues: create post, create a post, publish post, write a post, submit post."),
)

# Deliberately underspecified starting point, for an apples-to-apples ablation.
BASELINE_TOOLS = (
    Tool("list_posts", "List posts from the demo API."),
    Tool("get_user", "Fetch one user from the demo API."),
    Tool("create_post", "Create one post in the demo API."),
)


def _cues(description: str) -> tuple[str, ...]:
    """Extract the declared selection cues from a tool description."""
    match = re.search(r"Intent cues:\s*(.*?)(?:\.|$)", description, re.IGNORECASE)
    if not match:
        return ()
    return tuple(cue.strip().lower() for cue in match.group(1).split(",") if cue.strip())


def route(query: str, *, tools: tuple[Tool, ...] = TOOLS, threshold: int = 1) -> str | None:
    """Select from description cues; clarify for multi-tool or absent intent."""
    normalized = re.sub(r"[^a-z0-9 ]", " ", query.lower())
    normalized = " ".join(normalized.split())
    scores = {tool.name: sum(1 for cue in _cues(tool.description) if cue in normalized) for tool in tools}
    # If distinct tool families are mentioned, ask instead of guessing.
    if sum(score > 0 for score in scores.values()) > 1:
        return None
    high = max(scores.values(), default=0)
    winners = [name for name, score in scores.items() if score == high and score >= threshold]
    return winners[0] if len(winners) == 1 else None


@dataclass(frozen=True, slots=True)
class Case:
    query: str
    expected: str | None
    rationale: str


CASES = (
    Case("Show posts page 2", "list_posts", "pagination intent"),
    Case("Browse posts", "list_posts", "read collection intent"),
    Case("Get user profile", "get_user", "single entity lookup"),
    Case("What is user info?", "get_user", "profile question"),
    Case("Create a post", "create_post", "explicit write intent"),
    Case("Publish post", "create_post", "explicit publish intent"),
    Case("Tell me a joke", None, "unsupported request"),
    Case("", None, "empty input"),
    Case("list posts and get user profile", None, "ambiguous multi-tool request"),
)


def _score(tools: tuple[Tool, ...]) -> dict[str, object]:
    results = [{"query": case.query, "expected": case.expected,
                "actual": route(case.query, tools=tools), "rationale": case.rationale}
               for case in CASES]
    correct = sum(row["expected"] == row["actual"] for row in results)
    return {"passed": correct, "total": len(CASES), "accuracy": correct / len(CASES),
            "results": results}


def evaluate() -> dict[str, object]:
    """Compare concise baseline descriptions with cue-rich descriptions."""
    baseline = _score(BASELINE_TOOLS)
    described = _score(TOOLS)
    return {"suite_size": len(CASES), "baseline": baseline,
            "description_driven": described,
            "description_gain_cases": described["passed"] - baseline["passed"]}
