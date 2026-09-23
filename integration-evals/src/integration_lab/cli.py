"""Command line interface: `python -m integration_lab.cli --help`."""
import argparse
from dataclasses import asdict
import json
import os
import sys

from .client import ApiError, IntegrationClient
from .evaluation import evaluate, route


def main() -> None:
    parser = argparse.ArgumentParser(prog="integration-lab", description="Explore a resilient JSONPlaceholder adapter and deterministic tool-routing evals.")
    sub = parser.add_subparsers(dest="command", required=True)
    posts = sub.add_parser("posts", help="list a page of posts")
    posts.add_argument("--page", type=int, default=1)
    posts.add_argument("--per-page", type=int, default=5)
    user = sub.add_parser("user", help="fetch a user")
    user.add_argument("user_id", type=int)
    route_cmd = sub.add_parser("route", help="inspect the deterministic tool router")
    route_cmd.add_argument("query")
    sub.add_parser("eval", help="run the routing evaluation set")
    args = parser.parse_args()
    if args.command == "eval":
        result = evaluate()
        print(json.dumps(result, indent=2))
        if result["description_driven"]["passed"] != result["description_driven"]["total"]:
            sys.exit(1)
        return
    if args.command == "route":
        print(route(args.query) or "clarify")
        return
    client = IntegrationClient(token=os.getenv("API_BEARER_TOKEN"))
    try:
        if args.command == "posts":
            values = client.list_posts(page=args.page, per_page=args.per_page)
        else:
            values = client.get_user(args.user_id)
        print(json.dumps([asdict(value) for value in values] if isinstance(values, list) else asdict(values), indent=2))
    except (ApiError, ValueError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)


if __name__ == "__main__":
    main()
