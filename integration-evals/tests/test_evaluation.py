from dataclasses import replace

from integration_lab.evaluation import CASES, TOOLS, evaluate, route


def test_descriptions_include_selection_boundaries_and_required_arguments():
    descriptions = {tool.name: tool.description for tool in TOOLS}
    assert "page" in descriptions["list_posts"]
    assert "numeric user_id" in descriptions["get_user"]
    assert "idempotency_key" in descriptions["create_post"]
    assert "explicit" in descriptions["create_post"]
    assert all("Intent cues:" in text for text in descriptions.values())


def test_description_change_changes_selected_tool():
    tools = tuple(replace(tool, description=tool.description.replace(
        "browse posts", "user profile")) if tool.name == "list_posts" else tool
        for tool in TOOLS)
    assert route("Browse posts") == "list_posts"
    # Routing follows the altered description rather than a separately stored phrase list.
    assert route("Browse posts", tools=tools) is None
    assert route("User profile", tools=tools) is None  # conflict with the user tool


def test_description_ablation_reports_baseline_and_gain_for_small_suite():
    report = evaluate()
    assert report["suite_size"] == len(CASES) == 9
    assert report["baseline"]["passed"] == 3
    assert report["description_driven"]["passed"] == 9
    assert report["description_gain_cases"] == 6
    # Unsupported, empty, and conflicting requests remain clarify/no-tool cases.
    assert report["description_driven"]["results"][-3]["actual"] is None
    assert report["description_driven"]["results"][-1]["actual"] is None


def test_router_clarifies_for_unsupported_and_conflicting_intent():
    assert route("Tell me about weather") is None
    assert route("list posts and get user profile") is None
