import json
import csv
import os
import requests
from Revizto_Config import BASE_URL, PROJECT_UUID, EXPORTS_DIR
from Revizto_MasterScript import get_headers


CLASH_TEST_FIELDS = [
    "projectUuid",
    "testName",
    "lockedAt", "lockedBy",
    "createdAt", "createdBy",
    "lastRunAt", "lastRunBy",
    "lastIssueSyncAt", "lastIssueSyncBy",
    "groupsNew", "groupsClashing", "groupsResolved",
    "groupsReviewed", "groupsNotReviewed", "groupsApproved", "groupsOnHold",
    "clashesNew", "clashesClashing", "clashesResolved",
    "issuesActive", "issuesNew",
]


def get_clash_tests(project_uuid: str, limit: int = 2000) -> list[dict]:
    """Get all clash tests for a given project, handling pagination."""
    all_tests = []
    page = 1

    while True:
        response = requests.get(
            f"{BASE_URL}/project/{project_uuid}/clash-test/export",
            headers=get_headers(),
            params={"limit": limit, "page": page},
            timeout=10,
        )

        if response.status_code != 200:
            print(f"[Revizto] Error fetching clash tests (page {page}): {response.status_code}")
            break

        try:
            body = response.json()
        except ValueError:
            print(f"[Revizto] Invalid JSON response on page {page}.")
            break

        if body.get("result") != 0:
            print(f"[Revizto] API error: {body.get('message', 'Unknown error')}")
            break

        entities = body["data"]["entities"]
        if not entities:
            break

        for test in entities:
            test["projectUuid"] = project_uuid
        all_tests.extend(entities)

        if len(entities) < limit:
            break

        page += 1

    return all_tests


def get_all_clash_tests(projects: list[dict]) -> list[dict]:
    """Fetch clash tests across all projects."""
    all_tests = []
    for project in projects:
        uuid = project["uuid"]
        print(f"[Revizto] Fetching clash tests for project: {project.get('title', uuid)}")
        tests = get_clash_tests(uuid)
        print(f"  → {len(tests)} clash test(s) found.")
        all_tests.extend(tests)
    return all_tests


def save_json(tests: list[dict], filename: str = "revizto_clash_tests.json"):
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w") as f:
        json.dump(tests, f, indent=2)
    print(f"Saved {len(tests)} clash tests to {path}")


def save_csv(tests: list[dict], filename: str = "revizto_clash_tests.csv"):
    if not tests:
        print("No clash tests to save.")
        return
    path = os.path.join(EXPORTS_DIR, filename)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CLASH_TEST_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(tests)
    print(f"Saved {len(tests)} clash tests to {path}")


if __name__ == "__main__":
    if not PROJECT_UUID:
        print("No project selected. Please set project_uuid in config.json.")
    else:
        tests = get_clash_tests(PROJECT_UUID)
        if tests:
            # Print one line per test (name only). The full JSON dump used to
            # be printed here, but on projects with hundreds/thousands of
            # tests the cockpit terminal had to render that as DOM and would
            # stall the browser tab. The full data is still written to the
            # JSON/CSV files below — terminal output is summary-only.
            print(f"\n{len(tests)} clash test(s):")
            for t in tests:
                print(f"  - {t.get('testName', '<unnamed>')}")
            save_json(tests)
            save_csv(tests)
        else:
            print("No clash tests found.")