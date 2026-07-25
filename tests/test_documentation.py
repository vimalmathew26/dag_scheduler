"""The README must not drift from the code.

Documented behaviour that does not hold is the single worst thing a reader
can find, and this project shipped with six such claims. These tests pin
the ones that can be checked mechanically.
"""

import re
from pathlib import Path

import pytest

from dag_scheduler.models import JobState
from dag_scheduler.persistence import Persistence

README = Path(__file__).resolve().parent.parent / "README.md"


@pytest.fixture(scope="module")
def readme() -> str:
    return README.read_text()


def mermaid_blocks(text: str) -> list:
    return re.findall(r"```mermaid\n(.*?)```", text, re.S)


class TestStateDiagram:
    def test_diagram_exists(self, readme):
        assert mermaid_blocks(readme), "the state machine diagram is missing"

    def test_diagram_matches_valid_transitions_exactly(self, readme):
        """The diagram is a second statement of VALID_TRANSITIONS.

        If someone adds a transition to the table and not the picture, or
        the other way round, this fails.
        """
        block = mermaid_blocks(readme)[0]
        documented = set()
        for line in block.splitlines():
            match = re.match(r"\s*(\w+)\s*-->\s*(\w+)\s*$", line)
            if not match:
                continue
            source, target = match.groups()
            if source == "[*]":
                continue
            documented.add((source, target))

        actual = {(f.value, t.value) for f, t in Persistence.VALID_TRANSITIONS}

        assert documented == actual, (
            f"only in README: {sorted(documented - actual)}; "
            f"only in code: {sorted(actual - documented)}"
        )

    def test_initial_state_is_marked(self, readme):
        assert "[*] --> defined" in mermaid_blocks(readme)[0]


class TestDocumentedSurface:
    def test_every_state_is_listed(self, readme):
        for state in JobState:
            assert f"`{state.value}`" in readme, f"{state.value} is not documented"

    def test_state_count_claim_is_right(self, readme):
        match = re.search(r"a (\d+)-state enum", readme)
        assert match, "the enum size claim is missing"
        assert int(match.group(1)) == len(JobState)

    def test_transition_count_claim_is_right(self, readme):
        match = re.search(r"all (\d+) legal transitions", readme)
        assert match, "the transition count claim is missing"
        assert int(match.group(1)) == len(Persistence.VALID_TRANSITIONS)

    def test_every_documented_api_route_exists(self, readme):
        from dag_scheduler.api import app

        real = set()
        for route in app.routes:
            for method in getattr(route, "methods", set()):
                if method in ("GET", "POST"):
                    real.add((method, route.path))

        documented = set(re.findall(r"^\| `(GET|POST)` \| `([^`]+)` \|", readme, re.M))
        assert documented, "no API table found"
        missing = documented - real
        assert not missing, f"documented but not implemented: {sorted(missing)}"

    def test_every_implemented_route_is_documented(self, readme):
        from dag_scheduler.api import app

        documented = {
            path for _, path in re.findall(r"^\| `(GET|POST)` \| `([^`]+)` \|", readme, re.M)
        }
        for route in app.routes:
            path = getattr(route, "path", "")
            if not path.startswith(("/jobs", "/stats", "/metrics", "/health")):
                continue
            assert path in documented, f"{path} ships but is undocumented"

    def test_every_cli_command_is_documented(self, readme):
        from dag_scheduler.cli import cli

        for name in cli.commands:
            assert f"dag-scheduler {name}" in readme, (
                f"the {name} command ships but is undocumented"
            )

    def test_environment_variables_are_documented(self, readme):
        for variable in [
            "DAG_SCHEDULER_JOBS_DIR",
            "DAG_SCHEDULER_DB",
            "DAG_SCHEDULER_MAX_CONCURRENT",
            "DAG_SCHEDULER_HOST",
            "DAG_SCHEDULER_PORT",
            "DAG_SCHEDULER_TOKEN",
            "DAG_SCHEDULER_LOG_LEVEL",
            "DAG_SCHEDULER_LOG_JSON",
        ]:
            assert variable in readme, f"{variable} is undocumented"


class TestExampleDefinitionsMatchTheirDescription:
    def test_failing_job_retries_the_documented_number_of_times(self):
        import yaml

        jobs_dir = README.parent / "jobs"
        spec = yaml.safe_load((jobs_dir / "failing.yaml").read_text())
        policy = spec["jobs"]["always_fails"]["retry"]
        assert policy["max_attempts"] == 3, "the README says always_fails retries exactly 3 times"
        assert policy["jitter"] is False

    def test_slow_job_timeout_is_shorter_than_its_sleep(self):
        import yaml

        jobs_dir = README.parent / "jobs"
        spec = yaml.safe_load((jobs_dir / "slow.yaml").read_text())["jobs"]["slow_job"]
        assert spec["timeout"] < 30, "slow_job must actually time out"

    def test_cycle_definition_really_is_a_cycle(self):
        from dag_scheduler.dag import CycleError, topological_sort
        from dag_scheduler.definition_parser import load_file
        from dag_scheduler.models import JobDefinition

        spec = load_file(README.parent / "jobs" / "cycle.yaml")
        jobs = {name: JobDefinition(**body) for name, body in spec["jobs"].items()}
        with pytest.raises(CycleError):
            topological_sort(jobs)
