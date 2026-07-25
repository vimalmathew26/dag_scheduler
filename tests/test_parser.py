"""Parser tests: the three passes, defaults, and format handling."""

import pytest

from dag_scheduler.definition_parser import DefinitionParser, ParseError, load_file


def write(d, name, body):
    (d / name).write_text(body)
    return d / name


def parse(d):
    return DefinitionParser().parse_directory(d)


class TestLoadFile:
    def test_yaml(self, tmp_path):
        p = write(tmp_path, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        assert load_file(p) == {"jobs": {"x": {"command": "echo"}}}

    def test_toml(self, tmp_path):
        p = write(tmp_path, "a.toml", '[jobs.x]\ncommand = "echo"\n')
        assert load_file(p) == {"jobs": {"x": {"command": "echo"}}}

    def test_unknown_suffix_yields_nothing(self, tmp_path):
        p = write(tmp_path, "a.txt", "whatever")
        assert load_file(p) == {}

    def test_empty_yaml_is_not_none(self, tmp_path):
        p = write(tmp_path, "a.yaml", "")
        assert load_file(p) == {}

    def test_malformed_yaml_raises_parse_error_naming_the_file(self, tmp_path):
        p = write(tmp_path, "a.yaml", "jobs:\n  x:\n   - [unclosed\n")
        with pytest.raises(ParseError) as exc:
            load_file(p)
        assert "a.yaml" in str(exc.value)


class TestPassOneStructure:
    def test_empty_directory(self, tmp_path):
        assert parse(tmp_path) == {}

    def test_file_without_jobs_key_is_ignored(self, tmp_path):
        write(tmp_path, "a.yaml", "something_else: 1\n")
        assert parse(tmp_path) == {}

    def test_non_yaml_files_are_ignored(self, tmp_path):
        write(tmp_path, "notes.txt", "jobs: nope")
        assert parse(tmp_path) == {}

    def test_malformed_file_does_not_block_the_others(self, tmp_path):
        write(tmp_path, "bad.yaml", "jobs:\n  x:\n   - [unclosed\n")
        write(tmp_path, "good.yaml", "jobs:\n  good:\n    command: 'echo'\n")
        assert set(parse(tmp_path)) == {"good"}

    def test_job_missing_command_is_rejected(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  x:\n    timeout: 5\n")
        assert "x" not in parse(tmp_path)

    def test_job_that_is_not_a_mapping_is_rejected(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  x: 'just a string'\n")
        assert "x" not in parse(tmp_path)

    def test_yaml_and_toml_share_one_flat_namespace(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  y:\n    command: 'echo'\n")
        write(tmp_path, "b.toml", '[jobs.t]\ncommand = "echo"\n')
        assert set(parse(tmp_path)) == {"y", "t"}


class TestPassTwoDependencies:
    def test_dependency_on_a_nonexistent_job_removes_the_dependent(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n  x:\n    command: 'echo'\n    depends_on: ['ghost']\n")
        assert "x" not in parse(tmp_path)

    def test_removal_cascades_down_the_chain(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n"
              "  b:\n    command: 'echo'\n    depends_on: ['ghost']\n"
              "  c:\n    command: 'echo'\n    depends_on: ['b']\n"
              "  d:\n    command: 'echo'\n    depends_on: ['c']\n")
        assert parse(tmp_path) == {}

    def test_unrelated_jobs_survive_a_cascade(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n"
              "  b:\n    command: 'echo'\n    depends_on: ['ghost']\n"
              "  fine:\n    command: 'echo'\n")
        assert set(parse(tmp_path)) == {"fine"}

    def test_depends_on_must_be_a_list(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n  x:\n    command: 'echo'\n    depends_on: 'notalist'\n")
        assert "x" not in parse(tmp_path)

    def test_cross_file_dependency_resolves(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  parent:\n    command: 'echo'\n")
        write(tmp_path, "b.toml",
              '[jobs.child]\ncommand = "echo"\ndepends_on = ["parent"]\n')
        assert set(parse(tmp_path)) == {"parent", "child"}


class TestPassThreeCycles:
    def test_two_node_cycle_is_dropped(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n"
              "  a:\n    command: 'echo'\n    depends_on: ['b']\n"
              "  b:\n    command: 'echo'\n    depends_on: ['a']\n")
        assert parse(tmp_path) == {}

    def test_self_loop_is_dropped(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n  a:\n    command: 'echo'\n    depends_on: ['a']\n")
        assert parse(tmp_path) == {}

    def test_valid_jobs_in_another_file_survive_a_cycle(self, tmp_path):
        write(tmp_path, "cycle.yaml",
              "jobs:\n"
              "  a:\n    command: 'echo'\n    depends_on: ['b']\n"
              "  b:\n    command: 'echo'\n    depends_on: ['a']\n")
        write(tmp_path, "ok.yaml", "jobs:\n  ok:\n    command: 'echo'\n")
        assert set(parse(tmp_path)) == {"ok"}

    def test_valid_job_in_the_same_file_as_a_cycle_survives(self, tmp_path):
        """Pass 3 drops cycle participants per job, not per file."""
        write(tmp_path, "a.yaml",
              "jobs:\n"
              "  a:\n    command: 'echo'\n    depends_on: ['b']\n"
              "  b:\n    command: 'echo'\n    depends_on: ['a']\n"
              "  independent:\n    command: 'echo'\n")
        assert set(parse(tmp_path)) == {"independent"}

    def test_dependents_of_a_cycle_are_cascaded_out(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n"
              "  a:\n    command: 'echo'\n    depends_on: ['b']\n"
              "  b:\n    command: 'echo'\n    depends_on: ['a']\n"
              "  downstream:\n    command: 'echo'\n    depends_on: ['a']\n")
        assert parse(tmp_path) == {}


class TestDefaults:
    def test_missing_retry_block_uses_model_defaults(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        job = parse(tmp_path)["x"]
        assert job.retry.max_attempts == 3
        assert job.retry.backoff_base == 2.0
        assert job.retry.jitter is True
        assert job.retry.retry_on_exit_codes == [1]

    def test_partial_retry_block_is_filled_in(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n  x:\n    command: 'echo'\n    retry:\n      max_attempts: 7\n")
        job = parse(tmp_path)["x"]
        assert job.retry.max_attempts == 7
        assert job.retry.backoff_base == 2.0

    def test_other_defaults(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  x:\n    command: 'echo'\n")
        job = parse(tmp_path)["x"]
        assert job.priority == 1
        assert job.timeout == 60
        assert job.tags == []
        assert job.depends_on == []

    def test_explicit_values_win(self, tmp_path):
        write(tmp_path, "a.yaml",
              "jobs:\n  x:\n    command: 'echo'\n    priority: 9\n"
              "    timeout: 5\n    tags: ['a','b']\n")
        job = parse(tmp_path)["x"]
        assert (job.priority, job.timeout, job.tags) == (9, 5, ["a", "b"])
