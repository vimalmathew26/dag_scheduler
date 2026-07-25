"""B6 / DECISION 1: a name declared in two files is rejected on both sides.

Before: the parser kept the first file it happened to read and discarded
the whole of any later file mentioning a name it had already seen. Which
file won depended on directory.glob() enumeration order. Observed: a
two-line file redefining one name took the job count from 7 to 3, threw
away etl.yaml entirely, and left the intruder's definition of extract_data
in place instead of the real one.

DECISION 1: reject every definition of a colliding name, do not pick a
winner, and let every other job in every affected file survive.
"""

from dag_scheduler.definition_parser import DefinitionParser


def write(directory, name, body):
    (directory / name).write_text(body)


class TestDuplicateNames:
    def test_both_definitions_of_a_colliding_name_are_rejected(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  shared:\n    command: 'echo a'\n")
        write(tmp_path, "b.yaml", "jobs:\n  shared:\n    command: 'echo b'\n")

        jobs = DefinitionParser().parse_directory(tmp_path)

        assert "shared" not in jobs, "a colliding name must not resolve to either definition"

    def test_unrelated_jobs_in_both_files_survive(self, tmp_path):
        write(
            tmp_path,
            "a.yaml",
            "jobs:\n  shared:\n    command: 'echo a'\n  only_in_a:\n    command: 'echo a2'\n",
        )
        write(
            tmp_path,
            "b.yaml",
            "jobs:\n  shared:\n    command: 'echo b'\n  only_in_b:\n    command: 'echo b2'\n",
        )

        jobs = DefinitionParser().parse_directory(tmp_path)

        assert set(jobs) == {"only_in_a", "only_in_b"}, (
            "the blast radius of a name collision must be that name only"
        )

    def test_three_way_collision_rejects_all_three(self, tmp_path):
        for letter in "abc":
            write(
                tmp_path,
                f"{letter}.yaml",
                f"jobs:\n"
                f"  shared:\n    command: 'echo {letter}'\n"
                f"  only_{letter}:\n    command: 'echo x'\n",
            )

        jobs = DefinitionParser().parse_directory(tmp_path)

        assert set(jobs) == {"only_a", "only_b", "only_c"}

    def test_rejection_is_logged_with_every_conflicting_path(self, tmp_path, caplog):
        write(tmp_path, "a.yaml", "jobs:\n  shared:\n    command: 'echo a'\n")
        write(tmp_path, "b.yaml", "jobs:\n  shared:\n    command: 'echo b'\n")

        with caplog.at_level("WARNING"):
            DefinitionParser().parse_directory(tmp_path)

        message = "\n".join(r.message for r in caplog.records)
        assert "shared" in message
        assert "a.yaml" in message and "b.yaml" in message, (
            "the operator needs every conflicting path, not just one"
        )

    def test_result_does_not_depend_on_enumeration_order(self, tmp_path):
        """The old behaviour picked whichever file glob() yielded first."""
        write(tmp_path, "zzz.yaml", "jobs:\n  shared:\n    command: 'echo z'\n")
        write(tmp_path, "aaa.yaml", "jobs:\n  shared:\n    command: 'echo a'\n")

        first = DefinitionParser().parse_directory(tmp_path)
        second = DefinitionParser().parse_directory(tmp_path)

        assert first == second
        assert "shared" not in first

    def test_dependents_of_a_rejected_name_cascade_out(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  shared:\n    command: 'echo a'\n")
        write(tmp_path, "b.yaml", "jobs:\n  shared:\n    command: 'echo b'\n")
        write(
            tmp_path,
            "c.yaml",
            "jobs:\n  downstream:\n    command: 'echo c'\n    depends_on: ['shared']\n",
        )

        jobs = DefinitionParser().parse_directory(tmp_path)

        assert "downstream" not in jobs, "a job depending on an unresolvable name must not load"

    def test_no_collision_means_nothing_changes(self, tmp_path):
        write(tmp_path, "a.yaml", "jobs:\n  one:\n    command: 'echo a'\n")
        write(tmp_path, "b.yaml", "jobs:\n  two:\n    command: 'echo b'\n")

        jobs = DefinitionParser().parse_directory(tmp_path)

        assert set(jobs) == {"one", "two"}

    def test_files_are_parsed_in_a_deterministic_order(self, tmp_path):
        for name in ["m.yaml", "a.yaml", "z.toml"]:
            stem = name.split(".")[0]
            if name.endswith(".toml"):
                write(tmp_path, name, f'[jobs.{stem}]\ncommand = "echo x"\n')
            else:
                write(tmp_path, name, f"jobs:\n  {stem}:\n    command: 'echo x'\n")

        parser = DefinitionParser()
        parser.parse_directory(tmp_path)

        ordered = [p.name for p in parser.parsed_files]
        assert ordered == sorted(ordered)
