"""CLI behaviour that is worth pinning.

Deliberately thin. Output formatting is not tested: it will churn and the
value is low. What matters is that `logs` picks the right run and that the
commands fail cleanly when the daemon is not running.
"""

import json

import httpx
import pytest
from click.testing import CliRunner

from dag_scheduler import cli as cli_module


@pytest.fixture
def runner():
    return CliRunner()


class TestLogsPicksTheLatestRun:
    def test_requests_logs_for_the_newest_run(self, runner, monkeypatch):
        """The API returns runs newest first; the CLI took the last one."""
        requested = {}

        def fake_get(url, *a, **kw):
            if url.endswith("/runs"):
                body = [
                    {"run_id": "newest", "state": "done"},
                    {"run_id": "middle", "state": "done"},
                    {"run_id": "oldest", "state": "done"},
                ]
            else:
                requested["url"] = url
                body = []
            return httpx.Response(200, content=json.dumps(body), request=httpx.Request("GET", url))

        monkeypatch.setattr(cli_module.httpx, "get", fake_get)

        result = runner.invoke(cli_module.cli, ["logs", "myjob"])

        assert result.exit_code == 0
        assert requested["url"].endswith("/runs/newest/logs"), (
            f"asked for the wrong run: {requested['url']}"
        )

    def test_no_runs_is_not_an_error(self, runner, monkeypatch):
        def fake_get(url, *a, **kw):
            return httpx.Response(200, content="[]", request=httpx.Request("GET", url))

        monkeypatch.setattr(cli_module.httpx, "get", fake_get)
        result = runner.invoke(cli_module.cli, ["logs", "myjob"])
        assert result.exit_code == 0
        assert "No runs found" in result.output


class TestDaemonDown:
    @pytest.mark.parametrize("args", [["status"], ["stats"], ["runs", "j"], ["logs", "j"]])
    def test_read_commands_report_connection_refused(self, runner, monkeypatch, args):
        def refuse(url, *a, **kw):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(cli_module.httpx, "get", refuse)
        result = runner.invoke(cli_module.cli, args)
        assert result.exit_code == 1
        assert "Daemon not running" in result.output

    @pytest.mark.parametrize("args", [["trigger", "j"], ["cancel", "j"]])
    def test_write_commands_report_connection_refused(self, runner, monkeypatch, args):
        def refuse(url, *a, **kw):
            raise httpx.ConnectError("refused")

        monkeypatch.setattr(cli_module.httpx, "post", refuse)
        result = runner.invoke(cli_module.cli, args)
        assert result.exit_code == 1
        assert "Daemon not running" in result.output


class TestCommandSurface:
    def test_every_documented_command_exists(self, runner):
        """The README lists these; all of them must be real."""
        result = runner.invoke(cli_module.cli, ["--help"])
        for command in ["load", "status", "trigger", "logs", "runs", "stats", "cancel", "reset"]:
            assert command in result.output, f"{command} is documented but missing"


class TestApiUrlOverride:
    def test_flag_overrides_the_default(self, runner, monkeypatch):
        seen = {}

        def fake_get(url, *a, **kw):
            seen["url"] = url
            return httpx.Response(200, content="[]", request=httpx.Request("GET", url))

        monkeypatch.setattr(cli_module.httpx, "get", fake_get)
        runner.invoke(cli_module.cli, ["--api-url", "http://elsewhere:9000", "status"])

        assert seen["url"].startswith("http://elsewhere:9000")

    def test_token_is_sent_on_mutating_calls(self, runner, monkeypatch):
        seen = {}

        def fake_post(url, *a, **kw):
            seen["headers"] = kw.get("headers") or {}
            return httpx.Response(
                200, content='{"message": "ok"}', request=httpx.Request("POST", url)
            )

        monkeypatch.setenv("DAG_SCHEDULER_TOKEN", "s3cret")
        monkeypatch.setattr(cli_module.httpx, "post", fake_post)
        runner.invoke(cli_module.cli, ["trigger", "j"])

        assert seen["headers"].get("Authorization") == "Bearer s3cret"

    def test_no_token_means_no_header(self, runner, monkeypatch):
        seen = {}

        def fake_post(url, *a, **kw):
            seen["headers"] = kw.get("headers") or {}
            return httpx.Response(
                200, content='{"message": "ok"}', request=httpx.Request("POST", url)
            )

        monkeypatch.delenv("DAG_SCHEDULER_TOKEN", raising=False)
        monkeypatch.setattr(cli_module.httpx, "post", fake_post)
        runner.invoke(cli_module.cli, ["cancel", "j"])

        assert "Authorization" not in seen["headers"]


class TestLoad:
    def test_copies_the_file_into_the_jobs_directory(self, runner, tmp_path, monkeypatch):
        source = tmp_path / "new.yaml"
        source.write_text("jobs:\n  x:\n    command: 'echo'\n")
        destination = tmp_path / "jobs"
        destination.mkdir()
        monkeypatch.setattr(cli_module, "JOBS_DIR", destination)

        result = runner.invoke(cli_module.cli, ["load", str(source)])

        assert result.exit_code == 0
        assert (destination / "new.yaml").read_text() == source.read_text()

    def test_missing_file_is_rejected(self, runner):
        result = runner.invoke(cli_module.cli, ["load", "/no/such/file.yaml"])
        assert result.exit_code != 0


class TestStatsFormatting:
    def test_renders_the_summary(self, runner, monkeypatch):
        def fake_get(url, *a, **kw):
            body = json.dumps(
                {
                    "total_runs": 9,
                    "pass_rate": 0.5556,
                    "avg_duration_seconds": 0.22,
                    "jobs_by_state": {"done": 5, "failed": 1},
                }
            )
            return httpx.Response(200, content=body, request=httpx.Request("GET", url))

        monkeypatch.setattr(cli_module.httpx, "get", fake_get)
        result = runner.invoke(cli_module.cli, ["stats"])

        assert result.exit_code == 0
        assert "55.56%" in result.output
        assert "done: 5" in result.output
