"""Environment-driven configuration and the API token gate."""

import importlib

import pytest
from httpx import ASGITransport, AsyncClient

import dag_scheduler.config as config_module


def reload_config(monkeypatch, **env):
    for key, value in env.items():
        if value is None:
            monkeypatch.delenv(key, raising=False)
        else:
            monkeypatch.setenv(key, value)
    return importlib.reload(config_module)


@pytest.fixture(autouse=True)
def restore_config():
    yield
    importlib.reload(config_module)


class TestEnvironmentOverrides:
    def test_concurrency_is_overridable(self, monkeypatch):
        cfg = reload_config(monkeypatch, DAG_SCHEDULER_MAX_CONCURRENT="16")
        assert cfg.MAX_CONCURRENT == 16

    def test_defaults_apply_when_unset(self, monkeypatch):
        cfg = reload_config(monkeypatch, DAG_SCHEDULER_MAX_CONCURRENT=None)
        assert cfg.MAX_CONCURRENT == 4

    def test_host_and_port(self, monkeypatch):
        cfg = reload_config(
            monkeypatch, DAG_SCHEDULER_HOST="0.0.0.0", DAG_SCHEDULER_PORT="9999"
        )
        assert (cfg.API_HOST, cfg.API_PORT) == ("0.0.0.0", 9999)

    def test_paths(self, monkeypatch, tmp_path):
        cfg = reload_config(
            monkeypatch,
            DAG_SCHEDULER_DB=str(tmp_path / "x.db"),
            DAG_SCHEDULER_JOBS_DIR=str(tmp_path / "defs"),
        )
        assert cfg.DB_PATH == tmp_path / "x.db"
        assert cfg.JOBS_DIR == tmp_path / "defs"

    @pytest.mark.parametrize("raw,expected", [
        ("1", True), ("true", True), ("YES", True), ("on", True),
        ("0", False), ("false", False), ("", False),
    ])
    def test_boolean_parsing(self, monkeypatch, raw, expected):
        cfg = reload_config(monkeypatch, DAG_SCHEDULER_LOG_JSON=raw)
        assert cfg.LOG_JSON is expected

    def test_a_bad_integer_fails_loudly(self, monkeypatch):
        with pytest.raises(ValueError, match="MAX_CONCURRENT"):
            reload_config(monkeypatch, DAG_SCHEDULER_MAX_CONCURRENT="lots")

    def test_importing_config_creates_nothing(self, monkeypatch, tmp_path):
        target = tmp_path / "never"
        reload_config(monkeypatch, XDG_DATA_HOME=str(target))
        assert not target.exists()


class TestRetryDefaultsHaveOneHome:
    def test_config_no_longer_duplicates_the_model(self):
        """The same five numbers used to be written down three times."""
        for name in [
            "DEFAULT_RETRY", "DEFAULT_BACKOFF_BASE", "DEFAULT_JITTER",
            "DEFAULT_RETRY_ON_EXIT_CODES", "DEFAULT_TIMEOUT",
        ]:
            assert not hasattr(config_module, name), (
                f"{name} is back in config; RetryPolicy owns these"
            )

    def test_parser_output_equals_the_model_defaults(self, tmp_path):
        """A job with no retry block must produce exactly RetryPolicy().

        The parser used to rebuild the policy field by field from constants
        in config, so the two could drift apart silently.
        """
        from dag_scheduler.definition_parser import DefinitionParser
        from dag_scheduler.models import JobDefinition, RetryPolicy

        (tmp_path / "a.yaml").write_text("jobs:\n  x:\n    command: 'echo'\n")

        job = DefinitionParser().parse_directory(tmp_path)["x"]

        assert job.retry == RetryPolicy()
        assert job == JobDefinition(command="echo")


class TestTokenGate:
    async def _client(self, persistence, token, monkeypatch):
        # Deliberately not importlib.reload here. Reloading the api module
        # rebinds `app` in the module dict while other test modules still
        # hold the original object, so init_api configures one app and the
        # client exercises another.
        import dag_scheduler.api as api_module
        monkeypatch.setattr(api_module, "API_TOKEN", token)

        from dag_scheduler.log_store import LogStore
        from dag_scheduler.process_manager import ProcessManager
        from dag_scheduler.scheduler import Scheduler

        class Reg:
            jobs = {}

            def get_job(self, name):
                return self.jobs.get(name)

            def get_all_jobs(self):
                return dict(self.jobs)

            def known_job_names(self):
                return set(self.jobs)

            async def snapshot(self):
                return dict(self.jobs)

        api_module.init_api(
            Scheduler(persistence, Reg(), None), Reg(), persistence,
            LogStore(persistence.db_path), ProcessManager(persistence),
        )
        return api_module

    @pytest.mark.parametrize("path", [
        "/jobs/x/trigger", "/jobs/x/cancel", "/jobs/x/reset",
    ])
    async def test_mutating_routes_reject_a_missing_token(
        self, persistence, path, monkeypatch
    ):
        api_module = await self._client(persistence, "s3cret", monkeypatch)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            assert (await c.post(path)).status_code == 401

    @pytest.mark.parametrize("path", [
        "/jobs/x/trigger", "/jobs/x/cancel", "/jobs/x/reset",
    ])
    async def test_mutating_routes_reject_a_wrong_token(
        self, persistence, path, monkeypatch
    ):
        api_module = await self._client(persistence, "s3cret", monkeypatch)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post(path, headers={"Authorization": "Bearer nope"})
            assert r.status_code == 401

    async def test_a_correct_token_passes_the_gate(self, persistence, monkeypatch):
        api_module = await self._client(persistence, "s3cret", monkeypatch)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post(
                "/jobs/x/trigger", headers={"Authorization": "Bearer s3cret"}
            )
            # 404 because the job does not exist: past the gate, which is
            # what is being asserted.
            assert r.status_code == 404

    @pytest.mark.parametrize("path", ["/health", "/jobs", "/stats"])
    async def test_reads_stay_open(self, persistence, path, monkeypatch):
        api_module = await self._client(persistence, "s3cret", monkeypatch)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            assert (await c.get(path)).status_code == 200

    async def test_no_token_configured_means_no_gate(self, persistence, monkeypatch):
        api_module = await self._client(persistence, None, monkeypatch)
        transport = ASGITransport(app=api_module.app)
        async with AsyncClient(transport=transport, base_url="http://t") as c:
            assert (await c.post("/jobs/x/trigger")).status_code == 404
