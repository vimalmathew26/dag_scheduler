"""Fixtures for tests that need a real database."""

import pytest
import pytest_asyncio

from dag_scheduler.models import JobDefinition
from dag_scheduler.persistence import Persistence


@pytest_asyncio.fixture
async def persistence(tmp_path):
    """A Persistence backed by a throwaway SQLite file, schema created."""
    p = Persistence(tmp_path / "test.db")
    await p.setup()
    return p


@pytest.fixture
def definition():
    def _make(command="true", **kwargs):
        return JobDefinition(command=command, **kwargs)

    return _make
