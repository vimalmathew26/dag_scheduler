"""Fixtures and platform markers."""

import os

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


# The daemon is POSIX only. It signals process groups via os.killpg, spawns
# jobs with start_new_session so the whole job dies rather than just its
# shell, and installs asyncio signal handlers. None of those exist on
# Windows, and job commands run through the platform shell, so the shell
# syntax these tests use is POSIX too.
#
# Marked tests are the ones that genuinely cannot pass off POSIX, verified
# against a real Windows run rather than inferred. Tests that happen to work
# on both, such as plain `echo` and `>&2` redirection, are deliberately left
# unmarked so they keep providing coverage there.
requires_posix = pytest.mark.skipif(
    os.name != "posix",
    reason="requires POSIX process groups, signals and a POSIX shell",
)
