"""Pytest bootstrap.

The package is currently mapped to the repository root by
pyproject.toml's ``package-dir = {dag_scheduler = "."}``, so importing
``dag_scheduler`` requires the repository's *parent* directory on
sys.path.  This shim goes away when the project moves to a src/ layout.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
