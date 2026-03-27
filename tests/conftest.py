"""
Global pytest fixtures. This file is automatically run by pytest before tests
are executed.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure imports resolve to the source package under the repository root,
# not the tests/pulao namespace path.
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
