"""Allow ``python -m databricks_agent_notebooks`` invocation."""

from __future__ import annotations

import sys

from .cli import main

sys.exit(main())
