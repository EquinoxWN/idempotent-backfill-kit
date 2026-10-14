"""Allow `python -m idempotent_backfill_kit`."""

import sys

from .cli import main

sys.exit(main())
