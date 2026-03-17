#!/usr/bin/env python3
"""Thin compatibility wrapper for research_loom CLI."""

import sys

from research_loom.cli import main


if __name__ == "__main__":
    sys.exit(main())
