#!/usr/bin/env python3
"""Convenience script to run a benchmark."""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from benchmark.cli import coordinator_main

if __name__ == '__main__':
    coordinator_main()