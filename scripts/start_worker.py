#!/usr/bin/env python3
"""Convenience script to start a Kafka worker."""

import sys
import asyncio
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from workers.cli import worker_main

if __name__ == '__main__':
    worker_main()