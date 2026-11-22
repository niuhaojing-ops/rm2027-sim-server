#!/usr/bin/env python3
"""Zero-install launcher for the standalone RM2027 simulation server."""

from __future__ import annotations

from pathlib import Path
import sys

SRC_DIR = Path(__file__).resolve().parent / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from rm2027_sim.cli import main

if __name__ == "__main__":
    sys.exit(main())
