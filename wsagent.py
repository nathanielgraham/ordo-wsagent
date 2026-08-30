#!/usr/bin/env python3
"""Backward-compatible CLI entry. Prefer: python3 -m ordo_wsagent"""
import os
import sys

# Allow running from a source checkout without installing.
_src = os.path.join(os.path.dirname(os.path.abspath(__file__)), "src")
if os.path.isdir(_src) and _src not in sys.path:
    sys.path.insert(0, _src)

from ordo_wsagent.cli import main

if __name__ == "__main__":
    main()
