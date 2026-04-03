#!/usr/bin/env python3
"""
Strict single-pool v3 exact1 (legacy) agent wrapper.
"""

import os

os.environ["STRICT_EXACT_VARIANT"] = "exact1"

from agent_v3_strict import main


if __name__ == "__main__":
    main()
