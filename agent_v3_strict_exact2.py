#!/usr/bin/env python3
"""
Strict single-pool v3 exact2.0 (ledger) agent wrapper.
"""

import os

os.environ["STRICT_EXACT_VARIANT"] = "exact2"

from agent_v3_strict import main


if __name__ == "__main__":
    main()
