#!/usr/bin/env python
"""直接运行入口：python run.py [serve|login|dl|...]"""

import sys

from tgget.cli import main

if __name__ == "__main__":
    sys.exit(main())
