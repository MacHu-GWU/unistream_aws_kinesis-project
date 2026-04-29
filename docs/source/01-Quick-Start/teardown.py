# -*- coding: utf-8 -*-

"""
Delete the Kinesis stream used by the Quick Start demo.

Usage::

    python docs/source/01-Quick-Start/teardown.py
"""

from shared import teardown

if __name__ == "__main__":
    teardown()
