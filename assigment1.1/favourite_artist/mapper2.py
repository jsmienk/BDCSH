#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

mapper()