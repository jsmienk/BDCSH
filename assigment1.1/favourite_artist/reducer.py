#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():
    for line in sys.stdin:
        data = line.strip().split(',')
        if len(data) != 3:
            continue

reducer()