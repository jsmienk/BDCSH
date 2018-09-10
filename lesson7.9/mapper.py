#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

# Outputs key: WEEKDAY     SALES
def mapper():
    for line in sys.stdin:

        weekday = datetime.strptime(date, "%Y-%m-%d").weekday()

mapper()