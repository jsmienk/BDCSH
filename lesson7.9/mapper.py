#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

# Outputs key: WEEKDAY    value: SALES
def mapper():
    for line in sys.stdin:

        row = line.split('\t')
        if len(row) < 5:
            continue

        # date
        date = row[0]

        # sales
        sales = row[4]
        if not isinstance(sales, float):
            continue

        # find weekday
        weekday = datetime.strptime(date, "%Y-%m-%d").weekday()

        print('{0}\t{1}'.format(weekday, sales))

mapper()