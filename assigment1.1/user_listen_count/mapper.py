#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        hour_of_day = '-'
        listened_count = 0

        if len(data) == 3: # playhistory.csv
            user_id = data[1]
            time_stamp = data[2]
            hour_of_day = datetime.strptime(time_stamp.strip(), '%Y-%m-%d %H:%M:%S').hour
            listened_count = 1
        elif len(data) == 7: # people.csv
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]
        else: # invalid source
            continue

        # Skip header line / invalid sources
        if not user_id.isdigit():
            continue

        print('{0},{1},{2},{3},{4}'.format(user_id, first_name, last_name, hour_of_day, listened_count))

mapper()