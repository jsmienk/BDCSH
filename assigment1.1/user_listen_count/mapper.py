#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        hour_of_day = '-'
        listened_count = 0

        # Two inputs: people.csv & playhistory.csv
        if len(data) < 3:
            continue

        # Read input of the playhistory.csv file
        if len(data) == 3:
            user_id = data[1]
            time_stamp = data[2]

            # Skip header line
            if track_id == 'track_id':
                continue

            # Extract the hour of the day from the datetime
            hour_of_day = datetime.strptime(date_string, '%Y-%m-%d %H:%M:%S').hour()
            listened_count = 1

        if len(data) == 7:
            user_id = data[0]

            # Skip header line
            if user_id == 'id':
                continue

            first_name = data[1]
            last_name = data[2]
        
        print('{0},{1},{2},{3},{4}'.format(user_id, first_name, last_name, hour_of_day, listened_count))

mapper()