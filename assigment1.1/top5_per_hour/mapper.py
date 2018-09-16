#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    HOUR_OF_DAY_CONST = 12

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.strip().split(',')

        # Skip header line
        if data[0] == 'track_id':
            continue

        track_id = None
        title = '-'
        artist = '-'
        listened_count = 0

        # Read input of the playhistory.csv file
        if len(data) == 3:
            track_id = data[0]
            time_stamp = data[2]

            # Extract the hour of the day from the datetime
            hour_of_day = datetime.strptime(time_stamp.strip(), '%Y-%m-%d %H:%M:%S').hour

            if hour_of_day != HOUR_OF_DAY_CONST:
                continue

            listened_count = 1
        elif len(data) > 3:
            track_id = data[0]
            artist = ','.join(data[1:-2])
            title = data[len(data) - 2]
        else:
            continue
        
        print('{0},{1},{2},{3}'.format(track_id, title, artist, listened_count))

mapper()