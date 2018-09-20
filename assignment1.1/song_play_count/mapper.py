#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    LISTEN_COUNT = 1

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.strip().split(',')

        if len(data) < 3:
            continue

        track_id, user_id, date_string = data
        # Skip header line
        if track_id == 'track_id':
            continue
            
        # Reformat date to lose useless time
        date = datetime.strptime(date_string.strip(), '%Y-%m-%d %H:%M:%S').strftime('%Y %m')

        print('{0},{1},{2}'.format(track_id, date, LISTEN_COUNT))

mapper()