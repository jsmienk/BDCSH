#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.split(',')

        if len(data) < 3:
            continue

        track_id, user_id, time_stamp = data
        # Skip header line
        if track_id == 'track_id':
            continue
            
        # Reformat date to lose useless time
        date = datetime.strptime(time_stamp, '%Y-%m-%d %HH:%MM:%ss').strftime('%Y %m')

        print('{0},{1},{2}'.format(track_id, date, 1))

mapper()