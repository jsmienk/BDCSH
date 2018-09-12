#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.strip().split(',')

        if len(data) < 3:
            continue

        track_id, user_id, date_string = data
        # Skip header line
        if track_id == 'track_id' or user_id == 'user' or date_string == 'datetime':
            continue
            
        # Reformat date to lose useless time
        date = datetime.strptime(date_string.strip(), '%Y-%m-%d').strftime('%Y %m')

        print('{0},{1},{2}'.format(track_id, date, 1))

mapper()