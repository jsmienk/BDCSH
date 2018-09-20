#!/usr/bin/python
"""mapper.py"""

# Input is track_id, artist, user_id, listen_count
# MonthMapper prints user_id, (first_name, last_name), (artist), listen_count

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        artist = '-'
        listen_count = 0

        if len(data) == 4: # output from the first mapper
            track_id = data[0]
            artist = data[1]
            user_id = data[2]
            listen_count = data[3]

            if not user_id.isdigit() or not listen_count.isdigit():
                continue
        elif len(data) == 7: # people.csv
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]

            # Skip header line
            if user_id == 'id':
                continue
        else:
            continue

        print('{0},{1},{2},{3},{4}'.format(user_id, first_name, last_name, artist, listen_count))

mapper()