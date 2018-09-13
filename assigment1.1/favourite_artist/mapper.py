#!/usr/bin/python
"""mapper.py"""

# Mapper prints user_id, (first_name, last_name), (track_id)

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        user_id = None
        first_name = '-'
        last_name = '-'
        track_id = '-'

        # Read input of the playhistory.csv file
        if len(data) == 3:
            track_id = data[0]
            user_id = data[1]

            # Skip header line
            if user_id == 'user':
                continue
        elif len(data) == 7:
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]

            # Skip header line
            if user_id == 'id':
                continue
        else:
            continue
        
        print('{0},{1},{2},{3}'.format(user_id, first_name, last_name, track_id))

mapper()