#!/usr/bin/python
"""mapper.py"""

# Mapper prints track_id, (artist), (user_id, first_name, last_name, listen_count)

import sys

# combine result round 1 with tracks to find the artists for every song the user listened to.
def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        track_id = None
        artist = '-'
        user_id = '-'
        first_name = '-'
        last_name = '-'
        listen_count = 0

        # Read input of the tracks.csv file
        if len(data) == 4:
            track_id = data[0]
            artist = data[1]
            # title not important

            # Skip header line
            if track_id == 'track_id':
                continue

        elif len(data) == 5: # Read input from the first mapper
            # user_id, first_name, last_name, track_id, listen_count
            track_id = data[3]
            user_id = data[0]
            first_name = data[1]
            last_name = data[2]
            listen_count = data[4]
        else:
            continue

        print('{0},{1},{2},{3},{4},{5}'.format(track_id, artist, user_id, first_name, last_name, listen_count))


mapper()