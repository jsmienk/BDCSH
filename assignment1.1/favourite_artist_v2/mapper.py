#!/usr/bin/python
"""mapper.py"""

# Mapper prints track_id, (artist), (user_id), listen_count

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')
        # Skip header line
        if data[0] == 'track_id':
            continue

        track_id = None
        artist = '-'
        user_id = '-'
        listen_count = 0

        if len(data) == 3: # playhistory.csv
            track_id = data[0]
            user_id = data[1]
            listen_count = 1

            if not user_id.isdigit():
                continue
        elif len(data) >= 4: # tracks.csv
            track_id = data[0]
            artist = data[1]
            # title not important
        else:
            continue
        
        print('{0},{1},{2},{3}'.format(track_id, artist, user_id, listen_count))

mapper()