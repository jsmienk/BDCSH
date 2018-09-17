#!/usr/bin/python
"""mapper.py"""

# Input is track_id, artist, user_id, first_name, last_name, listen_count
# Mapper prints user_id, first_name, last_name, artist, listen_count

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        if len(data) != 6:
            continue

        track_id, artist, user_id, first_name, last_name, listen_count = data

        if not user_id.isdigit() or not listen_count.isdigit():
            continue

        print("{0},{1},{2},{3},{4}".format(user_id, first_name, last_name, artist, listen_count))

mapper()