#!/usr/bin/python
"""mapper.py"""

# Input is track_id, artist, user_id, first_name, last_name, listen_count
# Mapper prints user_id, first_name, last_name, track_id, artist, listen_count

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

        if len(data) != 6:
            continue

        

mapper()