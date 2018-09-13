#!/usr/bin/python
"""mapper.py"""

# Mapper prints user_id, first_name, last_name, track_id, artist, listen_count

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')

mapper()