#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():
    for line in sys.stdin:
        data = line.strip().split(',')
        if len(data) != 4:
            continue

        track_id, title, artist, listen_count = data

        if not listen_count.isdigit():
            continue
        
        print('{0},{1},{2}'.format(listen_count, title, artist))

mapper()