#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prev_track = None
    curr_track = None
    curr_track_plays = {}

    # Input comes from STDIN
    for line in sys.stdin:

        # Check argument count
        data = line.strip().split(',')
        if len(data) != 3:
            continue

        # Check argument type
        if not data[2].isdigit():
            continue

        curr_track = data[0]
        date = data[1]
        count = int(data[2])

        # If current word does not equal previous word
        if prev_track and prev_track != curr_track:

            # Print the previous track and its count per month
            for month in sorted(curr_track_plays.keys()):
                print("{0}\t{1}\t{2}".format(prev_track, month, curr_track_plays[month]))

            # Reset dict
            curr_track_plays.clear()

        # Increase count per month
        if not curr_track_plays.has_key(date):
            curr_track_plays[date] = 0

        curr_track_plays[date] = curr_track_plays[date] + count

        # Set the current track as the previous track for next iteration
        prev_track = curr_track

    # Print the last track and its count
    for month in curr_track_plays.keys():
        print("{0}\t{1}\t{2}".format(curr_track, month, curr_track_plays[month]))

reducer()