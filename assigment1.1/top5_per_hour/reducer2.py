#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prev_track = None
    curr_track = None
    curr_title = None
    curr_artist = None
    curr_listen_count = 0

    # Input comes from STDIN
    for line in sys.stdin:

        data = line.strip().split(',')

        # Check argument count        
        if len(data) != 4:
            continue

        curr_track, title, artist, listened_count = data

        # Check argument type
        if not listened_count.isdigit():
            continue

        if curr_title == None and title != '-':
            curr_title = title

        if curr_artist == None and artist != '-':
            curr_artist = artist

        # If current track_id does not equal previous track_id
        if prev_track and prev_track != curr_track:

            # Print the previous track information
            print("{0}\t{1}\t{2}\t{3}".format(prev_track, curr_title, curr_artist, curr_listen_count))

            # Reset variables
            curr_title = None
            curr_artist = None
            curr_listen_count = 0
            
        # Increase listen count
        curr_listen_count += int(listened_count)

        # Set the current track_id as the previous track_id for next iteration
        prev_track = curr_track

    # Print the current user's playhistory
    print("{0}\t{1}\t{2}\t{3}".format(curr_track, curr_title, curr_artist, curr_listen_count))

reducer()