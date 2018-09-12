#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prev_track = None
    curr_track = None

    curr_track_title = None
    curr_track_artist = None
    curr_track_listened_count = 0

    all_tracks = 

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

        if curr_track_title == None and title != '-':
            curr_track_title = title

        if curr_track_artist == None and artist != '-':
            curr_track_artist = artist

        # If current track_id does not equal previous track_id
        if prev_track and prev_track != curr_track:

            # Print the previous track information
            all_tracks.append("{0}\t{1}\t{2}".format(curr_track_title, curr_track_artist, curr_track_listened_count))

            # Reset variables
            curr_track_title = None
            curr_track_artist = None
            curr_track_listened_count = 0
            
        # Increase listen count
        curr_track_listened_count += int(listened_count)

        # Set the current track_id as the previous track_id for next iteration
        prev_track = curr_track

    # Print the current user's playhistory
    all_tracks.append("{0}\t{1}\t{2}".format(curr_track_title, curr_track_artist, curr_track_listened_count))

    for track in sorted(all_tracks, reverse = True)[:10]:
        print(track)

reducer()