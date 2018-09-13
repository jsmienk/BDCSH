#!/usr/bin/python
"""reducer.py"""

import sys

# combine result round 1 with tracks to find the artists for every song the user listened to.
# reducer prints track_id, artist, user_id, first_name, last_name, listen_count

def reducer():

    prev_track = None
    curr_track = None
    curr_track_count = 0

    curr_artist = None
    curr_user_id = None
    curr_user_first_name = None
    curr_user_last_name = None

    # Input comes from STDIN: track_id, (artist), (user_id, first_name, last_name, listen_count)
    for line in sys.stdin:
        data = line.strip().split(',')

        if len(data) != 6:
            continue

        track_id, artist, user_id, first_name, last_name, listen_count = data

        # Check argument type
        if not listen_count.isdigit():
            continue

        if curr_artist == None and artist != '-':
            curr_artist = artist

        if curr_user_id == None and user_id != '-':
            curr_user_id = user_id

        if curr_user_first_name == None and first_name != '-':
            curr_user_first_name = first_name

        if curr_user_last_name == None and last_name != '-':
            curr_user_last_name = last_name

        # If current track_id does not equal previous track_id
        if prev_track and prev_track != curr_track:

            # Print the previous user's playhistory
            print_result(track_id, artist, curr_user_id, curr_user_first_name, curr_user_last_name, curr_track_count)

            # Reset variables
            curr_track_count = 0

            curr_artist = None
            curr_user_id = None
            curr_user_first_name = None
            curr_user_last_name = None

        # Increase the count
        curr_track_count += int(listen_count)

        # Set the current track_id as the previous track_id for next iteration
        prev_track = curr_track

    # Print the current track's listen count
    print_result(curr_track, curr_artist, curr_user_id, curr_user_first_name, curr_user_last_name, curr_track_count)

# Print track_id, artist, user_id, first_name, last_name, listen_count
def print_result(track_id, artist, user_id, first_name, last_name, listen_count):
    print("{0}\t{1}\t{2}\t{3}\t{4}\t{5}".format(track_id, artist, user_id, first_name, last_name, listen_count))  

reducer()