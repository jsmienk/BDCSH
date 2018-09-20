#!/usr/bin/python
"""reducer.py"""

# MonthMapper prints track_id, (artist), (user_id), listen_count
# Reducer prints track_id, artist, user_id, listen_count

import sys

def reducer():
    prev_track = None
    curr_track = None
    prev_artist = None
    prev_track_listeners = []

    for line in sys.stdin:
        data = line.strip().split(',')

        if len(data) != 4:
            continue

        curr_track, curr_artist, user_id, listen_count = data

        # Check argument type
        if not listen_count.isdigit():
            continue

        # If current track_id does not equal previous track_id
        if prev_track and prev_track != curr_track:

            # Print the previous track's listeners
            print_result(prev_track, prev_artist, prev_track_listeners)

            # Reset variables
            prev_artist = None
            prev_track_listeners = []

        # Save the artist of the current track_id
        if curr_artist != '-':
            prev_artist = curr_artist

        # Append a user to the listeners list
        if user_id != '-':
            prev_track_listeners.append((user_id, listen_count))

        # Set the current track_id as the previous track_id for next iteration
        prev_track = curr_track

    # Print the current track's listeners
    print_result(prev_track, prev_artist, prev_track_listeners)

# Print track_id, artist, user_id, listen_count
def print_result(track_id, artist, listeners):
    for listener in listeners:
        print("{0},{1},{2},{3}".format(track_id, artist, listener[0], listener[1]))

reducer()