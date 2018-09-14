#!/usr/bin/python
"""reducer.py"""

# Mapper prints user_id, (first_name, last_name), (track_id)
# Reducer prints user_id, first_name, last_name, track_id, listen_count

import sys

def reducer():

    prev_user = None
    curr_user = None
    curr_track_count = {}
    curr_first_name = None
    curr_last_name = None

    for line in sys.stdin:
        data = line.strip().split(',')
        if len(data) != 4:
            continue

        curr_user, first_name, last_name, track_id = data

        if curr_first_name == None and first_name != '-':
            curr_first_name = first_name

        if curr_last_name == None and last_name != '-':
            curr_last_name = last_name

        # If current user_id does not equal previous user_id
        if prev_user and prev_user != curr_user:

            # Print the previous user's track count
            print_result(prev_user, curr_first_name, curr_last_name, curr_track_count)

            # Reset variables
            curr_first_name = None
            curr_last_name = None
            curr_track_count.clear()

        if track_id != '-':
            # Initialize key
            if not curr_track_count.has_key(track_id):
                curr_track_count[track_id] = 0

            # Increase listen count
            curr_track_count[track_id] = curr_track_count[track_id] + 1

        # Set the current user_id as the previous user_id for next iteration
        prev_user = curr_user

    # Print the current user's playhistory
    print_result(prev_user, curr_first_name, curr_last_name, curr_track_count)

def print_result(user_id, first_name, last_name, dict):
    for track_id in dict.keys():
        print("{0},{1},{2},{3},{4}".format(user_id, first_name, last_name, track_id, dict[track_id]))

reducer()