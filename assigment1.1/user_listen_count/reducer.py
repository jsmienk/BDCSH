#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    # Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song in that hour of the day)

    prev_user = None
    curr_user = None
    curr_user_playhistory = {}

    curr_user_first_name = None
    curr_user_last_name = None

    # Input comes from STDIN
    for line in sys.stdin:

        data = line.strip().split(',')

        # Check argument count        
        if len(data) != 5:
            continue

        # Check argument type
        if not data[4].isdigit():
            continue

        user_id, first_name, last_name, hour_of_day, listened_count = data

        if curr_user_first_name == None and first_name != '-':
            curr_user_first_name = first_name

        if curr_user_last_name == None and last_name != '-':
            curr_user_last_name = last_name

        # If current user_id does not equal previous user_id
        if prev_user and prev_user != curr_user:

            # Print the previous user's playhistory
            for hour in curr_user_playhistory.keys():
                print("{0}, {1}, {2}, {3}".format(curr_user_first_name, curr_user_last_name, hour, curr_user_playhistory[hour]))

            # Reset variables
            curr_user_first_name = None
            curr_user_last_name = None
            curr_user_playhistory.clear()

        # Increase listen count
        if hour_of_day != '-' and curr_user_playhistory.has_key(hour_of_day):
            curr_user_playhistory[hour_of_day] = curr_user_playhistory[hour_of_day] + listened_count

        # Set the current user_id as the previous user_id for next iteration
        prev_user = curr_user

    # Print the current user's playhistory
    for hour in curr_user_playhistory.keys():
        print("{0}, {1}, {2}, {3}".format(curr_user_first_name, curr_user_last_name, hour, curr_user_playhistory[hour]))

reducer()