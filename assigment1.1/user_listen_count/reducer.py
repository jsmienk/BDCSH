#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    # Expected output: (FirstName, LastName, hourOfday, numberOfTimesListened to a song in that hour of the day)

    prev_user = None
    curr_user = None
    curr_user_playhistory = [0] * 24
    curr_user_first_name = None
    curr_user_last_name = None

    for line in sys.stdin:
        data = line.strip().split(',')

        # Check argument count        
        if len(data) != 5:
            continue

        curr_user, first_name, last_name, hour_of_day, listened_count = data

        # Check argument type
        if not listened_count.isdigit():
            continue

        if curr_user_first_name == None and first_name != '-':
            curr_user_first_name = first_name

        if curr_user_last_name == None and last_name != '-':
            curr_user_last_name = last_name

        # If current user_id does not equal previous user_id
        if prev_user and prev_user != curr_user:

            # Print the previous user's playhistory
            print_result(curr_user_first_name, curr_user_last_name, curr_user_playhistory)

            # Reset variables
            curr_user_first_name = None
            curr_user_last_name = None
            curr_user_playhistory = [0] * 24

        if hour_of_day.isdigit():  
            hour_of_day = int(hour_of_day)
            # Increase listen count
            curr_user_playhistory[hour_of_day] = curr_user_playhistory[hour_of_day] + int(listened_count)

        # Set the current user_id as the previous user_id for next iteration
        prev_user = curr_user

    # Print the current user's playhistory
    print_result(curr_user_first_name, curr_user_last_name, curr_user_playhistory)

def print_result(first_name, last_name, list):
    for hour, count in enumerate(list):
        print("{0}\t{1}\t{2}\t{3}".format(first_name, last_name, hour, count))

reducer()