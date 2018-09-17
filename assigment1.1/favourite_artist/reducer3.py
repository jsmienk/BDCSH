#!/usr/bin/python
"""reducer.py"""

# Input is user_id, first_name, last_name, artist, listen_count
# Reducer prints first_name, last_name, top_artist, listen_count

import sys
import operator

def reducer():
    prev_user = None
    curr_user = None
    prev_first_name = None
    prev_last_name = None
    prev_user_artists = {}

    for line in sys.stdin:
        print(line)
#         data = line.strip().split(',')
#         if len(data) != 5:
#             continue

#         curr_user, first_name, last_name, artist, listen_count = data

#         # Check argument type
#         if not curr_user.isdigit() or not listen_count.isdigit():
#             continue

#         listen_count = int(listen_count)

#         # If current user does not equal previous user
#         if prev_user and prev_user != curr_user:

#             # Print results
#             print_result(prev_first_name, prev_last_name, prev_user_artists)

#             # Reset dict
#             prev_user_artists.clear()

#         # New name
#         prev_first_name = first_name
#         prev_last_name = last_name

#         # Increase count per artist
#         if not prev_user_artists.has_key(artist):
#             prev_user_artists[artist] = 0

#         prev_user_artists[artist] += listen_count

#         # Set the current user as the previous user for next iteration
#         prev_user = curr_user

#     # Print the last user and its favourite artist
#     print_result(prev_first_name, prev_last_name, prev_user_artists)

# # Print the previous user and its favourite artist
# def print_result(first_name, last_name, dict):
#     artists = sorted(dict.items(), key=lambda x:x[1], reverse=True)
#     favouriteTuple = artists[0]
#     print("{0} {1}\t{2}\t{3}".format(first_name, last_name, favouriteTuple[0], favouriteTuple[1]))

reducer()