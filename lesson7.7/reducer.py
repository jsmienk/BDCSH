#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prevWord = None
    currentWord = None
    currentWordCount = 0

    # Input comes from STDIN
    for line in sys.stdin:

        # Check argument count
        data = line.strip().split('\t')
        if len(data) != 2:
            continue

        currentWord = data[0]

        # If current word does not equal previous word
        if prevWord and prevWord != currentWord:

            # Print the previous word and its count
            print "{0}\t{1}".format(prevWord, currentWordCount)

            # Set current word count to 0
            currentWordCount = 0

            # New current word
            prevWord = currentWord

        # Increase word count
        currentWordCount += data[1]

    # Print the current word and its count
    print "{0}\t{1}".format(currentWord, currentWordCount)