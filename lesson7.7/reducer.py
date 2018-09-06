#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prevWord = None
    currentWord = None
    currentWordCount = 0
    currentWordNodeIdSet = {}
    sortedNodeIdList = []

    # Input comes from STDIN
    for line in sys.stdin:

        # Check argument count
        data = line.strip().split('\t')
        if len(data) != 3:
            continue

        currentWord = data[0]
        nodeId = data[2]

        # If current word does not equal previous word
        if prevWord and prevWord != currentWord:

            # Print the previous word and its count
            print("{0}\t{1}\t{2}".format(prevWord, currentWordCount, sorted(currentWordNodeIdSet)))

            # Set current word count to 0
            currentWordCount = 0
            currentWordNodeIdSet = {}

            # New current word
            prevWord = currentWord

        # Increase word count
        currentWordCount += int(data[1])
        currentWordNodeIdSet.add(nodeId)

        # Set the current word as the previous word for next iteration
        prevWord = currentWord

    # Print the current word and its count
    print("{0}\t{1}\t{2}".format(currentWord, currentWordCount, sorted(currentWordNodeIdSet)))

reducer()