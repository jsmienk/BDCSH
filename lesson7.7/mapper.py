#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        # Replace chars: . , ! ? : ; " ( ) < > [ ] # $ = - / with whitespace
        for ch in ['.',',','!','?',':',';','"','(',')','<','>','[',']','#','$','=','-','/']:
            if ch in line:
                line = line.replace(ch, ' ')

        # Split on all the whitespace
        line = line.split()

        # Print every word in the correct format
        for word in line:
            print('{0}\t{1}'.format(word.lower(), 1))

mapper()