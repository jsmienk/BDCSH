#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        # Replace chars: . , ! ? : ; " ( ) < > [ ] # $ = - / with whitespace
        for ch in ['.',',','!','?',':',';','"','(',')','<','>','[',']','#','$','=','-','/']:
            if ch in string:
                line = line.replace(ch, ' ')

        # Split on all the whitespace
        line = line.split()

        # Print every word in the correct format
        for word in line:
            print('{0}\t{1}'.format(word, 1))