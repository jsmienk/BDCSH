#!/usr/bin/python
"""mapper.py"""

import sys
from datetime import datetime

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        # Split on all whitespace and on chars: . , ! ? : ; " ( ) < > [ ] # $ = - /

        # For all words in list
            
            # Print "word\t1"