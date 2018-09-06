#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.split('\t')

        if len(data) < 5:
            continue

        node_id = data[0]
        body = data[4]

        # Replace chars: . , ! ? : ; " ( ) < > [ ] # $ = - / with whitespace
        for ch in ['.',',','!','?',':',';','"','(',')','<','>','[',']','#','$','=','-','/']:
            if ch in body:
                body = body.replace(ch, ' ')

        # Split on all the whitespace
        body = body.split()

        # Print every word in the correct format
        for word in body:
            print('{0}\t{1}\t{2}'.format(word.lower(), 1, node_id))

mapper()