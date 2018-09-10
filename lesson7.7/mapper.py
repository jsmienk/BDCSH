#!/usr/bin/python
"""mapper.py"""

import sys

def mapper():

    # Input comes from STDIN (standard input)
    for line in sys.stdin:

        data = line.split('\t')

        if len(data) < 5:
            continue

        node_id = data[0].strip('"')
        if not node_id.isdigit():
            continue

        title = data[2]
        body = data[4]

        content = title + ' ' = body

        # Replace chars: . , ! ? : ; " ( ) < > [ ] # $ = - / with whitespace
        for ch in ['.',',','!','?',':',';','"','(',')','<','>','[',']','#','$','=','-','/']:
            if ch in content:
                content = content.replace(ch, ' ')

        # Split on all the whitespace
        content = content.split()

        # Print every word in the correct format
        for word in content:
            print('{0}\t{1}\t{2}'.format(word.lower(), 1, node_id))

mapper()