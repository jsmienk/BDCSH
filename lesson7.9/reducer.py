#!/usr/bin/python
"""reducer.py"""

import sys

def reducer():

    prevWeekday = None
    currWeekday = None
    currWeekdaySales = []

    for line in sys.stdin:

        data = line.split('\t')
        if len(data) != 2:
            continue

        # weekday
        currWeekday = data[0]
        # sales
        sales = float(data[1])

        # If current weekday does not equal previous weekday
        if prevWeekday and prevWeekday != currWeekday:

            # Calc mean sales
            mean = sum(currWeekdaySales) / len(currWeekdaySales)

            # Print the previous weekday and its mean sales
            print("{0}\t{1}".format(prevWeekday, mean))

            # Reset vars
            currWeekdaySales = []

            # New current weekday
            prevWeekday = currWeekday

        # Add sales to total sales list
        currWeekdaySales.append(sales)

        # Set the current weekday as the previous weekday for next iteration
        prevWeekday = currWeekday

    # Print the final weekday and its mean sales
    mean = sum(currWeekdaySales) / len(currWeekdaySales)
    print("{0}\t{1}".format(prevWeekday, mean))

reducer()