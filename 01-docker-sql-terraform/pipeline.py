import sys
import pandas as pd

print(sys.argv)

try:
    day = sys.argv[1]
except IndexError:
    print("Please provide a day")
    sys.exit(1)

# some fancy data processing

print(f"Done processing data from day {day}")