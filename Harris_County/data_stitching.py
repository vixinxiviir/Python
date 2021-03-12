import pandas as pd
import numpy as np


def append_data(data_frame, precinct, input_year, input_month):   # Our append function
    append_frame = pd.read_csv(
        "C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/" + precinct + "/" + input_year + "/month" +
        input_month + ".txt", delimiter="\t")  # Reading in our file into a separate data frame from the main one.
    frames = [data_frame, append_frame]
    result = pd.concat(frames)  # Concat is the pandas append function. We pass in a list because that's what it wants.
    return result


main_frame = pd.DataFrame()  # We want to start with an empty dataframe so the loop works for all files.
checker = 1  # This just goes up with the files so I know the script is actually doing something.
p = 305  # Precinct number
while p <= 380:
    year = 1993  # This and the one below are to reset the month and year timers after each successful loop.
    while year <= 2020:
        month = 1
        while month <= 12:
            p = str(p)  # We need these in string form so the function can use them in the filename.
            month = str(month)
            year = str(year)
            main_frame = append_data(main_frame, p, year, month)  # Making our blank frame the new appended version.
            print(checker)
            month = int(month)
            year = int(year)
            p = int(p)
            month = month + 1
            checker = checker + 1
        year = year + 1
    p = p + 5

main_frame.to_csv("C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/all_precincts_cases.csv",
                  index=False)  # Reading it out successfully (I hope!).
