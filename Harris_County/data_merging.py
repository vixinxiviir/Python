import pandas as pd
import numpy as np

brac_frame = pd.read_stata("C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/texas_breath_tests.dta")
brac_frame.drop(brac_frame[brac_frame["cnty"] != "101"].index, inplace=True)  # Filtering out everything that isn't
# Harris County
brac_frame["full_name"] = brac_frame["fname"] + " " + brac_frame["lname"]  # Creating a full name variable
brac_frame["cdot"] = brac_frame["cdot"].astype(str)  # Making our data a string variable
brac_frame = brac_frame.rename(columns={"aofficer": "off_full"})

crime_frame = pd.read_csv("C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/all_precincts_cases.csv")
crime_frame["full_name"] = crime_frame["Def First Name "] + " " + crime_frame["Def Last Name"]
crime_frame["full_name"] = crime_frame["full_name"].str.upper()
crime_frame["Filed Date"] = pd.to_datetime(crime_frame["Filed Date"])
crime_frame["year"] = crime_frame["Filed Date"].dt.year
crime_frame["year"] = crime_frame["year"].astype(int)
crime_frame.drop(crime_frame[crime_frame["year"] < 2004].index, inplace=True)
crime_frame["off_last_name"] = crime_frame["Officer Name"].str.split(",").str[0]
crime_frame["off_last_name"] = crime_frame["off_last_name"].str.upper()
crime_frame["off_first_name"] = crime_frame["Officer Name"].str.split(",").str[1]
crime_frame["off_first_name"] = crime_frame["off_first_name"].str[1]
crime_frame["off_full"] = crime_frame["off_last_name"] + " " + crime_frame["off_first_name"]

full_frame = crime_frame.merge(brac_frame, on=["full_name", "off_full"])
full_frame = full_frame.drop_duplicates()
full_frame.to_csv("C:/Users/Re(d)ginald/Documents/RA Stuff/harris county/input_data/cases_brac.csv")

