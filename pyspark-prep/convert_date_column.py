# Convert a column with dates in the format 'MM/DD/YYYY' to a datetime object in Python.
import pandas as pd
datetime_val = '01/10/2023' # 10th Jan 2023

print(pd.to_datetime(datetime_val, format="%m/%d/%Y"))