# Write a function to fill missing values in a DataFrame column with the mean of that column.

import pandas as pd
import numpy as np
sample_dict = ({"name":"Sukumar", "age": 22}
               ,{"name":"Sushree", "age": 21}
               ,{"name":"XYZ", "age": np.nan}
               )

df = pd.DataFrame(sample_dict)

print(df)

df["age"] = df["age"].fillna(np.mean(df["age"]))

print(df)