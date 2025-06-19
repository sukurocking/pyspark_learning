from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.master("local[*]").appName("fictional_army").getOrCreate()
# Create an example dataframe about a fictional army
raw_data = {'regiment': ['Nighthawks', 'Nighthawks', 'Nighthawks', 'Nighthawks', 'Dragoons', 'Dragoons', 'Dragoons', 'Dragoons', 'Scouts', 'Scouts', 'Scouts', 'Scouts'],
            'company': ['1st', '1st', '2nd', '2nd', '1st', '1st', '2nd', '2nd','1st', '1st', '2nd', '2nd'],
            'deaths': [523, 52, 25, 616, 43, 234, 523, 62, 62, 73, 37, 35],
            'battles': [5, 42, 2, 2, 4, 7, 8, 3, 4, 7, 8, 9],
            'size': [1045, 957, 1099, 1400, 1592, 1006, 987, 849, 973, 1005, 1099, 1523],
            'veterans': [1, 5, 62, 26, 73, 37, 949, 48, 48, 435, 63, 345],
            'readiness': [1, 2, 3, 3, 2, 1, 2, 3, 2, 1, 2, 3],
            'armored': [1, 0, 1, 1, 0, 1, 0, 1, 0, 0, 1, 1],
            'deserters': [4, 24, 31, 2, 3, 4, 24, 31, 2, 3, 2, 3],
            'origin': ['Arizona', 'California', 'Texas', 'Florida', 'Maine', 'Iowa', 'Alaska', 'Washington', 'Oregon', 'Wyoming', 'Louisana', 'Georgia']}

# Converting to a list of dictionaries, so that Spark can 'digest'
columns = raw_data.keys()
n_rows = len(raw_data['regiment'])
data_converted = []

# print(columns)

for i in range(n_rows):
    row_dict = {column:raw_data[column][i] for column in columns}
    data_converted.append(row_dict)

army = spark.createDataFrame(data_converted)
# Maintaining the order of the columns
army = army.select(list(columns))
army.show()
army.printSchema()

# 5. Print only the column veterans
army.select("veterans").show(5)

# 6. Print the columns veterans and deaths
army.select(["veterans", "deaths"]).show(5)

# 7. Print the name of all the columns
print(army.columns)

# 8. Select the 'deaths', 'size' and 'deserters' columns from Maine and Alaska
(army
    .filter(F.col("origin").isin(['Maine', 'Alaska']))
    .select(["deaths","size", "deserters"])
    .explain()
)

# 9. select the rows 3 to 7 and the columns 3 to 6 - Probably not required
row_start = 3
row_end = 7
column_start = 3
column_end = 6


# army.limit(row_end).tail(row_end - row_start + 1)
help(army.head)


# 10. select every row after the fourth row and all columns - Probably not required


# 13. select rows where deaths greater than 50
army.filter("deaths > 50").show()

# 14. select rows where deaths > 500 or less than 50
army.filter((F.col("deaths") > 500) | (F.col("deaths") < 50)).show()
# help(army.filter)

# 15. select all the regiments not named "Dragoons"
army.filter("regiment != 'Dragoons'").show()

# 16. select the rows with Texas and Arizona
army.where(F.col("origin").isin(['Texas', 'Arizona'])).show()


spark.stop()