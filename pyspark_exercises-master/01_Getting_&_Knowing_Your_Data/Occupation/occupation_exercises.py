# Import necessary libraries
from pyspark.sql import SparkSession
import os
spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("occupation") \
    .getOrCreate()

os.chdir("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/Occupation")
os.getcwd()
!wget "https://raw.githubusercontent.com/justmarkham/DAT8/master/data/u.user"

users = spark.read \
    .option("header", True) \
    .option("inferschema", True) \
    .option("delimiter", "|") \
    .csv("pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/Occupation/u.user")
# print(spark.sparkContext.getConf().get("spark.sql.warehouse.dir"))

users.show(5)
users.columns

# See the first 25 entries
users.head(25)
users.printSchema()

# See the last 10 entries
users.tail(10)

# Get the number of partitions in the df
users.rdd.getNumPartitions()

# Number of observations in the dataset
users.count()

# Number of columns in the dataset
len(users.columns)

# Print the name of all the columns
print(users.columns)

# What is the data type of each column
users.printSchema()

# Print only the occupation column
users.select("occupation").show()

# How many different occupations are in the dataset
users.select("occupation").distinct().count()

# Alternative method
from pyspark.sql.functions import count_distinct, col
users.agg(count_distinct("occupation").alias("distinct_occupation_count")).show()

# Column renaming testing
users.withColumnRenamed("occupation", "occupation_1").select("occupation_1").show(10)
users.select(col("occupation").alias("occupation_new")).show(5)

# Most frequent occupation
occupation_wise_count = users.groupBy("occupation").count()
occupation_wise_count.orderBy(col("count").desc()).limit(1).show()

# Summarize the dataframe
users.describe().show()

# Summarize only the occupation column
users.select("occupation").describe().show()

# What is the mean age of users
users.agg({"age": "mean"}).show()
from pyspark.sql.functions import avg
users.agg(avg("age").alias("avg_age")).show()


# What is the age with the least occurrence
users.groupBy("age")\
    .count().orderBy(col("count").asc())\
    .limit(1).show()




















# Age with the least occurrence
users.groupBy("age")\
    .count()\
    .orderBy("count")\
    .limit(1).show()


spark.stop()