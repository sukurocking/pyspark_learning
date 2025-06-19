from pyspark.sql import SparkSession, functions as F
import requests
spark = SparkSession.builder.master("local[1]").appName("spark_exercises").getOrCreate()
url = "https://raw.githubusercontent.com/justmarkham/DAT8/master/data/u.user"
file_path = "/pyspark_exercises-master/data/u.user"

# Writing the response from the url into a text file
response = requests.get(url)
data = response.text
with open(file_path, "w") as myfile:
    myfile.write(data)

users = spark.read\
    .option("delimiter", "|")\
    .option("header", True)\
    .option("inferschema",True)\
    .csv(file_path)

users.show()
occ_wise_count = users.groupBy(F.col("occupation")).count()
occ_wise_count.orderBy("count", ascending=False).show(1)
spark.stop()
