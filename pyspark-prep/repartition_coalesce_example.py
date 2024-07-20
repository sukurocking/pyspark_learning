from pyspark.sql import SparkSession

spark = SparkSession.Builder.appName("partitioning example").getOrCreate()

# Creating a dataframe
data = [("Sukumar", 31), ("Manoj", 32)]
spark_df = spark.createDataFrame(data,schema=["Name", "Age"])

print(spark_df)
