from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
file_path = "/pyspark_exercises-master/data/chipotle.tsv"
raw_df = spark.read.option("header", True).option("delimiter", "\t").option("inferschema",True).csv(file_path)
raw_df.count()
# print(raw_df_count)
spark.stop()