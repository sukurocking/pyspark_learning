from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("test").getOrCreate()
file_path = "/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/Chipotle/chipotle.tsv"
raw_df = spark.read.option("header", True).option("delimiter", "\t").option("inferschema",True).csv(file_path)
raw_df.count()
# print(raw_df_count)
spark.stop()