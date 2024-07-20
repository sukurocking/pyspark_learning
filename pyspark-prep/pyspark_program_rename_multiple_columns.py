from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("rename_columns").getOrCreate()
my_df = spark.createDataFrame(data=[
    ("Amit", 35),
    ("Rajoria", 34)
], schema=["Name", "Age"])
my_df.show()


existing_cols = my_df.columns
new_cols = ["class_" + col for col in my_df.columns]
cols_mapping = {k:v for k, v in zip(existing_cols, new_cols)}
my_new_df = my_df.withColumnsRenamed(cols_mapping)
my_new_df.show()

spark.stop()
