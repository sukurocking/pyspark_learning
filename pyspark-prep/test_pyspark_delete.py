from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col

spark = SparkSession.builder.appName("test").getOrCreate()
data = [
    ("Ajay", 24),
    ("Vijay", 23)
]
columns = ["Name", "Age"]
df = spark.createDataFrame(data, columns)
df.show()

new_df = df.withColumn("Age_after_5_years", col("Age") + 5).withColumnRenamed("Name", "Name1")
new_df.show()
spark.stop()