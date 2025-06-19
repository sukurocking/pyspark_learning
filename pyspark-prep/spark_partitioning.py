from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]") \
    .appName("partitioning_practise") \
    .getOrCreate()

df = spark.read.option("header", True)\
    .option("inferSchema", True)\
    .csv("data/country_vaccinations.csv")
df.show()
df.printSchema()
# repartition, coalesce, partitionby
df.rdd.getNumPartitions()
df.count()

def show_partition(index, iterator):
    yield (f"Partition {index}", list(iterator))

# Display data in each partition
# Get just 10 total records spread across partitions
sample_data = df.rdd.take(10)  # Returns list of Row objects

# See which partition they came from
from pyspark.sql.functions import spark_partition_id
df.withColumn("partition_id", spark_partition_id()).select("country", "date", "partition_id").show(10)




spark.stop()