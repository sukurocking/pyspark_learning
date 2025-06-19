from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.master("local[*]").appName("most_freq_words").getOrCreate()
s = "A quick brown fox jumps over the lazy dog the the the"
df = spark.createDataFrame([[s,]], schema="text string")
df.show()
df.printSchema()

df = df.withColumn("words", F.split("text", " "))
df = df.withColumn("new_words", F.explode("words"))
df = df.select("new_words")

# Most frequent elements
df_cnt = df.groupBy("new_words").count().orderBy(F.desc("count"))
df_cnt.show()
df_cnt.limit(1).show()

spark.stop()

