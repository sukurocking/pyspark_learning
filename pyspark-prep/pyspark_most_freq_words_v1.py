from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession.builder.master("local[*]").appName("most_freq_words").getOrCreate()
s = "A quick brown fox jumps over the lazy dog the the the"
df = spark.createDataFrame([(s,)], ["text"])
df.show()
df.printSchema()

type([(s,)])
type([(s,)][0])
words_df = df.withColumn("word", F.split("text", " "))
words_df.show()
words_df = words_df.withColumn("word_new", F.explode("word"))
words_df = words_df.select("word_new")
words_df.show()
words_df_grp = words_df.groupBy("word_new").count()
words_df_grp.orderBy(F.desc("count")).limit(1).show()
