from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("Chipotle").getOrCreate()

# Converting rdd to a Dataframe
rdd = spark.sparkContext.parallelize([(1,'A',25), (2,'B',30), (3, 'C', 32)])
rdd.collect()
type(rdd)
# help(rdd)
df = rdd.toDF(["id", "name", "age"])
df.show()
type(df)

df.write.option("header", True).csv("people.csv",mode="overwrite")

# Loading data as raw text
# With RDD, its more manual work
sc = spark.sparkContext
rdd = sc.textFile("people.csv")
rdd.collect()
rdd.count()

# Split each line and filter
rdd = rdd.filter(lambda row: row != "id,name,age")
filtered = rdd.map(lambda line: line.split(',')).filter(lambda row: int(row[2]) > 30)
filtered.collect()

df = spark.read.options(header=True, inferSchema=True).csv("people.csv")
df.show()

from pyspark.sql.functions import col
df.filter(col("age") > 30).show()

spark.stop()