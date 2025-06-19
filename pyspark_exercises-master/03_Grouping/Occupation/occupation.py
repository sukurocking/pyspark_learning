from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("occupation")\
    .getOrCreate()

users = (spark
         .read
         .options(header=True, inferSchema=True, delimiter="|")
         .csv("pyspark_exercises-master/data/u.user"))

users.show(5)

# 4. Discover what is the mean age per occupation
users.groupBy("occupation").mean("age").show()

(users
 .groupBy("occupation")
 .agg(F.mean("age").alias("avg_age"))
 .orderBy("avg_age")
 .show()
)
users.show(5)
# 5. Discover the male ratio per occupation and sort it from the most to the least
(users
 .withColumn("male_gender_flag",
             F.when(F.col('gender')=='M', 1).otherwise(0)
             )
 .groupBy("occupation")
 .agg(F.avg("male_gender_flag").alias("male_ratio"))
 .orderBy(F.desc("male_ratio"))
 .explain()
)

# Alternative method
(users
 .groupBy("occupation")
 .pivot("gender")
 .count()
 .withColumn("male_ratio", F.col("M") / (F.col("M") + F.col("F")))
 .orderBy(F.desc("male_ratio"))
 .select("occupation", "male_ratio")
 .show(30)
)
# help(F.when)

# No of unique occupations
users.agg(F.count_distinct("occupation")).show()


# 6. For each occupation, calculate the minimum and maximum ages
users.groupBy("occupation").agg(F.min("age").alias("min_age"), F.max("age").alias("max_age")).show()

# 7. For each combination of occupation and gender, calculate the mean age
help(users.groupBy)
(users
 .groupBy('occupation', 'gender')
 .mean('age')
 .orderBy('occupation', 'gender')
 .show(30)
)

# 8. For each occupation present the percentage of women and men
users \
    .groupBy('occupation') \
    .pivot("gender") \
    .count() \
    .na.fill(0) \
    .withColumn("percent_women", F.round(
    (F.col("F") * 100.0) / (F.col("F") + F.col("M")),2)
                ) \
    .withColumn("percent_men", F.round(
    (F.col("M") * 100.0) / (F.col("F") + F.col("M")),2)
                ) \
    .show(30)
    # .explain()


spark.stop()