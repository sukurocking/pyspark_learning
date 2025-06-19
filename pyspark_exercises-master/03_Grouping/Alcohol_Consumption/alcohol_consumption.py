from pyspark.sql import SparkSession
import pyspark.sql.functions as F
spark = SparkSession\
    .builder.master("local[*]")\
    .appName("alcohol_consumption")\
    .getOrCreate()
# import os
# os.system("wget 'https://raw.githubusercontent.com/justmarkham/DAT8/master/data/drinks.csv'")

# help(os.system)
# os.getcwd()

# 3. Assign to a variable called drinks
drinks = spark.read\
    .options(inferSchema=True, header=True)\
    .csv("pyspark_exercises-master/data/drinks.csv")
drinks.show(5)
drinks.printSchema()

# 4. Which continent drinks more beer on average
drinking_continent = drinks\
    .groupBy("continent")\
    .agg(F.mean("beer_servings").alias("avg_beer_servings"))\
    .orderBy(F.desc("avg_beer_servings"))\
    .limit(1)
drinking_continent.show()
drinking_continent.explain()

help(drinks.head)
help(drinks.limit)

# 5. For each continent print the statistics for wine consumption
# drinks.select(["continent", "wine_servings"]).groupBy("continent").describe()
# drinks.describe().show()
ps_drinks = drinks.select(["continent", "wine_servings"]).pandas_api()
type(ps_drinks)
type(drinks)

type(ps_drinks.groupby("continent").describe())

# 6. Print the mean alcohol consumption per continent for every column
mean_alcohol_consumption = (drinks
 .groupBy("continent")
 .agg(
        F.mean("total_litres_of_pure_alcohol").alias("mean_alcohol_consumption"),
        F.median("total_litres_of_pure_alcohol").alias("median_alcohol_consumption")
      )
)
mean_alcohol_consumption.explain()
mean_alcohol_consumption.show()

# 8. Print the mean, min and max values for spirit consumption
drinks.agg(
    F.mean("spirit_servings").alias("mean_spirit_servings"),
    F.min("spirit_servings").alias("min_spirit_servings"),
    F.max("spirit_servings").alias("max_spirit_servings")
).show()

help(drinks.agg)


spark.stop()