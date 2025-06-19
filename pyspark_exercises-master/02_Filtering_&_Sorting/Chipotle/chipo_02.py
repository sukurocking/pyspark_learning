from os import truncate
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import DoubleType

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("chipo") \
    .getOrCreate()

chipo = spark.read \
    .option("header", True) \
    .option("inferschema", True) \
    .option("delimiter", "\t") \
    .csv("pyspark_exercises-master/data/chipotle.tsv")

chipo.show(5)

# 4. How many products cost more than $10
# We need to clean the price column
chipo = chipo.withColumn("item_price_clean", F.regexp_replace("item_price", "\$", replacement=""))\
    .withColumn("item_price_clean", F.col("item_price_clean").cast(DoubleType())) \
    
# help(F.cast)
chipo.printSchema()

chipo\
    .filter(F.col("item_price_clean") > 10)\
    .agg(F.count_distinct("item_name")) \
    .show()

chipo.filter("item_price_clean > 10")\
    .agg(F.count_distinct("item_name").alias("n_distinct_items")).show()

# help(chipo.filter)
# help(chipo.agg)

chipo = chipo.withColumn("item_price_cleaned",
                 F.regexp_replace(F.col("item_price"),
                                  pattern="\$",
                                  replacement="") \
                 .cast("double"))
chipo.filter(F.col("item_price_cleaned") > 10).show(truncate=False)
chipo.filter(chipo["item_price_cleaned"] > 10)\
    .agg(F.count_distinct("item_name").alias("distinct_items_count"))\
    .show()

# 5. What is the price of each item?
chipo.show(n=5, truncate=False)
help(chipo.show)
chipo.count()

chipo\
    .select(["quantity", "item_name", "item_price"])\
    .filter("quantity=1")\
    .select(["item_name", "item_price"])\
    .distinct().show(50, truncate=False)

# help(spark)
# help(spark.sparkContext)
# spark.sparkContext.uiWebUrl


# 6. Sort by the name of the item
help(chipo.orderBy)
chipo.orderBy(F.col("item_name").asc()).show()

# 7. What was the quantity of the most expensive item ordered?

# Maximum item price
max_item_price = chipo.agg({"item_price_clean":"max"}).rdd.collect()[0][0]

chipo\
    .filter(F.col("item_price_clean")==max_item_price)\
    .select("quantity")\
    .show()

# 8. How many times was a Veggie Salad Bowl ordered
chipo \
    .filter(F.col("item_name")=="Veggie Salad Bowl") \
    .count()

# 9. How many times did someone order more than one Canned Soda
chipo \
    .filter((F.col("item_name")=="Canned Soda") & (F.col("quantity") > 1)) \
    .count()


spark.stop()