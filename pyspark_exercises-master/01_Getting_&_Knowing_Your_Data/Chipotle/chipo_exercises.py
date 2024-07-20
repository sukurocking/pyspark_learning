from pyspark.sql import SparkSession, functions as f
from pyspark.sql.types import FloatType

spark = SparkSession \
    .builder \
    .master("local[1]") \
    .appName("spark_practise") \
    .getOrCreate()

# Importing from the address
folder = "/Users/sukumarsubudhi/Downloads/Learning/Pyspark/"
file_path = "pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/Chipotle/chipotle.tsv"
absolute_file_path = folder + file_path
# print(absolute_file_path)
chipo = spark.read \
    .option("inferSchema", True) \
    .option("header", True) \
    .option("delimiter", '\t') \
    .csv(absolute_file_path)
chipo.show(5)

# Total number of observations in the dataset
print(chipo.count())

# Step 6. What is the number of columns in the dataset?
print(len(chipo.columns))

# Step 7. Print the name of all the columns.
print(chipo.columns)

# Step 9. Which was the most-ordered item?
chipo_item_w_cnt = chipo.groupBy(f.col("item_name")).sum("quantity").withColumnRenamed("sum(quantity)", "qty_sum")
chipo_item_w_cnt.orderBy("qty_sum", ascending=False).show(5, False)

# Step 10. For the most-ordered item, how many items were ordered?
# chipo.groupBy(f.col("item_name"))\
#     .agg(f.sum(f.col("quantity")).alias("qty_sum"))\
#     .orderBy("qty_sum", ascending=False)\
#     .show(5,False)

# noinspection PyStatementEffect
chipo \
    .groupBy("item_name") \
    .agg(f.sum(f.col("quantity")).alias("qty_sum")) \
    .sort(f.col("qty_sum").desc()).select("qty_sum").first()[0]

spark.stop()

# Step 11. What was the most ordered
# item in the choice_description column?

chipo.select("choice_description", "quantity") \
    .groupBy("choice_description") \
    .agg(f.sum(f.col("quantity")).alias("qty_sum")) \
    .orderBy("qty_sum", ascending=False) \
    .show(5, False)

# Step 12. How many items were orderd in total?
chipo.groupBy().sum("quantity").show()

# Step 13. Turn the item price into a float
pattern_to_replace = r"\$"
chipo = chipo.withColumn("item_price1", f.regexp_replace(string=f.col("item_price"), pattern=pattern_to_replace, replacement=""))
chipo.show(5)
chipo = chipo.withColumn("item_price2", f.col("item_price1").cast("float"))
chipo.printSchema()

# Step 13.b. Create a lambda function and change the type of item price
from pyspark.sql.functions import udf, regexp_replace, col
clean_dollar_col_udf = udf(lambda a_col: float(a_col.replace("$", "")), FloatType())

# udf(lambda a_col: regexp_replace(string=a_col, pattern=pattern_to_replace, replacement="").cast("float"), FloatType()))
chipo1 = chipo.withColumn("item_price3",
                          clean_dollar_col_udf("item_price")
                          )
chipo1.show(5)
chipo1.select("item_price3").printSchema()