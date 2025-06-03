# Importing necessary libraries
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[2]").appName("Chipotle").getOrCreate()
help(spark)
help(spark.read.csv)
help(spark.read.options)
# Reading the data and assigning the data to a variable called chipo
chipo = (spark.read.options(header= True, delimiter= "\t", inferSchema=True).
         format("csv").
         load("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/Chipotle/chipotle.tsv")
         )
chipo.show()
chipo.printSchema()


# First 10 entries
chipo.show(10)

# Number of observations in the dataset
chipo.count()

# Number of columns in the dataset
len(chipo.columns)

# Printing columns in the dataset
print(chipo.columns)

# Most ordered item and how many
chipo.groupBy('item_name').count().orderBy('count',ascending=False).limit(1).show()


# What was the most ordered item in the choice description column?
chipo.groupBy("choice_description").count().orderBy("count", ascending=False).limit(2).show()


# How many items were ordered in total?
chipo.select("item_name").distinct().count()

# Alternative method
from pyspark.sql.functions import countDistinct, count_distinct
chipo.agg(countDistinct("item_name")).show()
chipo.agg(count_distinct("item_name")).show()


# Turn the item price into a float
from pyspark.sql.functions import regexp_replace, col
# Removing $ sign from the column
chipo2 = chipo.withColumn("item_price", regexp_replace('item_price', "\$", ""))
chipo2 = chipo2.withColumn("item_price", col("item_price").cast('double'))
chipo2.show(10)
chipo2.printSchema()

# Create a lambda function and change the type of item price
from pyspark.sql.functions import udf, sum
from pyspark.sql.types import DoubleType
clean_cast_udf = udf(lambda x: float(x.replace('$', '')), DoubleType())
chipo3 = chipo.withColumn("item_price", clean_cast_udf(col("item_price")))
chipo3.show(10)
chipo3.printSchema()


# How much was the revenue for the period in the dataset?
chipo3 = chipo2.withColumns({"quantity", "item_price"}, col("quantity") * col("item_price")).alias("order_amt")
chipo3 = chipo2.withColumn("order_amt", col("quantity") * col("item_price"))
chipo3.agg(sum("order_amt").alias("revenue")).show()

# How many orders were made during the period
n_orders = chipo3.count()

# What is the average revenue per order
revenue = chipo3.agg(sum("order_amt")).collect()[0][0]
avg_revenue_per_order = revenue / n_orders

# How many different items are sold?
chipo3.show()
from pyspark.sql.functions import count_distinct
chipo3.agg(count_distinct("item_name")).show()

# spark.sparkContext.uiWebUrl
spark.stop()