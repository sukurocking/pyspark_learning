from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("food_facts") \
    .getOrCreate()

food = spark.read \
    .option("header", True) \
    .option("delimiter", "\t") \
    .option("inferschema", True) \
    .csv("pyspark_exercises-master/01_Getting_&_Knowing_Your_Data/World Food Facts/en.openfoodfacts.org.products.tsv")

# See the first 5 entries
food.head(5)
food.show(5)

# Number of observations in the dataset
food.count()
# 356027

# 6. Number of columns in the dataset
len(food.columns)

# 7. Print the name of all the columns
print(food.columns)

# 8. What is the name of the 105th column
N = 105
print(food.columns[N-1])

# 9. What is the type of the observations of the 105th column
food.select(food.columns[N-1]).printSchema()
food.select(food.columns[N-1]).head(5)

# Which records have non-null values of -glucose_100g column
from pyspark.sql.functions import col
food.filter(col("-glucose_100g").isNull() == False).select("-glucose_100g").show(5)

# 11. What is the product name of the 19th observation
N = 19
print(food.limit(N).select("product_name").tail(1)[0][0])

spark.stop()