from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

spark = SparkSession.builder.master("local").appName("window_practise").getOrCreate()

# spark.sparkContext.uiWebUrl
# Define the schema for clarity and type safety
schema = StructType([
    StructField("EmpID", IntegerType(), True),
    StructField("EmpName", StringType(), True),
    StructField("Department", StringType(), True),
    StructField("Salary", DoubleType(), True)
])

# Dummy data for the DataFrame
data = [
    (101, "Alice", "HR", 60000.00),
    (102, "Bob", "Engineering", 95000.00),
    (103, "Charlie", "HR", 75000.00),
    (104, "David", "Sales", 80000.00),
    (105, "Eve", "Engineering", 110000.00),
    (106, "Frank", "HR", 62000.00),
    (107, "Grace", "Sales", 85000.00),
    (108, "Heidi", "Engineering", 90000.00),
    (109, "Ivan", "Sales", 78000.00),
    (110, "Judy", "HR", 70000.00),
    (111, "Alice", "Marketing", 65000.00), # Another Alice in a different department
    (112, "Bob", "Sales", 82000.00),     # Another Bob in a different department
    (113, "Kevin", "Engineering", 105000.00),
    (114, "Liam", "Marketing", 72000.00),
    (115, "Mia", "HR", 68000.00),
    (116, "Nora", "Engineering", 120000.00),
    (117, "Oscar", "Sales", 90000.00),
    (118, "Paul", "Marketing", 69000.00)
]

# Create the DataFrame
df = spark.createDataFrame(data, schema=schema)

# Show the DataFrame to verify
print("Employee DataFrame:")
df.show()

# Find the out the top 2 highest salaries per department in the dataframe
from pyspark.sql.window import Window
from pyspark.sql.functions import col, row_number,desc, rank

window_spec = Window.partitionBy("Department").orderBy(desc("Salary"))
df.withColumn("row", rank().over(window=window_spec)) \
    .filter(col("row") <= 2) \
    .drop("row") \
    .show()


window_spec = Window.partitionBy("Department").orderBy(col("Salary").desc())
df.withColumn("row", row_number().over(window=window_spec)) \
    .filter(col("row") <= 2) \
    .show()

spark.stop()
