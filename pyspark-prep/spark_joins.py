from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col, when, lit
spark = SparkSession.builder.appName("joins").getOrCreate()

# First DataFrame
df1 = spark.createDataFrame([
    (1, "John", "IT", None),
    (2, "Mike", "HR", 5000),
    (3, "Sara", None, 6000),
    (4, None, "Finance", 4500)
], ["id", "name", "department", "salary"])

# Second DataFrame
df2 = spark.createDataFrame([
    (1, "John", "New York"),
    (2, None, "Chicago"),
    (3, "Sara", "Boston"),
    (5, "Tom", "Seattle")
], ["id", "name", "city"])


# Inner join
df1.join(df2, on="id", how="inner").show()

# Left join
df1.join(df2, on="id", how="left").show()

# Handle duplicate columns in join (name column is duplicated in this case)

(df1.join(df2.withColumnRenamed("name", "df2_name"), on="id", how="left")
 .withColumn("name", coalesce(col("name"), col("df2_name"), lit("Unknown")))
 .drop(col("df2_name"))
 .show()
 )

# Full join with comprehensive null handling
df1.show()
df2.show()

df1.join(df2, on="id", how="full").show()
df1.join(df2, on="id", how="full").select(
    coalesce(df1.id, df2.id).alias("id"),
    coalesce(df1["name"], df2["name"]).alias("name"),
    coalesce(df1["department"], lit("No dept")).alias("department"),
    coalesce(df1["salary"], lit(0)).alias("salary"),
    coalesce(df2["city"], lit("Unknown")).alias("city")
).show()

# df1.select(df1["id"]).show()
# df1.select("id").show()

# Conditional null handling
df1_alias = df1.withColumnRenamed("name", "name_df1")
df2_alias = df2.withColumnRenamed("name", "name_df2")
df1_alias.show()
df2_alias.show()
full_joined = df1_alias.join(df2_alias, on="id", how="full")
full_joined.show()
conditional_join = full_joined.select(
    col("id"),
    when(col("name_df1").isNull() & col("name_df2").isNull(), lit("Unknown"))
        .when(col("name_df1").isNull(), col("name_df2"))
        .when(col("name_df2").isNull(), col("name_df1"))
        .otherwise(col("name_df1"))
        .alias("refined_name")
    )
conditional_join.show()


full_joined.na.fill({
    "city": "Unknown",
    "department": "No dept",
    "salary": 0,
    "city": "No city"
}).drop("name_df2").show()

spark.stop()