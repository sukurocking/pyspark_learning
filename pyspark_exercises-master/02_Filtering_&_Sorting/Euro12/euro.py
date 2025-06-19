from more_itertools import first
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession\
    .builder\
    .master("local[*]")\
    .appName("euro")\
    .getOrCreate()

euro12 = spark \
    .read \
    .option("inferschema", True) \
    .option("header", True) \
    .csv("pyspark_exercises-master/data/Euro_2012_stats_TEAM.csv")

euro12.show(5, truncate=True)
euro12.printSchema()

# 4. select only the Goal column
euro12.select("Goals").show(5)

# 5. How many teams participated in Euro2012
# euro12.agg({"Team": "count_distinct"}).show()
euro12.agg(F.count_distinct("Team").alias("N_teams")).show()
euro12.count()

# 6. Number of columns in the dataset
len(euro12.columns)

# 7. 
discipline = euro12.select(["Team", "Yellow Cards", "Red Cards"])
discipline.show(5, truncate=False)


# 8. Sort the team by Red Cards, then to Yellow Cards
discipline\
    .orderBy(F.col("Red Cards").asc())\
    .orderBy(F.col("Yellow Cards").asc())\
    .show()
discipline.orderBy(["Red Cards", "Yellow Cards"], ascending=[False, False]).show()
help(discipline.orderBy)

# 9. Calculate the mean Yellow Cards given per Team
discipline.agg(F.mean("Yellow Cards").alias("mean_Yellow_cards")).show()

# 10. Filter teams that scored more than 6 goals
euro12 \
    .filter(F.col("Goals") > 6) \
    .select(["Team", "Goals"]).show()

# 11. Select the teams that start with G
## Method1
euro12 \
    .filter(F.col("Team").startswith("G"))\
    .select("Team").show()
## Method2
euro12.filter("Team like 'G%'").select("Team").show()
euro12.printSchema()

## Method3
euro12\
    .filter(F.regexp_extract(F.col("Team"),pattern=r"^G", idx=0)=="G")\
    .select("Team")\
    .show()

## Method4
euro12.filter(F.col("Team").like("G%")).select("Team").show()

euro12.filter(F.col("Team").rlike("^G")).select("Team").show()


help(F.regexp_extract)

# 12. Select the first 7 columns
first_7_cols = euro12.columns[:7]
# first_7_cols_esc = [F.col(name) for name in first_7_cols]
# Backtick Escaping the columns
first_7_cols_esc = [f"`{name}`" for name in first_7_cols]
first_7_cols_esc

euro12.selectExpr(*first_7_cols_esc).show(5)
# print(*first_7_cols_esc)

# Select all columns except the last 3
excl_last3_cols = euro12.columns[:-3]
excl_last3_cols_esc = [f"`{name}`"for name in excl_last3_cols]
excl_last3_cols_esc
euro12.selectExpr(*excl_last3_cols_esc).show(5)

help(euro12.selectExpr)

# Present only the Shooting Accuracy from England, Italy and Russia
euro12.select("Team").show()

euro12.filter("Team in ('England', 'Italy', 'Russia')").select("Team").show()
euro12\
    .filter(F.col("Team").isin(['England', 'Italy', 'Russia']))\
    .select(["Team", "Shooting Accuracy"])\
    .show()

spark.stop()