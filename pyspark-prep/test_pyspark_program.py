from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
spark = SparkSession.builder.appName("test_lit").getOrCreate()

data = [("Manoj", 32),
        ("Suraj", 33)
        ]
columns = ["Name", "Age"]
spark_df = spark.createDataFrame(data=data, schema=columns)
spark_df.show(20, truncate=False)
spark_df_new = spark_df.withColumn("Default tag", lit("Born before 2000"))
spark_df_new.show(20,False)
spark.stop()
