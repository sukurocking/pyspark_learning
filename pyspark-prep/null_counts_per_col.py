from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[*]").appName("null_count_practise").getOrCreate()
data = [
    (1,'Sagar',23),
    (2, None, 30),
    (None, 'Raja', 40),
    (5, 'ABC', None),
    (4, 'XYZ', None)
    ]
schema = "ID int, Name string, Age int"
df = spark.createDataFrame(data,schema)
df.show()

from pyspark.sql.functions import count, col, when
df1 = df.select([count(when(col(i).isNull(), i)).alias(i) for i in df.columns])
df1.show()

spark.stop()