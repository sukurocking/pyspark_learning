from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("find_word").getOrCreate()
sc = spark.sparkContext
my_file = sc.textFile("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark-prep/example.txt")

def isFound(line):
    if line.find("testing") > -1:
        return 1
    else:
        return 0
foundbits = my_file.map(isFound)
sum_bits = foundbits.reduce(sum)
if sum_bits > 0:
    print("Exists")

spark.stop()