# Python program to count the number of words in a text file
from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local[1]").appName("word_count").getOrCreate()
sc = spark.sparkContext
abc = sc.textFile("/Users/sukumarsubudhi/Downloads/Learning/Pyspark/pyspark-prep/example.txt")
print(abc.collect())
all_words = abc.flatMap(f=lambda x: x.split(" "))
print("Total number of words in the file: ", all_words.count())
spark.stop()
