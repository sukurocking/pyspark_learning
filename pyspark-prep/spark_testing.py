import os
import sys

print("PYTHONPATH:", os.environ.get('PYTHONPATH'))
print("sys.path:", sys.path)

from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
print("Spark version:", spark.version)
spark.stop()