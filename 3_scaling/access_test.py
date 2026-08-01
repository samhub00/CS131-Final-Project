import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler

input_path = sys.argv[1]
#output_path = sys.argv[2]

spark = SparkSession.builder.appName("access_test").getOrCreate() 

df = (spark.read.format("csv")
	.option("inferSchema", "true")
	.option("header", "true")
	.load(input_path))

df.show(5)
df.printSchema()

spark.stop()