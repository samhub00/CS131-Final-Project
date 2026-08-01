import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler

input_path = sys.argv[1]
#output_path = sys.argv[2]

spark = SparkSession.builder.appName("top15reviewedgames").getOrCreate() 

df = (spark.read.format("csv")
	.option("inferSchema", "true")
	.option("header", "true")
	.load(input_path))

df.show(5)
df.printSchema()

# select game name column
# count occurrences of each game name
df.select("game").groupBy("game").count().orderBy("count").show(15)
# sort by ocurrences, descending order
#df.sort("count", ascending=False)

spark.stop()
