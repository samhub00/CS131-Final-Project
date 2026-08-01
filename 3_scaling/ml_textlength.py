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

#df.show(5)
#df.printSchema()

# select game name column
# count occurrences of each game name
# order by count column descending
# show 15 rows
#df.select("game").groupBy("game").count().orderBy("count", ascending=False).show(15)

# filter data first
# get review column and review status (review, voted_up)
s1 = df.select("review", "voted_up")
# convert review column into string length of column
s1.withColumn("review_length", F.length("review")).show(15)

spark.stop()
