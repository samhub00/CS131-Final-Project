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

filtered_df = df.filter(df["author_playtime_at_review"].isNotNull())

from pyspark.sql import functions as F

# Total average playtime per game (in minutes and hours)
avg_playtime_df = df.groupBy("appid", "game").agg(
    F.avg("author_playtime_at_review").alias("avg_playtime_minutes"),
    (F.avg("author_playtime_at_review") / 60).alias("avg_playtime_hours")
)

#avg_playtime_df.select("game").groupBy("game").count().orderBy("avg_playtime_minutes", ascending=False).show(20)

avg_playtime_df.show()

