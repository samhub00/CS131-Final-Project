import sys
import time


start_time = time.time()


from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType

input_path = sys.argv[1]
#output_path = sys.argv[2]

spark = SparkSession.builder.appName("access_test").getOrCreate() 

schema = StructType([
    StructField('recommendationid', LongType(), True),
    StructField('appid', IntegerType(), True),
    StructField('game', StringType(), True),
    StructField('author_steamid', LongType(), True),
    StructField('author_num_games_owned', IntegerType(), True),
    StructField('author_num_reviews', IntegerType(), True),
    StructField('author_playtime_forever', LongType(), True),
    StructField('author_playtime_last_two_weeks', IntegerType(), True),
    StructField('author_playtime_at_review', IntegerType(), True),
    StructField('author_last_played', IntegerType(), True),
    StructField('language', StringType(), True),
    StructField('review', StringType(), True),
    StructField('timestamp_created', LongType(), True),
    StructField('timestamp_updated', LongType(), True),
    StructField('voted_up', IntegerType(), True),
    StructField('votes_up', IntegerType(), True),
    StructField('votes_funny', IntegerType(), True),
    StructField('weighted_vote_score', FloatType(), True),
    StructField('comment_count', IntegerType(), True),
    StructField('steam_purchase', IntegerType(), True),
    StructField('received_for_free', IntegerType(), True),
    StructField('written_during_early_access', IntegerType(), True),
    StructField('hidden_in_steam_china', IntegerType(), True),
    StructField('steam_china_location', StringType(), True),
])

df = (spark.read.format("csv")
	.schema(schema)
	.option("header", "true")
	.load(input_path))

df = df.filter(df["author_playtime_at_review"].isNotNull())
df = df.filter(df['voted_up'] < 2)
df = df.filter(df['author_playtime_at_review'] >= 0)
filtered_df = df.filter(df['appid'].isNotNull())

from pyspark.sql import functions as F

avg_playtime_df = filtered_df.groupBy("appid", "game").agg(
    (F.avg("author_playtime_at_review") / 60).alias("avg_playtime_hours"),
    F.count("recommendationid").alias("review_count")
)

print("Highest Average Playtime in hours at time of review")
sorted_df = avg_playtime_df.filter(F.col("review_count") >= 1000).orderBy("avg_playtime_hours", ascending=False)
sorted_df.show(30, truncate=False)

print("Most Reviewed Games")
sorted_df = avg_playtime_df.filter(F.col("review_count") >= 1000).orderBy("review_count", ascending=False)
sorted_df.show(30, truncate=False)

end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

spark.stop()