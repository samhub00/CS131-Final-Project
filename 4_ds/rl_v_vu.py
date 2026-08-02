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


# some filtering to remove nulls and odd values
df = df.filter(df["author_playtime_at_review"].isNotNull())
df = df.filter(df['voted_up'] < 2)
df = df.filter(df['author_playtime_at_review'] >= 0)
df = df.filter(df['appid'].isNotNull())

from pyspark.sql import functions as F
"""

# This dataframe creates the table below, borrowed from the average_playtime.py file
review_count_df = df.groupBy("appid", "game").agg(
    (F.avg("author_playtime_at_review") / 60).alias("avg_playtime_hours"),
    F.count("recommendationid").alias("review_count")
)


Here are some of results from the most reviewed games. We will be picking some of the top games
Most Reviewed Games
+-------+------------------------------+------------------+------------+
|appid  |game                          |avg_playtime_hours|review_count|
+-------+------------------------------+------------------+------------+
|730    |Counter-Strike 2              |462.6056151608064 |7654993     |
|578080 |PUBG: BATTLEGROUNDS           |376.1220455301746 |2216757     |
|271590 |Grand Theft Auto V            |168.8377715886412 |1644997     |
|105600 |Terraria                      |160.90774024182394|1194588     |
|359550 |Tom Clancy's Rainbow Six Siege|265.65661804171356|1179984     |
|4000   |Garry's Mod                   |304.9940807543383 |999905      |
|440    |Team Fortress 2               |388.0432984231703 |987678      |
|252490 |Rust                          |422.3570424333919 |963133      |
"""

#For instance Counter-Strike 2:
counter_strike_df = df.filter(df['appid'] == 730)
# now we can look at the distribution of positive and negative reviews for this game.
"""
counter_strike_df_pn = counter_strike_df.groupBy("appid", "game").agg(
    F.count(F.when(F.col("voted_up") == 1, True)).alias("positive_reviews"),
    F.count(F.when(F.col("voted_up") == 0, True)).alias("negative_reviews")
)
"""
#counter_strike_df_pn.show()
"""
+-----+----------------+----------------+----------------+
|appid|            game|positive_reviews|negative_reviews|
+-----+----------------+----------------+----------------+
|  730|Counter-Strike 2|         6737222|          917771|
+-----+----------------+----------------+----------------+

Resulting table from splitting positive reviews and negative reviews.
"""
# prepping df for graphing using review length and positive/negative review split. 
cs2_for_graphing = counter_strike_df.withColumn(
    "review_length", F.length(F.col("review"))
).select("appid", "voted_up", "review_length")

# filter out reviews that are too long or short.
cs2_for_graphing = cs2_for_graphing.filter(cs2_for_graphing["review_length"] < 8000)
cs2_for_graphing = cs2_for_graphing.filter(cs2_for_graphing["review_length"] > 0)

cs2_for_graphing.show(10)

print("Counter-Strike 2 Review Length Distribution")

cs2_for_graphing.describe("review_length").show()
cs2_for_graphing.summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

#cs2_for_graphing.plot.line(x="review_length", y=["voted_up"==1, "voted_up"==0], title="Counter-Strike 2 Review Length Distribution", xlabel="Review Length", ylabel="Voted Up (1=Positive, 0=Negative)")

pivoted_df = (
    cs2_for_graphing
    .groupBy("review_length")
    .pivot("voted_up", [0, 1]) # Splits voted_up into columns named '0' and '1'
    .count()
    .fillna(0)
)


pivoted_df = pivoted_df.withColumnRenamed("1", "Positive").withColumnRenamed("0", "Negative")

pivoted_df.plot.line(
    x="review_length", 
    y=["Positive", "Negative"], 
    title="Counter-Strike 2 Review Length Distribution", 
    xlabel="Review Length", 
    ylabel="Number of Reviews"
)

"""
positive_reviews = df.select("review","voted_up").filter("voted_up == 1")
negative_reviews = df.select("review","voted_up").filter("voted_up == 0")

"""

end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

spark.stop()