import sys, io
import time


start_time = time.time()


from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
from google.cloud import storage

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("analysis").getOrCreate() 

# defined schema to avoid incorrect inference of data types. 
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
df = df.filter(df['language'] == 'english')

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
df = df.filter(df['appid'].isin([730, 504320, 548430, 105600])) # filter for Counter-Strike 2, Celeste, Deep Rock Galactic, and Terraria

# Celeste is 504230
# Deep Rock galactic is 548430 
# Terraria is 105600

# prepping df for graphing using review length and positive/negative review split. 
df = df.withColumn(
    "review_length", F.length(F.col("review"))
).select("appid", "voted_up", "review_length")

# filter out reviews that are too long or short.
df = df.filter(df["review_length"] < 8001)
df = df.filter(df["review_length"] > 0)


def make_graphing_csv(game_df, gameid):
    #describe the statistics of the review_length column in the game_df DataFrame
    game_df.describe("review_length").show()
    # Aggregate review counts by review_length AND sentiment (voted_up) in PySpark
    agg_df = (
        game_df
        .groupBy("review_length", "voted_up")
        .count()
    )

    # Bring the aggregated data (small) to Pandas
    pandas_df = agg_df.toPandas()
    # Need to change data so that plot can have two lines, one for positive reviews and one for negative reviews.
    # Pivot so review_length is the index, and True/False (positive/negative) become columns
    pivot_df = pandas_df.pivot(
        index="review_length", 
        columns="voted_up", 
        values="count"
    ).fillna(0)

    print("Saving outputs to files in " + output_path)
    pivot_df.to_csv(output_path + f"/graphing_data{gameid}.csv")

print("Statistics for Counter-Strike 2 Reviews:")
make_graphing_csv(df.filter(df["appid"] == 730), 730) # Counter-Strike 2
print("Statistics for Celeste Reviews:")
make_graphing_csv(df.filter(df["appid"] == 504230), 504230) # Celeste
print("Statistics for Deep Rock Galactic Reviews:")
make_graphing_csv(df.filter(df["appid"] == 548430), 548430) # Deep Rock Galactic
print("Statistics for Terraria Reviews:")
make_graphing_csv(df.filter(df["appid"] == 105600), 105600) # Terraria

end_time = time.time()
elapsed_time = end_time - start_time

print(f"\nTotal execution time: {elapsed_time:.2f} seconds")

spark.stop()