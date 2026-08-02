import sys

from pyspark.sql import SparkSession, functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, FloatType
from pyspark.sql import DataFrameWriter

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder.appName("top15reviewedgames").getOrCreate() 

# define custom schema
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

#df.show(5)
#df.printSchema()
# check that schema worked
#df.show(25)

# select game name column
# count occurrences of each game name
# order by count column descending
# show 15 rows
#df.select("game").groupBy("game").count().orderBy("count", ascending=False).show(15)

# filter data first
# need to view the voted_up column to actually determine what it is, had some odd values in it
#df.select("voted_up").groupBy("voted_up").count().orderBy("count", ascending=False).show(25)
# +--------------------+--------+
# |            voted_up|   count|
# +--------------------+--------+
# |                   1|95383123|
# |                   0|15545068|
# |                NULL|   25767|
# | and I became the...|    3061|
# | если остался про...|    2464|
# |        ~: : : : : :|    1741|
# |             however|    1641|
# ...
# 1 seems to be positive and 0 seems negative. there's also NULL and other junk data from the review in there
# choosing to exclude rows with a voted_up value that isn't 1 or 0
# may as well split the data into positive and negative reviews
positive_reviews = df.select("review","voted_up").filter("voted_up == 1")
negative_reviews = df.select("review","voted_up").filter("voted_up == 0")

# show to check
#positive_reviews.show(10)
#negative_reviews.show(10)




# convert review column into string length of column
# drop the review text because there are slurs :(
pos_review_length = positive_reviews.withColumn("review_length", F.length("review")).select("review_length","voted_up")
pos_count_by_length = pos_review_length.select("review_length").groupBy("review_length").count().withColumnRenamed("count","positive_reviews")
#print("Showing positive review count by length")
#pos_count_by_length.orderBy("positive_reviews", ascending=False).show(25)
output1 = pos_count_by_length.orderBy("review_length", ascending=True)


neg_review_length = negative_reviews.withColumn("review_length", F.length("review")).select("review_length","voted_up")
neg_count_by_length = neg_review_length.select("review_length").groupBy("review_length").count().withColumnRenamed("count","negative_reviews").withColumnRenamed("review_count","review_count_n")
#print("Showing negative review count by length")
#neg_count_by_length.orderBy("negative_reviews", ascending=False).show(25)
output2 = neg_count_by_length.orderBy("review_length", ascending=True)

# merging is a bit difficult for some reason

#merged = pos_count_by_length.join(neg_count_by_length, pos_count_by_length.review_length == neg_count_by_length.review_length_n, "inner").sort("review_length", ascending=True).show(100)
#merged = pos_count_by_length.unionByName(neg_count_by_length, allowMissingColumns=True).sort("review_length", ascending=True)

#merged.show(100)

# save sorted outputs to two separate files for display later
print("Showing positive review counts by length")
output1.show(10)
print("Showing negative review counts by length")
output2.show(10)

print("Saving outputs to files in " + output_path)
output1.write.csv(output_path + "/positive_reviews.csv", mode="overwrite")
output2.write.csv(output_path + "/negative_reviews.csv", mode="overwrite")

spark.stop()
