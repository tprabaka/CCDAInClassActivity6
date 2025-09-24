# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.functions import col, count, avg, hour, when, sum as ssum, row_number, round as sround
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

spark = SparkSession.builder.appName("MusicAnalysis").getOrCreate()

# Load datasets
listeningLogsSchema = StructType([
    StructField("user_id",StringType(),True),
    StructField("song_id",StringType(),True),
    StructField("timestamp", TimestampType(), True),
    StructField("duration_sec", IntegerType(), True)
])

dfListenLogs = spark.read.csv(
    "listening_logs.csv",
    schema=listeningLogsSchema,
    header=True
)

songsMetadataSchema = StructType([
    StructField("song_id",StringType(),True),
    StructField("title",StringType(),True),
    StructField("artist",StringType(),True),
    StructField("genre",StringType(),True),
    StructField("mood",StringType(),True)
])

dfSongsMetadata = spark.read.csv(
    "songs_metadata.csv",
    schema=songsMetadataSchema,
    header=True
)

# dfSongsMetadata.printSchema()
# dfSongsMetadata.show()

plays = dfListenLogs.join(dfSongsMetadata.select("song_id", "genre", "title", "artist"),
                          on="song_id", how="left")

# Task 1: User Favorite Genres
user_genre_counts = plays.groupBy("user_id", "genre").agg(count("*").alias("plays"))
w = Window.partitionBy("user_id").orderBy(col("plays").desc(), col("genre").asc())
user_favorite_genres = (
    user_genre_counts
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
    .select("user_id", col("genre").alias("favorite_genre"), col("plays").alias("favorite_genre_plays"))
)
user_favorite_genres.coalesce(1).write.mode("overwrite").format("csv").option("header", True)\
    .save("output/user_favorite_genres")

# Task 2: Average Listen Time

avg_listen_time_per_song = (
    dfListenLogs
    .filter(col("duration_sec").isNotNull())
    .groupBy("song_id")
    .agg(sround(avg("duration_sec"), 2).alias("avg_duration_sec"))
    .join(dfSongsMetadata.select("song_id", "title", "artist", "genre"), on="song_id", how="left")
    .select("song_id", "title", "artist", "genre", "avg_duration_sec")
)

avg_listen_time_per_song.coalesce(1).write.mode("overwrite").format("csv").option("header", True)\
    .save("output/avg_listen_time_per_song")

# Task 3: Genre Loyalty Scores
user_totals = user_genre_counts.groupBy("user_id").agg(ssum("plays").alias("total_plays"))

loyalty = (
    user_favorite_genres.join(user_totals, on="user_id", how="inner")
    .withColumn("loyalty_score", col("favorite_genre_plays") / col("total_plays"))
    .filter(col("loyalty_score") > 0.8)
    .select("user_id", "favorite_genre", "favorite_genre_plays", "total_plays",
            sround(col("loyalty_score"), 4).alias("loyalty_score"))
)

loyalty.coalesce(1).write.mode("overwrite").format("csv").option("header", True)\
    .save("output/genre_loyalty_scores")

# Task 4: Identify users who listen between 12 AM and 5 AM
with_hours = dfListenLogs.withColumn("hour", hour(col("timestamp")))
night_plays = (
    with_hours
    .filter((col("hour") >= 0) & (col("hour") < 5))
    .groupBy("user_id")
    .agg(count("*").alias("night_plays"))
    .filter(col("night_plays") >= 5)
)

night_plays.coalesce(1).write.mode("overwrite").format("csv").option("header", True)\
    .save("output/night_owl_users")

spark.stop()