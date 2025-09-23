# main.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
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

# Task 1: User Favorite Genres


# Task 2: Average Listen Time



# Task 3: Genre Loyalty Scores


# Task 4: Identify users who listen between 12 AM and 5 AM
