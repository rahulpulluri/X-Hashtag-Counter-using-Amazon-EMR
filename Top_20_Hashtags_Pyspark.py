# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,explode,split,lower

# Create Spark Session
spark = SparkSession.builder \
.appName("PySpark Trial") \
.getOrCreate()

# Read from input S3
s3_path = "s3://p2-inputdata/smallTwitter.json"
df_tweet = spark.read.json(s3_path)

# Hashtag extract
pattern = "[\\s,:!*.]+"
# pattern = r"[\w\d\s]+"
fields = ["doc.text", "value.properties.text", "doc.user.description"]
# fields = ['doc.user.description']
hashtags_df = None
for field in fields:
    if hashtags_df is None:
        hashtags_df = df_tweet.select(explode(split(lower(col(field)), pattern)).alias("word")) \
                        .filter(col("word").startswith("#")) \
                        .select(col("word").alias("hashtag"))
    else:
        field_df = df_tweet.select(explode(split(lower(col(field)), pattern)).alias("word")) \
                     .filter(col("word").startswith("#")) \
                     .select(col("word").alias("hashtag"))
        hashtags_df = hashtags_df.union(field_df)

hashtag_counts = hashtags_df.groupBy("hashtag").count()
# Sort hashtags by count and select the top 20
top_20_hashtags = hashtag_counts.orderBy(col("count").desc()).limit(20)

# Output S3
s3_folder_uri = "s3://cloudcomputingvt/Jyothi/"
top_20_hashtags.write.mode("overwrite").csv(s3_folder_uri)