# Import necessary libraries from PySpark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

# Initialize a Spark session
spark = SparkSession.builder \
    .appName("PySpark Trial") \
    .getOrCreate()

# Load JSON data from S3 bucket
s3_input_path = "s3://p2-inputdata/smallTwitter.json"
tweets_df = spark.read.json(s3_input_path)

# Define the pattern for splitting text and the fields to search for hashtags
split_pattern = "[\\s,:!*.]+"
text_fields = ["doc.text", "value.properties.text", "doc.user.description"]

# Extract hashtags from specified fields
all_hashtags_df = None
for text_field in text_fields:
    current_field_df = tweets_df.select(explode(split(lower(col(text_field)), split_pattern)).alias("word")) \
                               .filter(col("word").startswith("#")) \
                               .select(col("word").alias("hashtag"))
    all_hashtags_df = current_field_df if all_hashtags_df is None else all_hashtags_df.union(current_field_df)

# Count occurrences of each hashtag and get the top 20 hashtags
hashtag_counts_df = all_hashtags_df.groupBy("hashtag").count()
top_20_hashtags_df = hashtag_counts_df.orderBy(col("count").desc()).limit(20)

# Save the top 20 hashtags to an S3 bucket
s3_output_path = "s3://cloudcomputingvt/Rahul/"
top_20_hashtags_df.write.mode("overwrite").csv(s3_output_path)
