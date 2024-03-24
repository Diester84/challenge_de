from typing import List, Tuple
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, col, regexp_replace,

file_path = "farmers-protest-tweets-2021-2-4.json"

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    spark = SparkSession.builder \
        .appName("MyApp") \
        .getOrCreate()
    df = spark.read.json(file_path)
    df = df.withColumn("mentionedUsers_username", col("mentionedUsers.username"))
    df_filtered = df.filter(df.mentionedUsers_username.isNotNull())
    df_cleaned = df_filtered.withColumn("mentionedUsers_username", regexp_replace(col("mentionedUsers_username"), "[", ""))\
                            .withColumn("mentionedUsers_username", regexp_replace(col("mentionedUsers_username"), "]", ""))
    df_exploded = df_cleaned.withColumn("mentionedUser", explode(split(col("mentionedUsers_username"), ",\s*")))
    df_count = df_exploded.groupBy("mentionedUser").count()
    top_users = df_count.orderBy(col("count").desc()).limit(10)
    top_users_list = [(row['mentionedUser'], row['count']) for row in top_users.collect()]
    return top_users_list
