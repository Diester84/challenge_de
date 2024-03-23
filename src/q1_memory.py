from typing import List, Tuple
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, rank, count, desc, max
from pyspark.sql.window import Window

file_path = "farmers-protest-tweets-2021-2-4.json"

def q1_pyspark(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Iniciar pyspark
    spark = SparkSession.builder \
        .appName("MyApp") \
        .getOrCreate()
    # Leer el archivo JSON
    df = spark.read.json(file_path)
    # Se aplana columna user.username
    df = df.withColumn("user_username", col("user.username"))

    # Se transforma para ver solo el dia
    df = df.withColumn("date", to_date(col("date")))

    # Se agrupa por fecha y se cuentan los registros para cada dia
    date_counts = df.groupBy("date").count()

    # Se buscan los top 10 dias con mas registros
    top_days = date_counts.orderBy(desc("count")).limit(10)

    # Se une el dataFrame original con los top días para filtrar solo esos días
    df_top_days = df.join(top_days, "date")

    # Cuenta la cantidad de cada username por día
    username_counts = df_top_days.groupBy("date", "user_username").count()

    # Busca el username con mayor cantidad de registros para cada dia
    max_username_count = username_counts.groupBy("date").agg(max("count").alias("max_count"))

    # Genera un dataframe con la informacion requerida
    top_dates_usernames = username_counts.join(
        max_username_count,
        (username_counts["date"] == max_username_count["date"]) & 
        (username_counts["count"] == max_username_count["max_count"]),
        "right"
    )

    # Join con toda la informacion que ordenar de mayor a menor cantidad de registros 
    top_usernames_per_day= top_days.join(top_dates_usernames, "date", "left")

    # Recolecta los resultados como una lista de tuplas
    top_usernames_per_day_list = [(row['date'], row['user_username']) for row in top_usernames_per_day.collect()]

    return top_usernames_per_day_list
