# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 spark_consumer.py

import re
import time
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, explode, split, udf, lower, desc,
    regexp_replace
)
from pyspark.sql.types import StringType

# Функция для загрузки стоп-слов 
def get_stopwords(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        return set(response.text.splitlines())
    except Exception as e:
        print(f"Error loading stopwords: {e}")
        return set()

STOPWORDS_URL = 'https://raw.githubusercontent.com/stopwords-iso/stopwords-ru/master/raw/stop-words-russian.txt'
STOPWORDS = get_stopwords(STOPWORDS_URL)

# Инициализация Spark Session 
spark = SparkSession.builder \
    .appName("TelegramProperNounsCounter") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "4") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# UDF для обработки
def process_word(word):
    if word is None: return None
    # Только русские слова с большой буквы
    if not re.match(r'^[А-ЯЁ][а-яё]+$', word):
        return None
    # Удаляем гласные и й в конце
    return re.sub(r'[аеёиоуыэюяй]+$', '', word)

process_word_udf = udf(process_word, StringType())

# --- Чтение Kafka ---
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "telegram_messages") \
    .option("startingOffsets", "latest") \
    .load()

# Обработка 
# Текст
lines = kafka_df.selectExpr("CAST(value AS STRING)")

# Разбивка на слова
words = lines.select(explode(split(col("value"), "\\s+")).alias("word"))

# Нормализация для стоп-слов (убираем пунктуацию, ловеркейс)
words_checked = words.withColumn(
    lower(regexp_replace(col("word"), r"[^\w\s]", ""))
)


# 4. Фильтр стоп-слов
words_filtered = words_checked.filter(~(lower(col("word")).isin(STOPWORDS)))

# 5. Обработка UDF (проверка на заглавную + обрезка окончаний)
words_processed = words_filtered.select(
    process_word_udf(col("word")).alias("processed_word")
).filter(col("processed_word").isNotNull())


# 6. Глобальная агрегация 
word_counts = words_processed.groupBy("processed_word").count()

# Запись 
def write_batch(batch_df, batch_id):
    batch_df \
        .sort(desc("count")) \
        .write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(f"output/batch_{batch_id}")

query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(write_batch) \
    .trigger(processingTime='1 minute') \
    .start()

print("Spark Streaming запущен. Накопление данных...")

# Работаем 30 минут 
try:
    time.sleep(30 * 60)
finally:
    query.stop()
    
    # Вывод топ-10 в консоль 
    print("\nФинальный топ-10 популярных слов:")
    try:
        # Читаем последний сохраненный батч (он самый полный в режиме complete)
        import os
        batches = [d for d in os.listdir("output") if d.startswith("batch_")]
        if batches:
            last_batch = sorted(batches, key=lambda x: int(x.split('_')[1]))[-1]
            path = f"output/{last_batch}"
            
            result = spark.read.option("header", "true").csv(path)
            
            result.select(
                col("processed_word").alias("слово"), 
                col("count").cast("long").alias("количество")
            ).sort(desc("количество")).show(10, truncate=False)
        else:
            print("Нет сохраненных батчей.")
            
    except Exception as e:
        print(f"Ошибка при чтении результатов: {e}")

    spark.stop()
