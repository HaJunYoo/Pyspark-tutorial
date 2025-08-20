from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType


# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 kafka_source_streaming.py

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("File Streaming Demo") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .getOrCreate()

    schema = StructType([
        StructField("id", StringType()),
        StructField("name", StringType()),
        StructField("title", StringType())
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "fake_people") \
        .option("startingOffsets", "earliest") \
        .load()
    
    kafka_df.printSchema()
    """
    |-- key: binary (nullable = true)
    |-- value: binary (nullable = true) # value:json -> str
    |-- topic: string (nullable = true)
    |-- partition: integer (nullable = true)
    |-- offset: long (nullable = true)
    |-- timestamp: timestamp (nullable = true)
    |-- timestampType: integer (nullable = true)
    """
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    value_df.createOrReplaceTempView("fake_people")
    value_df.printSchema()
    count_df = spark.sql("SELECT value.title, COUNT(1) count FROM fake_people GROUP BY 1 ORDER BY 2 DESC LIMIT 10")

    # 실제로는 console이 아닌 의미있는 sink를 사용
    count_writer_query = count_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "chk-point-dir-json") \
        .start()

    print("Listening to Kafka")
    count_writer_query.awaitTermination() # 새 배치가 시작하는 형태로 loop가 돈다
