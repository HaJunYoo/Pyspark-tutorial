from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# nc -lk 9999
# spark-submit wordcount_streaming.py
'''
소스 코드는 Spark Streaming으로 실시간 데이터 처리를 수행하는 애플리케이션입니다.
스트리밍 애플리케이션은 지속적으로 데이터를 수신하고 처리하는데, 이를 위해 spark-submit 명령어를 사용하여 실행해야 합니다.
spark-submit으로 애플리케이션을 실행하면 클러스터에서 애플리케이션을 분산 처리하고, 지속적인 데이터 수신과 처리를 수행합니다.
외부 리소스와의 통신 및 자원 관리, 로깅 등을 효율적으로 처리할 수 있습니다.
'''

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("Streaming Word Count") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnShutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    # READ
    # socket : tcp 프로토콜에서 읽음
    lines_df = spark.readStream \
        .format("socket") \
        .option("host", "localhost") \
        .option("port", "9999") \
        .load()

    # TRANSFORM
    words_df = lines_df.select(expr("explode(split(value,' ')) as word"))
    
    # counts_df = words_df.groupBy("word").count() #  과제 : Spark SQL을 사용하게 변경해보기
    
    # Using Spark SQL
    words_df.createOrReplaceTempView("words_view")
    counts_df = spark.sql("SELECT word, count(*) as count FROM words_view GROUP BY word")

    # SINK
    word_count_query = counts_df.writeStream \
        .format("console") \
        .outputMode("complete") \
        .option("checkpointLocation", "chk-point-dir") \
        .start()

    print("Listening to localhost:9999")
    word_count_query.awaitTermination()