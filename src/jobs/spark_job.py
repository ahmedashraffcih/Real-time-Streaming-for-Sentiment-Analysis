import pyspark
from pyspark.sql import SparkSession
from time import sleep
from pyspark.sql.functions import from_json, col, when, udf
from pyspark.sql.types import StructType, StructField, StringType, FloatType

def start_streaming(spark):
    try:
        stream_df = spark.readStream.format("socket") \
            .option("host", "localhost") \
            .option("port", 9999) \
            .load()
        
        schema = StructType([
            StructField("review_id", StringType()),
            StructField("user_id", StringType()),
            StructField("business_id", StringType()),
            StructField("stars", FloatType()),
            StructField("date", StringType()),
            StructField("text", StringType())
        ])
        
        stream_df = stream_df.select(from_json(col('value'), schema).alias("data")).select(("data.*"))

        query = stream_df.writeStream.outputMode("append").format("console").options(truncate=False).start()
        query.awaitTermination()
    except Exception as e:
        print(e)


if __name__ == '__main__':
    sc = SparkSession.builder.appName("SocketConsumer").getOrCreate()
    start_streaming(sc)
