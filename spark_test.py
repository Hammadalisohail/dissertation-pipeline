from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("KafkaTest") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

print("Reading from Kafka as batch...")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "retail-events") \
    .option("startingOffsets", "earliest") \
    .option("endingOffsets", "latest") \
    .load()

print(f"Total messages in Kafka: {df.count()}")
df.show(5, truncate=False)

spark.stop()