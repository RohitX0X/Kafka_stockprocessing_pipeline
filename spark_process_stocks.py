import findspark
import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

KAFKA_TOPIC_NAME = "stock_prices"
KAFKA_BOOTSTRAP_SERVER = "localhost:9092"

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 pyspark-shell'
# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.1'
# findspark.init()

spark = (
	SparkSession.builder.appName("Kafka Pyspark Streaming Learning")
	.getOrCreate()
    )
df = (
	spark.readStream.format("kafka")
	.option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER)
	.option("subscribe", KAFKA_TOPIC_NAME)
	.option("startingOffsets", "latest")
	.load()
    )



df.writeStream.format("json").outputMode("append").option("path", "./read").option("checkpointLocation", "./chkpoints").start().awaitTermination();

print(df)

ehStream.writeStream.trigger(Trigger.ProcessingTime("5 seconds")).foreachBatch { (ehStreamDF,_) => handleEhDataFrame(ehStreamDF)}.start.awaitTermination

val connectionString = ConnectionStringBuilder().setEventHubName("EVENTHUB_NAME").build

val df = spark.readStream.format("eventhubs").options(ehConf.toMap).load()