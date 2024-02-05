from kafka import KafkaConsumer
import json

# Connect to Cassandra cluster
from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient('localhost', 27017)  # Replace with your MongoDB host and port
db = client['Stocks_data']  # Replace with your MongoDB database name
collection = db['stocks']  # Replace with your MongoDB collection name




kafka_broker = "localhost:9092"  # Replace with your Kafka broker(s)
kafka_topic = "output_topic"  # Replace with the Kafka topic you want to consume from

# Create a Kafka consumer instance
consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_broker,
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
)

# Start consuming messages
def consume_stocks():

    while True:
        for message in consumer:
            if message is None:
                print("swd")
                continue
            stock_data = message.value
            print("Received Stock Data:", message.value)
            print("\n")

            kafka_message = json.dumps(message.value)
            print(kafka_message)

            
            # collection.insert_one(stock_data)

        print("#########################\n")
    

if __name__ == "__main__":
    consume_stocks()
