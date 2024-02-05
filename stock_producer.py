from pandas_datareader import data as pdr
import pandas as pd
import yfinance as yf
yf.pdr_override() # <== that's all it takes :-)


# Define the stock symbol and the date range for which you want to fetch data
stock_symbol = "RELIANCE.BO"
start_date = "2023-09-01"
end_date = "2023-09-02"

from kafka import KafkaProducer

import json
import time

# Define the Kafka broker address and topic
kafka_broker = "localhost:9092"  # Replace with your Kafka broker(s)
kafka_topic = "stock_prices"

# Create a Kafka producer instance
producer = KafkaProducer(bootstrap_servers=kafka_broker, value_serializer=lambda v: json.dumps(v).encode('utf-8'))

def fetch_stock_price_data():
	try:
		# Fetch stock price data using yfinance
		##stock_data = yf.download(stock_symbol, period="1d", interval="1m")
		stock_data = yf.Ticker(stock_symbol)

		# Convert the DataFrame to a dictionary
		#stock_data_dict = stock_data.to_dict(orient="records")
		stock_data_dict = stock_data.info
		print(stock_data_dict)

		required_keys = ['symbol','open','currentPrice','averageVolume','fiftyDayAverage','dayLow','dayHigh','previousClose']
		
		filtered_stock = {key: stock_data_dict[key] for key in required_keys}



		producer.send(kafka_topic, value=(filtered_stock))
		


		# Send the fetched data to the Kafka topic
		# for key,value in stock_data_dict.items():
		# 	producer.send(kafka_topic, value={key,value})
		
		producer.flush()  # Flush the producer to ensure messages are sent

		print("##############################\n\n\n\n\n\n\n\n\n\n\n")
		# Wait briefly before fetching more data
		time.sleep(5)  # Adjust the interval as needed
	except Exception as e:
		print("Error fetching or sending data to Kafka:", str(e))

if __name__ == "__main__":
	while True:
		fetch_stock_price_data()