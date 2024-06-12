from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

# Kafka consumer configuration
consumer = KafkaConsumer(
    'stock_topic',
    bootstrap_servers='kafka:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# InfluxDB client configuration
token = "NXJSdPiQYRJQZc9fyIV9H-g-6jOzK5HeUDE-1Vg059jhCWiNrlzKzmYnVrlCEnbf9boWMRhS-IxrUYPq-GOZMQ=="
org = "esprims"
bucket = "stock_data"
influxdb_url = "http://host.docker.internal:8086"

client = InfluxDBClient(url=influxdb_url, token=token)
write_api = client.write_api(write_options=SYNCHRONOUS)

# Consume messages from Kafka and write to InfluxDB
for message in consumer:
    data = message.value
    for record in data:
        symbol = record['s']
        price = record['p']
        timestamp = datetime.fromtimestamp(record['t'] / 1000, tz=timezone.utc)  # Convert to datetime with timezone
        volume = record['v']
        
        # Create a point for InfluxDB
        point = Point("stock_prices") \
            .tag("symbol", symbol) \
            .field("price", float(price)) \
            .field("volume", volume) \
            .time(timestamp, WritePrecision.MS)
        
        # Write the point to InfluxDB
        write_api.write(bucket=bucket, org=org, record=point)
        print(f"Written to InfluxDB: {point}")

print("Finished consuming messages.")
