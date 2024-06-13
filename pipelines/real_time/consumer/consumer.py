from kafka import KafkaConsumer
import json
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timezone

def consume_data():
    print("Configuring Kafka consumer...")
    try:
        consumer = KafkaConsumer(
            'stock_topic',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='my-group-docker',
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            consumer_timeout_ms=10000  # Example: Increase timeout to 10 seconds
        )
    except Exception as e:
        print(f"Failed to configure Kafka consumer: {e}")
        return
    
    print("Configuring InfluxDB client...")
    token = "NXJSdPiQYRJQZc9fyIV9H-g-6jOzK5HeUDE-1Vg059jhCWiNrlzKzmYnVrlCEnbf9boWMRhS-IxrUYPq-GOZMQ=="
    org = "esprims"
    bucket = "stock_data"
    influxdb_url = "host.docker.internal:9092"

    try:
        client = InfluxDBClient(url=influxdb_url, token=token)
        write_api = client.write_api(write_options=SYNCHRONOUS)
        print("Kafka and InfluxDB clients configured.")
    except Exception as e:
        print(f"Failed to configure InfluxDB client: {e}")
        return

    print("Starting message consumption...")
    print("consumer :",consumer)
    try:
        for message in consumer:
            print("Received a message.")
            data = message.value
            for record in data:
                print("Processing record:", record)
                symbol = record['s']
                price = record['p']
                timestamp = datetime.fromtimestamp(record['t'] / 1000, tz=timezone.utc)  # Convert to datetime with timezone
                volume = record['v']
                print(f"symbol: {symbol}, price: {price}")

                # Create a point for InfluxDB
                point = Point("stock_prices") \
                    .tag("symbol", symbol) \
                    .field("price", float(price)) \
                    .field("volume", volume) \
                    .time(timestamp, WritePrecision.MS)

                # Write the point to InfluxDB
                try:
                    write_api.write(bucket=bucket, org=org, record=point)
                    print(f"Written to InfluxDB: {point}")
                except Exception as e:
                    print(f"Failed to write to InfluxDB: {e}")
    except Exception as e:
        print(f"Error during message consumption: {e}")
    finally:
        consumer.close()
        print("Finished consuming messages.")

if __name__ == "__main__":
    print("Starting consumer script.")
    consume_data()
