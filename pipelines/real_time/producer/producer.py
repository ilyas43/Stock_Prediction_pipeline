import websocket
import json
from kafka import KafkaProducer

# Initialize KafkaProducer once
bootstrap_server = 'kafka:9092'
producer = KafkaProducer(bootstrap_servers=bootstrap_server)

def on_message(ws, message):
    try:
        message = json.loads(message)
        print("Received message: ", message)

        # Check if the message contains 'data' key
        if 'data' in message:
            print("Message contains 'data' key")

            # Serialize the message to JSON and send it to Kafka
            producer.send("stock_topic", json.dumps(message['data']).encode('utf-8'))
            producer.flush()
            print("Message sent to Kafka")
        else:
            print("Message does not contain 'data' key: ", message)
    except Exception as e:
        print(f"Error processing message: {e}")

def on_error(ws, error):
    print("WebSocket error: ", error)

def on_close(ws):
    print("### WebSocket closed ###")

def on_open(ws):
    print("WebSocket connected")
    ws.send('{"type":"subscribe","symbol":"AAPL"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(
        "wss://ws.finnhub.io?token=clk9sq1r01qso7g5hot0clk9sq1r01qso7g5hotg",
        on_message=on_message,
        on_error=on_error,
        on_close=on_close
    )
    ws.on_open = on_open
    ws.run_forever()
