import websocket
import json
from kafka import KafkaProducer

def on_message(ws, message):
    message = json.loads(message)
    print("Received message: ", message)

    bootstrap_server = 'kafka:9092'
    producer = KafkaProducer(bootstrap_servers=bootstrap_server)

    # Serialize the message to JSON and send it to Kafka
    producer.send("stock_topic", json.dumps(message['data']).encode('utf-8'))
    producer.flush()

def on_error(ws, error):
    print(error)

def on_close(ws):
    print("### closed ###")

def on_open(ws):
    ws.send('{"type":"subscribe","symbol":"AAPL"}')

if __name__ == "__main__":
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp("wss://ws.finnhub.io?token=clk9sq1r01qso7g5hot0clk9sq1r01qso7g5hotg",
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    ws.on_open = on_open
    ws.run_forever()
