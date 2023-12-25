from kafka import KafkaProducer

#setting up a Kafka connection
def load_producer(kafka_server):
    return KafkaProducer(bootstrap_servers=kafka_server)