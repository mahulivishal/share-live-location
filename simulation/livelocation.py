import json
import random
import time

from pykafka import KafkaClient

kafka_brokers = "localhost:9092"
location_source_data_topic = "device.bms.data.source.v1"
client = KafkaClient(hosts=kafka_brokers)
topic = client.topics[location_source_data_topic]
producer = topic.get_sync_producer()
devices = ["d_01", "d_01", "d_01"]
no_of_packets = 1000
lat = 19.99
long = 73.78


def push_speed_data(data):
    message = json.dumps(data)
    message_bytes = message.encode('utf-8')
    producer.produce(message_bytes)


def prepare_data():
    i = 0
    while i < no_of_packets:
        latitude, longitude = generate_random_lat_long()
        data = {"deviceId": random.choice(devices), "timestamp": int(time.time() * 1000),
                "latitude": latitude, "longitude": longitude}
        print(data)
        time.sleep(0.5)
        push_speed_data(data)
        i += 1


def generate_random_lat_long():
    dec_lat = random.random() / 100
    dec_lon = random.random() / 100
    return float(format(lat + dec_lat, '.4f')), float(format(long + dec_lon, '.4f'))


if __name__ == '__main__':
    prepare_data()
