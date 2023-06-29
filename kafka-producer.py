import time
import datetime
import json
import sys
import random
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
from kafka import KafkaProducer

topic = "kafka-spark-streaming-test_nested"
client_id = "raspberrypi"

def collect_and_send_data():
    publish_count = 0
    while(True):

        humidity = random.randint(0,120)
        print("Humidity: %s H" % humidity)

        temp = random.randint(0,60)
        print("Temperature: %s C" % temp)

        pressure = random.randint(0,1600)
        print("Pressure: %s Millibars" % pressure)

        orientation = {"pitch":"sample", "roll":"demo", "yaw":"test"}
        print("p: {pitch}, r: {roll}, y: {yaw}".format(**orientation))

        timestamp = datetime.datetime.fromtimestamp(
            time.time()).strftime('%Y-%m-%d %H:%M:%S')

        message = {
            "client_id": client_id,
            "timestamp": timestamp,
            "humidity": humidity,
            "pitch": orientation['pitch'],
            "roll": orientation['roll'],
            "yaw": orientation['yaw'],
            "count": publish_count,
            "nested": {
                "temperature": temp,
                "pressure_test": pressure
            }
        }
        print("Publishing message to topic '{}': {}".format(topic, message))
        
        kafka_producer(message)
        
        time.sleep(1)
        publish_count += 1

def kafka_producer(message):
    producer = KafkaProducer(bootstrap_servers=[<Your_Broker_List>], 
    value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    #producer = KafkaProducer(bootstrap_servers=['b-1.mskworkshopcluster.8bx5lx.c4.kafka.cn-north-1.amazonaws.com.cn:9094','b-2.mskworkshopcluster.8bx5lx.c4.kafka.cn-north-1.amazonaws.com.cn:9094'], 
    #value_serializer=lambda m: json.dumps(m).encode('utf-8'),
    #security_protocol = 'SSL', ssl_certfile = '/home/ec2-user/kafka/ssl/private/kafka.client.truststore.jks',
    #ssl_keyfile = '/home/ec2-user/kafka/ssl/private/kafka.client.keystore.jks')
    
    future = producer.send(topic,  value= message, partition= 0)
    future.get(timeout= 10)
        
    

if __name__ == '__main__':
    collect_and_send_data()
