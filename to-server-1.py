from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import requests
import random

me="alaa_ashraf_2"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': me,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'smallest'}

Consumer = Consumer(conf)
running = True

def detect_object(id):
    return random.choice(['car', 'house', 'person'])

def msg_process(msg):
    id = msg.value().decode()
    requests.put('http://127.0.0.1:5000/object/' + id, json={"object": detect_object(id)})

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                msg_process(msg)
    finally:
        consumer.close()

def shutdown():
    running = False

basic_consume_loop(Consumer, [me])