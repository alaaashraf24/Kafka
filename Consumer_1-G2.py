from confluent_kafka import Consumer
from confluent_kafka import Producer, KafkaError,KafkaException
import sys

me="alaa_ashraf"

conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': 'G2',
        'auto.offset.reset': 'smallest'}

consumer = Consumer(conf)

def msg_process(msg):
    print("Recieved Message ", msg.value())

running = True

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

basic_consume_loop(consumer,[me])
