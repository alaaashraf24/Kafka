from confluent_kafka import Consumer, KafkaError, KafkaException
import sys
import requests
import random
import json
import cv2
import numpy as np

YOLO_CONFIG = 'yolov3.cfg'
YOLO_WEIGHTS = 'yolov3.weights'
YOLO_CLASSES = 'coco.names'

net = cv2.dnn.readNet(YOLO_WEIGHTS, YOLO_CONFIG)
with open(YOLO_CLASSES, 'r') as f:
    classes = [line.strip() for line in f.readlines()]

layer_names = net.getLayerNames()
output_layers = [layer_names[i - 1] for i in net.getUnconnectedOutLayers()]

me="alaa_ashraf_2"
conf = {'bootstrap.servers': '34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094',
        'group.id': me,
        'enable.auto.commit': 'false',
        'auto.offset.reset': 'smallest'}

Consumer = Consumer(conf)
running = True

def detect_object(image_path):
    img = cv2.imread(image_path)
    height, width, channels = img.shape

    blob = cv2.dnn.blobFromImage(img, 0.00392, (416, 416), (0, 0, 0), True, crop=False)
    net.setInput(blob)
    outs = net.forward(output_layers)

    class_ids = []
    confidences = []
    boxes = []
    for out in outs:
        for detection in out:
            scores = detection[5:]
            class_id = np.argmax(scores)
            confidence = scores[class_id]
            if confidence > 0.5:  
                center_x = int(detection[0] * width)
                center_y = int(detection[1] * height)
                w = int(detection[2] * width)
                h = int(detection[3] * height)
                x = int(center_x - w / 2)
                y = int(center_y - h / 2)
                boxes.append([x, y, w, h])
                confidences.append(float(confidence))
                class_ids.append(class_id)

    indexes = cv2.dnn.NMSBoxes(boxes, confidences, 0.5, 0.4)
    detected_objects = [classes[class_ids[i]] for i in indexes.flatten()]  
    return detected_objects[0] if detected_objects else "undefined"

def msg_process(msg):
    try:
        msg_value = msg.value()
        if msg_value is None:
            print("Received an empty message.")
            return

        print(f"Received message: {msg_value}")

        msg_dict = json.loads(msg_value)
        id = msg_dict["id"]
        filename = msg_dict["filename"]

        object_detected = detect_object(f"images/{filename}")
        requests.put(f'http://127.0.0.1:5000/object/{id}', json={"object": object_detected})
    except json.JSONDecodeError as e:
        print(f"JSON decoding error: {e}")
    except Exception as e:
        print(f"Error processing message: {e}")


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