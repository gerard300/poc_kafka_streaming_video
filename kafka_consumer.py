from kafka import KafkaConsumer
from pymongo import MongoClient
from json import loads
import base64
import numpy as np
import cv2

consumer = KafkaConsumer(
    'video2',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: loads(x.decode('utf-8')))

print(consumer.topics())
for message in consumer:
    print(message.value)
    # print(message.value)
    # En destino
    flat_frame_encoded = message.value['data']
    rows = message.value['rows']
    cols = message.value['cols']
    dim = message.value['dim']
    flat_frame_decoded = flat_frame_encoded.encode('utf-8')
    flat_frame_decoded = base64.decodebytes(flat_frame_decoded)
    decoded_frame = np.frombuffer(flat_frame_decoded, dtype=np.uint8)
    repe_frame = decoded_frame.reshape(rows, cols,dim)
    cv2.imshow("Video", repe_frame)
    if cv2.waitKey(30) & 0xFF == ord('q'):
        break
