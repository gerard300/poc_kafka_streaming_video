import base64
import json
import os
import random
import sys
from datetime import datetime
from time import sleep
import numpy as np
import cv2
from kafka import KafkaProducer

# Ejemplo encoded-decoded base64
# t = np.arange(25, dtype=np.float64)
# s = base64.b64encode(t)
# r = base64.decodebytes(s)
# q = np.frombuffer(r, dtype=np.float64)

cap = cv2.VideoCapture("/home/gerardo/PycharmProjects/video_streaming/people_walking.mp4")
while True:

    check, frame = cap.read()

    if check:

        # En origen
        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        rows, cols = frame.shape
        #flat_frame = frame.flatten()
        flat_frame_encoded = base64.b64encode(frame)
        flat_frame_encoded = flat_frame_encoded.decode('utf-8')

        dict_info = {
            "ID": 2,
            "Timestamp": datetime.timestamp(datetime.now()),
            "rows": rows,
            "cols": cols,
            "dtype": "unit8",
            "data": flat_frame_encoded
        }
        string_json = json.dumps(dict_info)
        print(string_json)
        break
        # En destino
        # flat_frame_decoded = flat_frame_encoded.encode('utf-8')
        # flat_frame_decoded = base64.decodebytes(flat_frame_decoded)
        # decoded_frame = np.frombuffer(flat_frame_decoded, dtype=np.uint8)
        # repe_frame = decoded_frame.reshape(rows, cols)
        # cv2.imshow("Video", repe_frame)
        #
        # if cv2.waitKey(30) & 0xFF == ord('q'):
        #     break
