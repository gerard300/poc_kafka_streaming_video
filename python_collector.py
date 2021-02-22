import base64
import json
import os
import random
import sys
from datetime import datetime
from time import sleep

import cv2
from kafka import KafkaProducer

# En destino
# flat_frame_decoded = base64.decodebytes(flat_frame_encoded)
# decoded_frame = np.frombuffer(flat_frame_decoded, dtype=np.uint8)
# repe_frame = decoded_frame.reshape(h, w)

# y = datetime.fromtimestamp(x)
# y.strftime("%Y/%m/%d %H:%M:%S")

# Ejemplo encoded-decoded base64
# t = np.arange(25, dtype=np.float64)
# s = base64.b64encode(t)
# r = base64.decodebytes(s)
# q = np.frombuffer(r, dtype=np.float64)

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 4:
        print("Usage: python 3-kafka_producer.py <low_thres> <high_thres> <topic_name> <data_filename>")
        print("Suggested: python 3-kafka_producer.py 0.4 0.9 test data/sample_new_users.csv")
    else:
        try:
            # Lectura de argumentos
            low = sys.argv[1]
            high = sys.argv[2]
            # Topic será: kafka_video_stream
            topic = sys.argv[3]
            video = sys.argv[4]

            # Lectura del video
            cap = cv2.VideoCapture(video)
            frame_number = 0
            while True:
                # En origen
                check, frame = cap.read()
                if check:
                    frame_number += 1
                    frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
                    frame = cv2.resize(frame, (640, 480))
                    rows, cols = frame.shape
                    # flat_frame = frame.flatten()
                    flat_frame_encoded = base64.b64encode(frame)
                    flat_frame_encoded = flat_frame_encoded.decode('utf-8')

                    # Crear el JSON a enviar
                    dict_info = {
                        "ID": frame_number,
                        "Timestamp": datetime.timestamp(datetime.now()),
                        "rows": rows,
                        "cols": cols,
                        "dtype": "unit8",
                        "data": flat_frame_encoded
                    }
                    string_json = json.dumps(dict_info).encode('utf-8')

                    # Productor de Kafka - Mejora de configuración
                    producer = KafkaProducer(bootstrap_servers='localhost:9092')

                    # Envío de datos a kafka - Podría no ser UTF-8
                    print("Sending messages to kafka {0} topic...".format(topic))
                    producer.send(topic, value=string_json)
                    producer.flush()

                    # Por comodidad se simula una espera de tiempo
                    sleep(random.uniform(float(low), float(high)))
                    # cv2.imshow("Video", frame)

                    # if cv2.waitKey(30) & 0xFF == ord('q'):
                    #     break
        except KeyboardInterrupt:
            print('Interrupted from keyboard, shutdown')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
