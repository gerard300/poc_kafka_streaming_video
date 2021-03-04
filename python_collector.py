import base64
import json
import os
import random
import sys
from datetime import datetime
from time import sleep

import cv2
from kafka import KafkaProducer

if __name__ == "__main__":
    print(sys.argv)
    if len(sys.argv) < 4:
        print("Usage: python python_collector.py 0.4 0.9 <topic_name> path/to/<file_name>.<video>")
    else:
        try:
            # Lectura de argumentos
            low = sys.argv[1]
            high = sys.argv[2]
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
                    # frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY) # Transformación básica a escala de grises
                    frame = cv2.resize(frame, (100, 100)) # Se redimensiona la imagen para poder ser enviada
                    rows, cols, dim = frame.shape
                    # flat_frame = frame.flatten() # Si se quiere un vector
                    flat_frame_encoded = base64.b64encode(frame) # Codificado en base64
                    flat_frame_encoded = flat_frame_encoded.decode('utf-8') # base64 a String

                    # Crear el JSON a enviar
                    dict_info = {
                        "ID": frame_number,
                        "Timestamp": datetime.timestamp(datetime.now()),
                        "rows": rows,
                        "cols": cols,
                        "dim": dim,
                        "dtype": "unit8",
                        "data": flat_frame_encoded
                    }
                    string_json = json.dumps(dict_info).encode('utf-8')

                    # Productor de Kafka - Mejora de configuración
                    # Dos brokers, dos particiones en el topic
                    producer = KafkaProducer(bootstrap_servers=['localhost:9092', 'localhost:9093'])

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
