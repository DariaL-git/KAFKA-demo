import subprocess
import time

consumer = subprocess.Popen(["python", "consumer.py"])

time.sleep(3)

producer = subprocess.Popen(["python", "producer.py"])

producer.wait()
consumer.wait()