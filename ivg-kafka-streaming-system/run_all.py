import subprocess

print("Starting Kafka system...")

subprocess.Popen(["python", "producer.py"])
subprocess.Popen(["python", "consumers/validation_consumer.py"])
subprocess.Popen(["python", "consumers/analytics_consumer.py"])

print("System running...")