import subprocess
import time

print("Starting IVG Kafka system...")


def start_process(command, name):
    print(f"Starting {name}...")

    return subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

producer = start_process(["python", "producer.py"], "Producer")

validation = start_process(
    ["python", "consumers/validation_consumer.py"],
    "Validation Consumer"
)

analytics = start_process(
    ["python", "consumers/analytics_consumer.py"],
    "Analytics Consumer"
)

reporting = start_process(
    ["python", "consumers/reporting_consumer.py"],
    "Reporting Consumer"
)

time.sleep(2)

dashboard = start_process(
    ["streamlit", "run", "dashboard.py"],
    "Streamlit Dashboard"
)

print("✅ System running successfully!")

try:
    while True:
        time.sleep(5)
except KeyboardInterrupt:
    print("\n🛑 Shutting down system...")
    producer.terminate()
    validation.terminate()
    analytics.terminate()
    reporting.terminate()
    dashboard.terminate()