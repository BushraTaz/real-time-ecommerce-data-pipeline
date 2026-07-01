import subprocess

print("🚀 Starting Kafka Consumer Service...")

while True:
    try:
        subprocess.run(["python", "consumer.py"])
    except Exception as e:
        print("Consumer crashed. Restarting...", e)