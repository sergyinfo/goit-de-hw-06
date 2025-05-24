import json
import random
import time
from datetime import datetime

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable, KafkaError

try:
    from configs import kafka_config
except ImportError:
    print("Error: 'configs.py' not found. Please create it with your Kafka configuration.")
    exit()

my_name = "sergeyp"
TARGET_TOPIC = f"{my_name}_building_sensors"

send_interval_seconds = 5

def create_kafka_producer():
    """Create a Kafka producer."""
    print(f"Спроба створення Kafka producer для серверів: {kafka_config.get('bootstrap_servers')}...")
    try:
        producer = KafkaProducer(
            bootstrap_servers=kafka_config['bootstrap_servers'],
            security_protocol=kafka_config['security_protocol'],
            sasl_mechanism=kafka_config['sasl_mechanism'],
            sasl_plain_username=kafka_config['username'],
            sasl_plain_password=kafka_config['password'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5,
        )
        print("Kafka Producer успішно створено.")
        return producer
    except NoBrokersAvailable:
        print(f"Помилка: Не вдалося підключитися до Kafka: {kafka_config.get('bootstrap_servers')}.")
        return None
    except Exception as e:
        print(f"Несподівана помилка під час створення Kafka Producer: {e}")
        return None

def generate_sensor_data(sensor_id):
    """Random data generator for sensors."""
    temperature = round(random.uniform(5.0, 50.0), 2)
    humidity = round(random.uniform(10.0, 95.0), 2)
    timestamp = datetime.now().isoformat()
    return {
        "sensor_id": sensor_id,
        "timestamp": timestamp,
        "temperature": temperature,
        "humidity": humidity
    }

if __name__ == "__main__":
    sensor_id = f"sensor_{random.randint(1000, 9999)}"
    print(f"Симулятор сенсора запущено для: {sensor_id}")
    print(f"Дані будуть відправлятись у топік: {TARGET_TOPIC}")
    print(f"Інтервал відправки: {send_interval_seconds} секунд.")

    producer = create_kafka_producer()

    if producer:
        try:
            while True:
                data = generate_sensor_data(sensor_id)
                future = producer.send(TARGET_TOPIC, value=data)
                try:
                    record_metadata = future.get(timeout=10)
                    print(
                        f"[{data['timestamp']}] ID: {data['sensor_id']} -> Дані: {{T:{data['temperature']}°C, H:{data['humidity']}%}} -> "
                        f"Топік: {record_metadata.topic}, Partition: {record_metadata.partition}, "
                        f"Offset: {record_metadata.offset}"
                    )
                except KafkaError as ke:
                    print(f"Помилка відправки повідомлення у {TARGET_TOPIC}: {ke}")
                time.sleep(send_interval_seconds)
        except KeyboardInterrupt:
            print("\nСимуляцію зупинено користувачем.")
        finally:
            print("Закриття Kafka Producer...")
            producer.flush()
            producer.close()
            print("Kafka Producer закрито.")
    else:
        print("Не вдалося запустити симулятор.")