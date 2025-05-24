import json
import random
import string
import time
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

try:
    from configs import kafka_config
except ImportError:
    print("Помилка: Файл 'configs.py' не знайдено. Будь ласка, створіть його з вашими налаштуваннями Kafka.")
    exit()

TARGET_TOPIC = "sergeyp_alerts"
BOOTSTRAP_SERVERS = kafka_config.get('bootstrap_servers')
SECURITY_PROTOCOL = kafka_config.get('security_protocol', 'PLAINTEXT')
SASL_MECHANISM = kafka_config.get('sasl_mechanism')
USERNAME = kafka_config.get('username')
PASSWORD = kafka_config.get('password')

GROUP_ID = 'alert_viewer_' + ''.join(random.choices(string.ascii_lowercase, k=5))

def create_kafka_consumer():
    print(f"Спроба створення Kafka consumer для серверів: {BOOTSTRAP_SERVERS}...")
    try:
        servers = BOOTSTRAP_SERVERS
        if isinstance(servers, str):
            servers = servers.split(',')

        consumer_config = {'bootstrap_servers': servers, 'group_id': GROUP_ID, 'auto_offset_reset': 'earliest',
                           'enable_auto_commit': False, 'consumer_timeout_ms': 5000,
                           'security_protocol': SECURITY_PROTOCOL, 'sasl_mechanism': SASL_MECHANISM,
                           'sasl_plain_username': USERNAME, 'sasl_plain_password': PASSWORD}

        consumer = KafkaConsumer(TARGET_TOPIC, **consumer_config)
        print("Kafka Consumer успішно створено.")
        return consumer

    except NoBrokersAvailable:
        print(f"Помилка: Не вдалося підключитися до Kafka: {BOOTSTRAP_SERVERS}.")
        return None
    except Exception as e:
        print(f"Несподівана помилка під час створення Kafka Consumer: {e}")
        return None

if __name__ == "__main__":
    print(f"Запуск Kafka consumer для топіку: {TARGET_TOPIC}")
    print(f"Використовується Group ID: {GROUP_ID} (читання з 'earliest')")
    print("Очікування повідомлень... (Натисніть Ctrl+C для зупинки)")

    consumer = create_kafka_consumer()

    if consumer:
        try:
            while True:
                for message in consumer:
                    print("-" * 40)
                    print(f"Topic:     {message.topic}")
                    print(f"Partition: {message.partition}")
                    print(f"Offset:    {message.offset}")
                    print(f"Timestamp: {datetime.fromtimestamp(message.timestamp / 1000)}")
                    try:
                        value = json.loads(message.value.decode('utf-8'))
                        print(f"Value:\n{json.dumps(value, indent=2, ensure_ascii=False)}")
                    except Exception:
                        print(f"Value:     {message.value.decode('utf-8', errors='ignore')}")
                print("No new messages, still waiting...") # Можна розкоментувати для відладки
                time.sleep(1)

        except KeyboardInterrupt:
            print("\nСпоживача зупинено користувачем.")
        except Exception as e:
            print(f"Несподівана помилка під час роботи: {e}")
        finally:
            print("Закриття Kafka Consumer...")
            consumer.close()
            print("Kafka Consumer закрито.")
    else:
        print("Не вдалося запустити споживача.")