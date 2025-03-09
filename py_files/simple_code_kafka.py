from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Producer, Consumer
import json
import os

BROKER = "localhost:29092"
TOPIC = "experiments"
MESSAGE = {"key": "value"}
OUTPUT_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "temp", "output.json")

def create_admin_client():
    return AdminClient({"bootstrap.servers": BROKER})

def topic_exists(admin_client, topic_name):
    topics = admin_client.list_topics(timeout=5).topics
    return topic_name in topics

def create_topic(admin_client, topic_name):
    if not topic_exists(admin_client, topic_name):
        futures = admin_client.create_topics([NewTopic(topic_name, num_partitions=1, replication_factor=1)])
        future = futures.get(topic_name)
        try:
            future.result()  # Ожидание завершения операции
            print(f"Топик {topic_name} создан")
        except Exception as e:
            print(f"Ошибка при создании топика {topic_name}: {e}")
    else:
        print(f"Топик {topic_name} уже существует")

def produce_message(topic, message):
    producer = Producer({"bootstrap.servers": BROKER})
    producer.produce(topic, key="test_key", value=json.dumps(message))
    producer.flush()
    print(f"Сообщение отправлено в {topic}")

def consume_message(topic):
    consumer = Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "test_group",
        "auto.offset.reset": "earliest"
    })
    consumer.subscribe([topic])
    
    msg = consumer.poll(timeout=10)
    consumer.close()
    
    if msg and not msg.error():
        data = msg.value().decode("utf-8")
        print(f"Прочитано сообщение: {data}")
        return data
    else:
        print("Нет сообщений")
        return None

def save_message_to_file(message, file_path):
    os.makedirs(os.path.dirname(file_path))
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(message)
    print(f"Сообщение сохранено в {file_path}")

if __name__ == "__main__":
    admin_client = create_admin_client()
    
    if not topic_exists(admin_client, TOPIC):
        print(f"Топик {TOPIC} не существует")
        create_topic(admin_client, TOPIC)
    
    if topic_exists(admin_client, TOPIC):
        print(f"Топик {TOPIC} существует")
        produce_message(TOPIC, MESSAGE)
        
        received_message = consume_message(TOPIC)
        if received_message:
            save_message_to_file(received_message, OUTPUT_FILE)
