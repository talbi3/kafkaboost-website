import time
import threading
import uuid
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.consumer import KafkaboostConsumer
from kafka.admin import KafkaAdminClient, NewTopic

TOPIC = "test_topic_1"
TOPIC2 = "test_topic_2"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = f"priority_test_group_{uuid.uuid4()}"  # Unique group ID for each test run
CONFIG_FILE = "/Users/noaalt/Projects/kafkaboost_repo/kafkaboost/tests/sample_config.json"

def clear_topics():
    """Clear all messages from the topics"""
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    try:
        # Delete and recreate topics
        admin_client.delete_topics([TOPIC, TOPIC2])
        time.sleep(2)  # Wait for topics to be deleted
        topics = [
            NewTopic(name=TOPIC, num_partitions=1, replication_factor=1),
            NewTopic(name=TOPIC2, num_partitions=1, replication_factor=1)
        ]
        admin_client.create_topics(topics)
        print(f"Cleared and recreated topics: {TOPIC}, {TOPIC2}")
    except Exception as e:
        print(f"Error clearing topics: {e}")
    finally:
        admin_client.close()

def create_topics():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    topics = [
        NewTopic(name=TOPIC, num_partitions=1, replication_factor=1),
        NewTopic(name=TOPIC2, num_partitions=1, replication_factor=1)
    ]
    try:
        admin_client.create_topics(topics)
        print(f"Created topics: {TOPIC}, {TOPIC2}")
    except Exception as e:
        print(f"Error creating topics: {e}")
    finally:
        admin_client.close()

def producer_thread():
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, config_file=CONFIG_FILE)

    messages = [
        {"data": "p10", "priority": 10},
        {"data": "p5", "priority": 5},
        {"data": "p9", "priority": 9},
        {"data": "p8 ", "priority": 8},
        {"data": "p4 Medium", "priority": 4}
    ]

    for msg in messages:
        producer.send(TOPIC, value=msg, priority=msg["priority"])
        print(f"Produced: {msg}")

    producer.flush()
    producer.close()

def consumer_thread(received_messages):
    print(f"Consumer subscribing to topic: {TOPIC} with group ID: {GROUP_ID}")
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[TOPIC],
        group_id=GROUP_ID,
        max_poll_records=500,
        consumer_timeout_ms=10000,
        config_file=CONFIG_FILE,
        auto_offset_reset='earliest'  # Start reading from the beginning
    )

    print("Polling for prioritized messages...")
    start_time = time.time()
    while len(received_messages) < 5 and time.time() - start_time < 15:
        batch = consumer.poll(timeout_ms=1000, max_records=500)
        if batch:
            print(f"Received batch of {len(batch)} messages")
            for message in batch:
                if message.value not in received_messages:
                    received_messages.append(message.value)
                    print(f"Added new message: {message.value}")
            print(f"Total unique messages received so far: {len(received_messages)}")
        else:
            print("No messages received in this poll")

    consumer.close()

def test_priority_order():
    # Clear topic before starting the test
    clear_topics()
    
    # Create a list to store received messages
    received_messages = []
    
    # Create and start threads
    producer = threading.Thread(target=producer_thread)
    consumer = threading.Thread(target=consumer_thread, args=(received_messages,))
    
    # Start producer first
    producer.start()
    time.sleep(2)  # Give producer time to send messages
    
    # Start consumer
    consumer.start()
    
    # Wait for both threads to complete
    producer.join()
    consumer.join()

    print(f"\nTotal messages received: {len(received_messages)}")
    
    # Verify messages are received in descending priority order
    priorities = [msg["priority"] for msg in received_messages]
    assert priorities == sorted(priorities, reverse=True), \
        f"Expected descending priorities, got: {priorities}"

    print("✅ Test passed: messages received in priority order.")
    for msg in received_messages:
        print(msg)

def producer_thread_multi_topic():
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS, config_file=CONFIG_FILE)

    messages_topic1 = [
        {"data": "topic1_p10", "priority": 10},
        {"data": "topic1_p5", "priority": 5},
        {"data": "topic1_p9", "priority": 9},
        {"data": "topic1_p8", "priority": 8},
        {"data": "topic1_p4", "priority": 4}
    ]

    messages_topic2 = [
        {"data": "topic2_p10", "priority": 10},
        {"data": "topic2_p5", "priority": 5},
        {"data": "topic2_p9", "priority": 9},
        {"data": "topic2_p8", "priority": 8},
        {"data": "topic2_p4", "priority": 4}
    ]

    # Send messages to topic 1
    for msg in messages_topic1:
        producer.send(TOPIC, value=msg, priority=msg["priority"])
        print(f"Produced to {TOPIC}: {msg}")

    # Send messages to topic 2
    for msg in messages_topic2:
        producer.send(TOPIC2, value=msg, priority=msg["priority"])
        print(f"Produced to {TOPIC2}: {msg}")

    producer.flush()
    producer.close()

def consumer_thread_multi_topic(received_messages):
    print(f"Consumer subscribing to topics: {TOPIC}, {TOPIC2} with group ID: {GROUP_ID}")
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[TOPIC, TOPIC2],
        group_id=GROUP_ID,
        max_poll_records=500,
        consumer_timeout_ms=10000,
        config_file=CONFIG_FILE,
        auto_offset_reset='earliest'  # Start reading from the beginning
    )

    print("Polling for prioritized messages from multiple topics...")
    start_time = time.time()
    while len(received_messages) < 30 and time.time() - start_time < 15:
        batch = consumer.poll(timeout_ms=1000, max_records=500)
        if batch:
            print(f"Received batch of {len(batch)} messages")
            for message in batch:
                if message.value not in received_messages:
                    received_messages.append(message.value)
                    print(f"Added new message: {message.value}")
            print(f"Total unique messages received so far: {len(received_messages)}")
        else:
            print("No messages received in this poll")

    consumer.close()

def test_priority_order_multi_topic():
    # Clear topics before starting the test
    clear_topics()
    
    # Create a list to store received messages
    received_messages = []
    
    # Create and start threads
    producer = threading.Thread(target=producer_thread_multi_topic)
    consumer = threading.Thread(target=consumer_thread_multi_topic, args=(received_messages,))
    
    # Start producer first
    producer.start()
    time.sleep(2)  # Give producer time to send messages
    
    # Start consumer
    consumer.start()
    
    # Wait for both threads to complete
    producer.join()
    consumer.join()

    print(f"\nTotal messages received: {len(received_messages)}")
    
    # Print the order in which messages were received (topic, data, priority)
    print("\nOrder of received messages (topic, data, priority):")
    for msg in received_messages:
        topic = "topic1" if msg["data"].startswith("topic1") else ("topic2" if msg["data"].startswith("topic2") else "unknown")
        print(f"{topic}: {msg['data']} (priority {msg['priority']})")
    
    # Verify messages are received in descending priority order
    priorities = [msg["priority"] for msg in received_messages]
    assert priorities == sorted(priorities, reverse=True), \
        f"Expected descending priorities, got: {priorities}"

    # Verify we received messages from both topics
    topic1_messages = [msg for msg in received_messages if msg["data"].startswith("topic1")]
    topic2_messages = [msg for msg in received_messages if msg["data"].startswith("topic2")]
    
    print(f"\nMessages from {TOPIC}: {len(topic1_messages)}")
    print(f"Messages from {TOPIC2}: {len(topic2_messages)}")
    
    assert len(topic1_messages) == 5, f"Expected 5 messages from {TOPIC}, got {len(topic1_messages)}"
    assert len(topic2_messages) == 5, f"Expected 5 messages from {TOPIC2}, got {len(topic2_messages)}"

    print("✅ Test passed: messages received in priority order from multiple topics.")
    print("\nMessages from Topic 1:")
    for msg in topic1_messages:
        print(msg)
    print("\nMessages from Topic 2:")
    for msg in topic2_messages:
        print(msg)

if __name__ == "__main__":
    print("Running single topic test...")
    test_priority_order()
    print("\nRunning multi-topic test...")
    test_priority_order_multi_topic()
