import time
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.consumer import KafkaboostConsumer

TOPIC = "test_topic_1"
BOOTSTRAP_SERVERS = "localhost:9092"
GROUP_ID = "priority_test_group"

def test_priority_order():
    # Step 1: Produce messages with different priorities
    producer = KafkaboostProducer(bootstrap_servers=BOOTSTRAP_SERVERS)

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

    # Step 2: Consume messages with priority logic
    consumer = KafkaboostConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        topics=[TOPIC],
        group_id=GROUP_ID
    )

    print("Polling for prioritized messages...")
    received = []
    start_time = time.time()
    while len(received) < len(messages) and time.time() - start_time < 10:
        batch = consumer.poll(timeout_ms=1000)
        received.extend([m.value for m in batch])

    consumer.close()

    # Step 3: Verify messages are received in descending priority order
    priorities = [msg["priority"] for msg in received]
    assert priorities == sorted(priorities, reverse=True), \
        f"Expected descending priorities, got: {priorities}"

    print("✅ Test passed: messages received in priority order.")
    for msg in received:
        print(msg)

if __name__ == "__main__":
    test_priority_order()
