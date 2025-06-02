import json
import os
import time
from kafka.admin import NewTopic, KafkaAdminClient
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
from kafkaboost.kafka_utils import KafkaConfigManager



CONFIG_FILE = "/Users/noaalt/Projects/kafkaboost_repo/kafkaboost/tests/sample_config.json"
BOOTSTRAP_SERVERS = "localhost:9092"

def write_config():
    with open(CONFIG_FILE, "r") as f:
        SAMPLE_CONFIG = json.load(f)


def assert_true(condition, message):
    if condition:
        print(f"[PASS] {message}")
    else:
        print(f"[FAIL] {message}")
        raise AssertionError(message)

def main():
    print("=== Manual KafkaConfigManager Test ===")
    write_config()

    try:
        print("→ Creating manager and connecting...")
        manager = KafkaConfigManager(BOOTSTRAP_SERVERS, CONFIG_FILE)
        assert_true(manager.connect(), "Connected to Kafka")

        print("→ Deleting test topic (if exists)...")
        try:
            manager.admin_client.delete_topics(["test_topic"])
            time.sleep(1)
        except Exception:
            pass

        print("→ Creating test topic...")
        topic = NewTopic(name="test_topic", num_partitions=1, replication_factor=1)
        try:
            manager.admin_client.create_topics([topic])
            time.sleep(1)
        except TopicAlreadyExistsError:
            print("⚠️  test_topic already exists")

        print("→ Fetching topic config...")
        result = manager.get_topic_config("test_topic")
        assert_true("test_topic" in result, "Topic config retrieved")
        assert_true(isinstance(result["test_topic"], dict), "Topic config is a dictionary")

        print("→ Running check_and_create_priority_topics...")
        result2 = manager.check_and_create_priority_topics()
        assert_true(result2, "Priority topics created")

        topics = manager.admin_client.list_topics()
        for i in range(5,11):
            topic_name = f"test_topic_{i}"
            assert_true(topic_name in topics, f"Priority topic {topic_name} exists")

        print("→ Cleaning up created topics...")
        manager.admin_client.delete_topics(["test_topic"] + [f"test_topic_{i}" for i in range(5,10)])
        time.sleep(1)

        manager.close()
        print("→ Connection closed.")
        print("✅ All tests completed successfully.")
    finally:
        cleanup_config()

if __name__ == "__main__":
    main()
