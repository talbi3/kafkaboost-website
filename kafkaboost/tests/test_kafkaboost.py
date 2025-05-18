import threading
import time
from typing import List, Dict
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.consumer import KafkaboostConsumer

class TestProducer(threading.Thread):
    def __init__(self, bootstrap_servers: str, topics: List[str]):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.messages_sent: List[Dict] = []
        self.stop_event = threading.Event()
        
    def run(self):
        producer = KafkaboostProducer(
            bootstrap_servers=self.bootstrap_servers,
            priority=50  # Default priority
        )
        
        try:
            # Test messages with different priorities and topics
            messages = [
                {'topic': self.topics[0], 'data': 'Low priority message', 'priority': 10},
                {'topic': self.topics[0], 'data': 'Medium priority message', 'priority': 50},
                {'topic': self.topics[0], 'data': 'High priority message', 'priority': 90},
                {'topic': self.topics[1], 'data': 'Custom priority message', 'priority': 75},
                {'topic': self.topics[1], 'data': 'Very high priority message', 'priority': 95},
                {'topic': self.topics[0], 'data': 'Another medium priority message', 'priority': 50}
            ]
            
            for msg in messages:
                if self.stop_event.is_set():
                    break
                    
                # Send the message
                producer.send(
                    msg['topic'],
                    value=msg['data'],
                    priority=msg['priority']
                )
                self.messages_sent.append(msg)
                print(f"Producer sent to {msg['topic']}: {msg}")
                time.sleep(0.5)  # Small delay between messages
                
        finally:
            producer.close()
            
    def stop(self):
        self.stop_event.set()

class TestConsumer(threading.Thread):
    def __init__(self, bootstrap_servers: str, topics: List[str], group_id: str):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.messages_received: List[Dict] = []
        self.stop_event = threading.Event()
        
    def run(self):
        consumer = KafkaboostConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topics=self.topics,
            group_id=self.group_id
        )
        
        try:
            while not self.stop_event.is_set():
                # Poll for messages
                records = consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in records.items():
                    for message in messages:
                        value = message.value
                        self.messages_received.append({
                            'topic': topic_partition.topic,
                            'data': value
                        })
                        print(f"Consumer received from {topic_partition.topic}: {value}")
                        
                # Small delay to prevent CPU overuse
                time.sleep(0.1)
                
        finally:
            consumer.close()
            
    def stop(self):
        self.stop_event.set()

def verify_message_order(messages: List[Dict]) -> bool:
    """Verify that messages are received in priority order within each topic."""
    topic_messages = {}
    for msg in messages:
        topic = msg['topic']
        if topic not in topic_messages:
            topic_messages[topic] = []
        topic_messages[topic].append(msg)
    
    for topic, msgs in topic_messages.items():
        priorities = [msg['priority'] for msg in msgs]
        if priorities != sorted(priorities, reverse=True):
            return False
    return True

def main():
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topics = ['test_topic_1', 'test_topic_2']
    group_id = 'test_group'
    
    # Create and start producer and consumer
    producer = TestProducer(bootstrap_servers, topics)
    consumer = TestConsumer(bootstrap_servers, topics, group_id)
    
    print("Starting consumer...")
    consumer.start()
    
    # Give the consumer time to initialize
    time.sleep(2)
    
    print("Starting producer...")
    producer.start()
    
    try:
        # Let them run for a while
        time.sleep(15)
    finally:
        # Stop both threads
        print("Stopping producer and consumer...")
        producer.stop()
        consumer.stop()
        producer.join()
        consumer.join()
        
        # Print summary
        print("\nTest Summary:")
        print(f"Producer sent {len(producer.messages_sent)} messages")
        print(f"Consumer received {len(consumer.messages_received)} messages")
        
        # Verify messages
        if producer.messages_sent and consumer.messages_received:
            print("\nVerifying messages...")
            for sent, received in zip(producer.messages_sent, consumer.messages_received):
                print(f"Sent to {sent['topic']}: {sent}")
                print(f"Received from {received['topic']}: {received}")
                print("---")
            
            # Verify message order
            if verify_message_order(consumer.messages_received):
                print("\nMessage order verification: PASSED")
            else:
                print("\nMessage order verification: FAILED - Messages not received in priority order")

if __name__ == "__main__":
    main() 