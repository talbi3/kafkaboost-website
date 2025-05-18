import threading
import time
from typing import List, Dict
from kafkaboost.producer import KafkaboostProducer
from kafkaboost.consumer import KafkaboostConsumer

class TestProducer(threading.Thread):
    def __init__(self, bootstrap_servers: str, topic: str):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.messages_sent: List[Dict] = []
        self.stop_event = threading.Event()
        
    def run(self):
        producer = KafkaboostProducer(
            bootstrap_servers=self.bootstrap_servers,
            priority=50  # Default priority
        )
        
        try:
            # Test messages with different priorities
            messages = [
                {'data': 'Low priority message', 'priority': 10},
                {'data': 'Medium priority message', 'priority': 50},
                {'data': 'High priority message', 'priority': 90},
                {'data': 'Very high priority message', 'priority': 95},
                {'data': 'Another medium priority message', 'priority': 50}
            ]
            
            for msg in messages:
                if self.stop_event.is_set():
                    break
                    
                # Send the message
                producer.send(
                    self.topic,
                    value=msg['data'],
                    priority=msg['priority']
                )
                self.messages_sent.append(msg)
                print(f"Producer sent: {msg}")
                time.sleep(1)  # Small delay between messages
                
        finally:
            producer.close()
            
    def stop(self):
        self.stop_event.set()

class TestConsumer(threading.Thread):
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str):
        super().__init__()
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.group_id = group_id
        self.messages_received: List[Dict] = []
        self.stop_event = threading.Event()
        
    def run(self):
        consumer = KafkaboostConsumer(
            bootstrap_servers=self.bootstrap_servers,
            topics=self.topic,
            group_id=self.group_id
        )
        
        try:
            while not self.stop_event.is_set():
                # Poll for messages
                records = consumer.poll(timeout_ms=1000)
                
                for topic_partition, messages in records.items():
                    for message in messages:
                        value = message.value
                        self.messages_received.append(value)
                        print(f"Consumer received: {value}")
                        
                # Small delay to prevent CPU overuse
                time.sleep(0.1)
                
        finally:
            consumer.close()
            
    def stop(self):
        self.stop_event.set()

def main():
    # Configuration
    bootstrap_servers = 'localhost:9092'
    topic = 'test_topic'
    group_id = 'test_group'
    
    # Create and start producer and consumer
    producer = TestProducer(bootstrap_servers, topic)
    consumer = TestConsumer(bootstrap_servers, topic, group_id)
    
    print("Starting consumer...")
    consumer.start()
    
    # Give the consumer time to initialize
    time.sleep(2)
    
    print("Starting producer...")
    producer.start()
    
    try:
        # Let them run for a while
        time.sleep(10)
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
                print(f"Sent: {sent}")
                print(f"Received: {received}")
                print("---")

if __name__ == "__main__":
    main()
