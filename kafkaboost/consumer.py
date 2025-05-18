from kafka import KafkaConsumer
from typing import Any, Optional, Union, List, Dict, Tuple
import json
from queue import PriorityQueue
from dataclasses import dataclass
from datetime import datetime
import time
from collections import defaultdict

@dataclass
class PrioritizedMessage:
    priority: int
    timestamp: float
    message: Any
    
    def __lt__(self, other):
        # Higher priority messages come first
        if self.priority != other.priority:
            return self.priority > other.priority
        # If priorities are equal, earlier timestamp comes first
        return self.timestamp < other.timestamp

class KafkaboostConsumer(KafkaConsumer):
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        number_of_messages: Optional[int] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostConsumer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics: Topic(s) to consume from
            group_id: Consumer group ID
            **kwargs: Additional arguments to pass to KafkaConsumer
        """
        # Convert single topic to list
        topics_list = [topics] if isinstance(topics, str) else topics
        
        # Initialize parent KafkaConsumer
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            **kwargs
        )
        
        # Subscribe to topics
        self.subscribe(topics_list)
        
        # Initialize iterator-related variables
        self._iterator = None
        self._consumer_timeout = float('inf')
        self._last_poll_time = datetime.now().timestamp()
        
    def _sort_messages_by_priority(self, records: Dict) -> List[Any]:
        """
        Sort messages by priority (descending) and timestamp (ascending).
        Each priority has its own queue. Partition/topic is ignored.
        """
        # Prepare queues for each priority
        priority_queues: Dict[int, Queue] = defaultdict(Queue)
        all_messages = []

        # Distribute messages into their priority queues
        for messages in records.values():
            for message in messages:
                priority = message.value.get('priority', 0)
                priority_queues[priority].put(message)

        # Merge all queues into a single list, ordered by priority and timestamp
        for priority in sorted(priority_queues.keys(), reverse=True):
            queue = priority_queues[priority]
            # Extract all messages from the queue and sort by timestamp
            priority_msgs = []
            while not queue.empty():
                priority_msgs.append(queue.get())
            # Sort by timestamp (ascending)
            priority_msgs.sort(key=lambda m: m.timestamp)
            all_messages.extend(priority_msgs)

        return all_messages
    
    def poll(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
        **kwargs: Any
    ) -> List[Any]:
        """
        Poll for new messages and return them sorted by priority.
        
        Args:
            timeout_ms: Time to wait for messages
            max_records: Maximum number of records to return
            **kwargs: Additional arguments to pass to KafkaConsumer.poll()
            
        Returns:
            List of messages sorted by priority and timestamp
        """
        # Get raw messages from parent class
        raw_records = super().poll(
            timeout_ms=timeout_ms,
            max_records=max_records,
            **kwargs
        )
        
        # Sort messages by priority
        sorted_messages = self._sort_messages_by_priority(raw_records)
        
        # Update last poll time
        self._last_poll_time = datetime.now().timestamp()
        
        return sorted_messages

    def _message_generator_v2(self):
        """Generator that yields messages in priority order."""
        timeout_ms = 1000 * max(0, self._consumer_timeout - time.time())
        record_map = super().poll(timeout_ms=timeout_ms, update_offsets=False)
        
        # Sort messages by priority
        sorted_messages = self._sort_messages_by_priority(record_map)
        
        # Yield messages in priority order
        for message in sorted_messages:
            if self._closed:
                break
            yield message

    def __iter__(self):
        """Return an iterator that yields messages in priority order."""
        return self

    def __next__(self):
        """Get the next message in priority order."""
        if self._closed:
            raise StopIteration('KafkaConsumer closed')
        
        self._set_consumer_timeout()
        
        while time.time() < self._consumer_timeout:
            if not self._iterator:
                self._iterator = self._message_generator_v2()
            try:
                return next(self._iterator)
            except StopIteration:
                self._iterator = None
                
        raise StopIteration()

    def _set_consumer_timeout(self):
        """Set the consumer timeout based on configuration."""
        if self.config['consumer_timeout_ms'] >= 0:
            self._consumer_timeout = time.time() + (
                self.config['consumer_timeout_ms'] / 1000.0)

    def close(self) -> None:
        """Close the consumer."""
        self.consumer.close() 