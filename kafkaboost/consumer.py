from kafka import KafkaConsumer
from typing import Any, Optional, Union, List, Dict, Tuple
import json
from queue import PriorityQueue
from dataclasses import dataclass
from datetime import datetime
import time
from collections import defaultdict
from kafkaboost.kafka_utils import KafkaConfigManager
from .s3_config_manager import S3ConfigManager


class KafkaboostConsumer(KafkaConsumer):
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        number_of_messages: Optional[int] = None,
        config_file: Optional[str] = None,  
        user_id: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostConsumer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics: Topic(s) to consume from
            group_id: Consumer group ID
            config_file: Path to config file (optional if using S3)
            user_id: User ID for S3 config manager (optional)
            **kwargs: Additional arguments to pass to KafkaConsumer
        """
        print("Initializing KafkaboostConsumer...")
        # Convert single topic to list
        topics_list = [topics] if isinstance(topics, str) else topics
        
        # Initialize parent KafkaConsumer
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')) if isinstance(v, bytes) else v,
            **kwargs
        )
        print("KafkaboostConsumer initialized")
        
        # Initialize configuration management
        self.user_id = user_id
        self.s3_config_manager = None
        self.boost_config = {}
        
     
        try:
            self.s3_config_manager = S3ConfigManager(user_id=user_id)
            print("✓ Consumer initialized with S3ConfigManager")
        except Exception as e:
            print(f"Warning: Could not initialize S3ConfigManager: {str(e)}")

        try:
            self.kafka_utils_manager = KafkaConfigManager(
                bootstrap_servers=bootstrap_servers,
                user_id=user_id
            )
            if self.kafka_utils_manager:
                boost_topics = self.kafka_utils_manager.find_matching_topics(topics_list)
                print(f"Found matching topics: {boost_topics}")
        except Exception as e:
                print(f"Warning: Could not initialize KafkaConfigManager: {str(e)}")
        
        # Subscribe to topics
        self.subscribe(topics_list)
        
        # Initialize iterator-related variables
        self._iterator = None
        self._consumer_timeout = float('inf')
        self._last_poll_time = datetime.now().timestamp()

        # Load priority settings from config
        if self.s3_config_manager:
            self.max_priority = self.s3_config_manager.get_max_priority()


    def _process_priority_messages(self, records: Dict) -> List:
        print("Processing priority messages...")

        queues = [[] for _ in range(self.max_priority + 1)]

        for tp, messages in records.items():
            for message in messages:
                priority = message.value.get("priority", 0)
                queues[priority].append(message)

        sorted_messages = []
        for queue in reversed(queues):
            sorted_messages.extend(queue)

        return sorted_messages
    
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
        print("Polling for messages in priority order...")
        raw_records = super().poll(
            timeout_ms=timeout_ms,
            max_records=max_records,
            **kwargs
        )
        
        # Sort messages by priority
        sorted_messages = self._process_priority_messages(raw_records)
        
        # Update last poll time
        self._last_poll_time = datetime.now().timestamp()
        
        return sorted_messages

    def _message_generator_v2(self):
        """Generator that yields messages in priority order."""
        timeout_ms = 1000 * max(0, self._consumer_timeout - time.time())
        record_map = super().poll(timeout_ms=timeout_ms, update_offsets=False)
        
        # Sort messages by priority
        sorted_messages = self._process_priority_messages(record_map)
        
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
        if hasattr(self, 'consumer_timeout_ms') and self.consumer_timeout_ms >= 0:
            self._consumer_timeout = time.time() + (
                self.consumer_timeout_ms / 1000.0)

    def refresh_config(self):
        """Refresh configuration from S3."""
        if self.s3_config_manager:
            try:
                self.boost_config = self.s3_config_manager.get_full_config_for_consumer()
                self.max_priority = self.s3_config_manager.get_max_priority()
                print("✓ Configuration refreshed from S3")
            except Exception as e:
                print(f"Warning: Failed to refresh config from S3: {str(e)}")

    def get_config_summary(self) -> dict:
        """
        Get a summary of the current configuration.
        
        Returns:
            Dictionary with configuration summary
        """
        if self.s3_config_manager:
            return self.s3_config_manager.get_config_summary()
        else:
            return {
                'config_source': 'none',
                'max_priority': self.max_priority,
                'topics_count': len(self.boost_config.get('Topics_priority', [])),
                'rules_count': len(self.boost_config.get('Rule_Base_priority', [])),
                'boost_configs_count': len(self.boost_config.get('Priority_boost', []))
            }

    def close(self) -> None:
        """Close the consumer."""
        super().close()