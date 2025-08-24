from kafka import KafkaConsumer
from typing import Any, Optional, Union, List, Dict, Tuple
import json
from queue import PriorityQueue
from dataclasses import dataclass
from datetime import datetime
import time
from collections import defaultdict
from kafkaboost.kafka_utils import KafkaConfigManager
from .config_manager import ConfigManager


class KafkaboostConsumer(KafkaConsumer):
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        number_of_messages: Optional[int] = None,
        config_file: Optional[str] = None,  
        use_config_manager: bool = True,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostConsumer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics: Topic(s) to consume from
            group_id: Consumer group ID
    
            use_config_manager: Whether to use ConfigManager for configuration (default: True)
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
        
        # Initialize ConfigManager if requested
        self.use_config_manager = use_config_manager
        self.config_manager = None
        self.boost_config = {}
        
        if use_config_manager:
            try:
                self.config_manager = ConfigManager()
                if config_file:
                    # Load config from file if provided
                    self.config_manager.load_config_from_file(config_file)
                else:
                    # Get config from server
                    self.config_manager.get_config()
                print("✓ Consumer initialized with ConfigManager")
            except Exception as e:
                print(f"Warning: Could not initialize ConfigManager: {str(e)}")
                # Fall back to config file if ConfigManager fails
                if config_file:
                    self.config_manager = None
                    with open(config_file, 'r') as f:
                        self.boost_config = json.load(f)
                else:
                    self.boost_config = {}
        else:
            # Use config file directly (legacy mode)
            if config_file:
                with open(config_file, 'r') as f:
                    self.boost_config = json.load(f)
        
        # Initialize KafkaConfigManager only if config_file is provided
        self.kafka_config_manager = None
        if config_file:
            self.kafka_config_manager = KafkaConfigManager(
                bootstrap_servers=bootstrap_servers,
                config_file=config_file
            )
            boost_topics = self.kafka_config_manager.find_matching_topics(topics_list)
            # Subscribe to topics
            self.subscribe(topics_list)
        else:
            # Subscribe to topics directly if no config file
            self.subscribe(topics_list)
        
        # Initialize iterator-related variables
        self._iterator = None
        self._consumer_timeout = float('inf')
        self._last_poll_time = datetime.now().timestamp()

        # Load priority settings from config
        if self.use_config_manager and self.config_manager:
            self.max_priority = self.config_manager.get_max_priority()
        else:
            self.max_priority = self.boost_config.get('max_priority', 10) if self.boost_config else 10

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

   

    def close(self) -> None:
        """Close the consumer."""
        super().close()