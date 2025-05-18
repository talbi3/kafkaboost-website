from kafka import KafkaConsumer
from typing import Any, Optional, Union, List
import json

class KafkaboostConsumer:
    def __init__(
        self,
        bootstrap_servers: Union[str, List[str]],
        topics: Union[str, List[str]],
        group_id: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostConsumer.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            topics: Topic(s) to consume from
            group_id: Consumer group ID
            **kwargs: Additional arguments to pass to KafkaConsumer
        """
        # Convert single topic to list
        topics_list = [topics] if isinstance(topics, str) else topics
        
        self.consumer = KafkaConsumer(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            **kwargs
        )
        self.consumer.subscribe(topics_list)
    
    def poll(
        self,
        timeout_ms: int = 1000,
        max_records: Optional[int] = None,
        **kwargs: Any
    ) -> Any:
        """
        Poll for new messages.
        
        Args:
            timeout_ms: Time to wait for messages
            max_records: Maximum number of records to return
            **kwargs: Additional arguments to pass to KafkaConsumer.poll()
            
        Returns:
            ConsumerRecords
        """
        return self.consumer.poll(
            timeout_ms=timeout_ms,
            max_records=max_records,
            **kwargs
        )
    
    def close(self) -> None:
        """Close the consumer."""
        self.consumer.close() 