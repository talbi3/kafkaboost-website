from kafka import KafkaProducer
from typing import Any, Optional, Union
import json

class KafkaboostProducer:
    def __init__(
        self,
        bootstrap_servers: Union[str, list],
        priority: int = 0,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostProducer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            priority: Message priority (0-100, where 0 is lowest priority)
            **kwargs: Additional arguments to pass to KafkaProducer
        """
        if not isinstance(priority, int) or priority < 0 or priority > 100:
            raise ValueError("Priority must be an integer between 0 and 100")
            
        self.priority = priority
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **kwargs
        )
    
    def send(
        self,
        topic: str,
        value: Optional[Union[str, dict, bytes]] = None,
        key: Optional[Union[str, bytes]] = None,
        priority: Optional[int] = None,
        **kwargs: Any
    ) -> Any:
        """
        Send a message to Kafka with priority.
        
        Args:
            topic: Topic to send the message to
            value: Message value (can be string, dict, or bytes)
            key: Message key
            priority: Override the default priority for this message
            **kwargs: Additional arguments to pass to KafkaProducer.send()
            
        Returns:
            FutureRecordMetadata
        """
        # Use message-specific priority if provided, otherwise use default
        message_priority = priority if priority is not None else self.priority
        
        # Create message dictionary with priority
        if isinstance(value, dict):
            message_dict = value.copy()
        else:
            message_dict = {'data': value}
            
        message_dict['priority'] = message_priority
        
        return self.producer.send(topic, value=message_dict, key=key, **kwargs)
    
    def flush(self) -> None:
        """Flush any pending messages."""
        self.producer.flush()
    
    def close(self) -> None:
        """Close the producer."""
        self.producer.close() 