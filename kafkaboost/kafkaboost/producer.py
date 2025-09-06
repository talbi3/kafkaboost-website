from kafka import KafkaProducer
from typing import Any, Optional, Union
import json

class KafkaboostProducer(KafkaProducer):
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
        
        # Initialize the parent KafkaProducer with value serializer
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **kwargs
        )
    
    def _prepare_message(self, value: Any, priority: Optional[int] = None) -> dict:
        """
        Prepare message by converting value to dictionary with priority.
        
        Args:
            value: Message value (can be any type)
            priority: Override the default priority for this message
            
        Returns:
            dict: Message dictionary with priority
        """
        message_priority = priority if priority is not None else self.priority
        
        if isinstance(value, dict):
            # If value is already a dict, add priority if not present
            message_dict = value.copy()
            if 'priority' not in message_dict:
                message_dict['priority'] = message_priority
        else:
            # Convert non-dict values to dict with 'data' field
            message_dict = {
                'data': value,
                'priority': message_priority
            }
            
        return message_dict
    
    def send(
        self,
        topic: str,
        value: Optional[Any] = None,
        key: Optional[Union[str, bytes]] = None,
        priority: Optional[int] = None,
        **kwargs: Any
    ) -> Any:
        """
        Send a message to Kafka with priority.
        
        Args:
            topic: Topic to send the message to
            value: Message value (can be any type)
            key: Message key
            priority: Override the default priority for this message
            **kwargs: Additional arguments to pass to KafkaProducer.send()
            
        Returns:
            FutureRecordMetadata
        """
        # Prepare message with priority
        message_dict = self._prepare_message(value, priority)
        
        # Call parent's send method with the modified message
        return super().send(topic, value=message_dict, key=key, **kwargs)
    
    def flush(self) -> None:
        """Flush any pending messages."""
        self.producer.flush()
    
    def close(self) -> None:
        """Close the producer."""
        self.producer.close() 