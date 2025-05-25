from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from typing import Any, Optional, Union
import json

class KafkaboostProducer(KafkaProducer):
    def __init__(
        self,
        bootstrap_servers: Union[str, list],
        config_file: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostProducer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            config_file: Path to the JSON configuration file
            **kwargs: Additional arguments to pass to KafkaProducer
        """
        self.config = {}
        if config_file:
            with open(config_file, 'r') as f:
                """NEEDS TO BE CHANGED"""
                self.config = json.load(f)
        
        self.max_priority = self.config.get('max_priority', 10)
        self.default_priority = self.config.get('default_priority', 0)
        
        # Initialize the parent KafkaProducer with value serializer
        super().__init__(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            **{k: v for k, v in kwargs.items() if k != 'priority'}
        )
    
    def _prepare_message(self, value: Any, priority: Optional[int] = None, topic: Optional[str] = None) -> dict:
        """
        Prepare message by converting value to dictionary with priority.
        
        Args:
            value: Message value (can be any type)
            priority: Override the default priority for this message
            
        Returns:
            dict: Message dictionary with priority
        """
        if priority is not None:
            message_priority = priority
        else:
            message_priority= self.check_priority(value, topic)
        
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
    
    def check_priority(self, message_dict: dict, topic: str) -> dict:
        """
        Add priority to the message based on the role and topic.
        
        Args:
            message_dict: The message dictionary to which priority will be added
            topic: The topic name
            
        Returns:
            dict: Message dictionary with priority added based on role and topic
        """
        # Check if the message contains a role
        role = message_dict.get('role')
        if role:
            for rule in self.config.get('Rule_Base_priority', []):
                if rule['role_name'] == role:
                    return rule['priority']
        
        # If no role is found or no matching rule, check topic priority
        for topic_rule in self.config.get('Topics_priority', []):
            if topic_rule['topic'] == topic:
                return topic_rule['priority']
        
        # If no matching topic, use default priority
        return self.default_priority
    

      
    
 