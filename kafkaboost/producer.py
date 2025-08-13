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
        self.boost_config = {}
        if config_file:
            with open(config_file, 'r') as f:
                """NEEDS TO BE CHANGED"""
                self.boost_config = json.load(f)

        
        self.max_priority = self.boost_config.get('max_priority', 10)
        self.default_priority = self.boost_config.get('default_priority', 0)
        
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
        message_dict = self._prepare_message(value, priority, topic)
        topic = self.check_and_change_topic_by_priority(topic, message_dict['priority'])
        
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
            for role_config in self.boost_config.get('Rule_Base_priority', []):
                if role_config['role_name'] == role:
                    return role_config['priority']
        
        # If no role is found or no matching rule, check topic priority
        for topic_config in self.boost_config.get('Topics_priority', []):
            if topic_config['topic'] == topic:
                return topic_config['priority']
        
        # If no matching topic, use default priority
        return self.boost_config.get('default_priority', 0)
    
    def check_and_change_topic_by_priority(self, topic: str, priority: int) -> str:
        """
        Check if the topic is configured in Priority_boost and change topic name if priority meets minimum threshold.
        
        Args:
            topic: The original topic name
            priority: The priority value of the message
            
        Returns:
            str: The modified topic name if priority boost applies, otherwise the original topic name
        """
        priority_config = self.boost_config.get('Priority_boost',[])
        for topic_entry in priority_config:
            topic_name = topic_entry.get("topic_name")
            min_priority = topic_entry.get("priority_boost_min_value", 0)
            # Check if this topic matches the Priority_boost configuration
            if topic_name == topic:
                # If priority equals or exceeds the minimum value, change topic name
                if priority >= min_priority:
                    modified_topic = f"{topic_name}_{priority}"
                    return modified_topic
                break
        
        # If no Priority_boost configuration found or priority doesn't meet threshold
        return topic
    

      
    
 