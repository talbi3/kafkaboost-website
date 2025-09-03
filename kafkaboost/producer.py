from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient
from typing import Any, Optional, Union
import json
from .s3_config_manager import S3ConfigManager


class KafkaboostProducer(KafkaProducer):
    def __init__(
        self,
        bootstrap_servers: Union[str, list],
        config_file: Optional[str] = None,
        user_id: Optional[str] = None,
        **kwargs: Any
    ):
        """
        Initialize the KafkaboostProducer with priority support.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            config_file: Path to the JSON configuration file (optional if using S3)
            user_id: User ID for S3 config manager (optional)
            **kwargs: Additional arguments to pass to KafkaProducer
        """
        self.boost_config = {}
        self.user_id = user_id
        self.s3_config_manager = None
        
        # Initialize configuration

        try:
            self.s3_config_manager = S3ConfigManager(user_id=user_id)
            self.boost_config = self.s3_config_manager.get_full_config_for_producer()
            print("✓ Producer initialized with S3ConfigManager")
        except Exception as e:
            print(f"Warning: Could not initialize S3ConfigManager: {str(e)}")
               
        
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
        print(f"Sending message to topic: {topic}")
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
            if self.s3_config_manager:
                role_priority = self.s3_config_manager.get_role_priority(role)
                if role_priority is not None:
                    return role_priority

        # If no role is found or no matching rule, check topic priority
        if self.s3_config_manager:
            topic_priority = self.s3_config_manager.get_topic_priority_by_name(topic)
            if topic_priority is not None:
                return topic_priority

        return self.s3_config_manager.get_default_priority()
    
    def check_and_change_topic_by_priority(self, topic: str, priority: int) -> str:
        """
        Check if the topic is configured in Priority_boost and change topic name if priority meets minimum threshold.
        
        Args:
            topic: The original topic name
            priority: The priority value of the message
            
        Returns:
            str: The modified topic name if priority boost applies, otherwise the original topic name
        """
        if self.s3_config_manager:
            min_priority = self.s3_config_manager.get_priority_boost_min_value(topic)
            print(f"Min priority: {min_priority}")
            print(f"Priority: {priority}")
            print(f"Topic: {topic}")
            
            # Only route to priority-specific topic if there's actual priority boost config
            # and the priority meets the minimum threshold
            if min_priority > 0 and priority >= min_priority:
                modified_topic = f"{topic}_{priority}"
                return modified_topic
 
        
        # If no Priority_boost configuration found or priority doesn't meet threshold
        return topic
    
   
      
    
 