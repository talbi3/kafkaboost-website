from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable
import json
from typing import Dict, List, Optional, Union
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class KafkaConfigManager:
    def __init__(self, bootstrap_servers: str, config_file: str):
        """
        Initialize the KafkaConfigManager.
        
        Args:
            bootstrap_servers: Kafka server address(es)
            config_file: Path to the JSON configuration file
        """
        self.bootstrap_servers = bootstrap_servers
        self.config_file = config_file
        self.config = self._load_config()
        self.admin_client = None
        
    def _load_config(self) -> Dict:
        """Load configuration from JSON file."""
        try:
            with open(self.config_file, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {self.config_file}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in configuration file: {self.config_file}")
            raise
            
    def connect(self) -> bool:
        """Establish connection to Kafka admin client."""
        try:
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=self.bootstrap_servers
            )
            logger.info("Successfully connected to Kafka admin client")
            return True
        except NoBrokersAvailable:
            logger.error(f"Could not connect to Kafka brokers at {self.bootstrap_servers}")
            return False
            
    def get_topic_config(self, topics: Union[str, List[str]]) -> Dict[str, Optional[Dict]]:
        """
        Get configuration for one or more topics.

        Args:
            topics: Single topic name or list of topic names

        Returns:
            Dictionary mapping topic names to their configurations
        """
        if not self.admin_client:
            if not self.connect():
                return {}

        topic_list = [topics] if isinstance(topics, str) else topics

        try:
            config_resources = [
                ConfigResource(ConfigResourceType.TOPIC, topic)
                for topic in topic_list
            ]
            # Get configurations
            configs_response_list = self.admin_client.describe_configs(config_resources)

            result = {}
            for response in configs_response_list:
                for resource in response.resources:
                    error_code, error_message, resource_type, resource_name, config_entries = resource

                    if error_code != 0:
                        logger.error(f"Error from broker for topic '{resource_name}': {error_message}")
                        result[resource_name] = None
                        continue

                    # Unpack tuple-style entries: (config_name, config_value, ...)
                    topic_config = {
                        entry[0]: entry[1]
                        for entry in config_entries
                    }
                    result[resource_name] = topic_config

            # Ensure all topics are present
            for topic in topic_list:
                if topic not in result:
                    result[topic] = None

            return result

        except Exception as e:
            logger.error(f"Error getting topic configs: {str(e)}")
            return {topic: None for topic in topic_list}
            
    def get_all_topic_configs(self) -> Dict[str, Optional[Dict]]:
        """
        Get configurations for all topics in the cluster.
        
        Returns:
            Dictionary mapping all topic names to their configurations
        """
        if not self.admin_client:
            if not self.connect():
                return {}
                
        try:
            # Get list of all topics
            topics = self.admin_client.list_topics()
            return self.get_topic_config(topics)
        except Exception as e:
            logger.error(f"Error getting all topic configs: {str(e)}")
            return {}
            
    def check_and_create_priority_topics(self) -> bool:
        if not self.admin_client:
            if not self.connect():
                return False

        priority_config = self.config.get('Priority_boost', [])
        if not priority_config:
            logger.warning("No priority configuration found in config file")
            return False

        self.max_priority = self.config.get('max_priority', 10)

        existing_topics = self._get_existing_topics()

        for topic_entry in priority_config:
            topic_name = topic_entry.get("topic_name")
            min_priority = topic_entry.get("priority_boost_min_value", 0)
            if not topic_name:
                continue

            for i in range(min_priority, self.max_priority + 1):
                full_topic = f"{topic_name}_{i}"
                if full_topic not in existing_topics:
                    try:
                        new_topic = NewTopic(
                            name=full_topic,
                            num_partitions=1,
                            replication_factor=1
                        )
                        self.admin_client.create_topics([new_topic])
                        logger.info(f"Created topic: {full_topic}")
                    except TopicAlreadyExistsError:
                        logger.info(f"Topic {full_topic} already exists")
                    except Exception as e:
                        logger.error(f"Error creating topic {full_topic}: {str(e)}")
                        return False

        return True

    def _get_existing_topics(self) -> List[str]:
        """Get list of existing topics."""
        try:
            return self.admin_client.list_topics()
        except Exception as e:
            logger.error(f"Error listing topics: {str(e)}")
            return []
            
    def close(self):
        """Close the admin client connection."""
        if self.admin_client:
            self.admin_client.close()
            self.admin_client = None 