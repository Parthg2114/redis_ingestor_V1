"""
Redis Client Module

This module provides Redis client functionality with connection pooling,
error handling, and data storage operations for MQTT messages.
"""

import json
import time
from typing import Dict, Any, List, Optional, Union
from contextlib import contextmanager
import redis
from redis.connection import ConnectionPool
from redis.exceptions import (
    ConnectionError as RedisConnectionError,
    TimeoutError as RedisTimeoutError,
    RedisError
)

from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    RedisConnectionError as CustomRedisConnectionError,
    RedisOperationError,
    ConfigurationError
)


class RedisClient:
    """
    Redis client with connection pooling and error handling.
    """
    
    def __init__(self):
        """Initialize Redis client with connection pool."""
        self.logger = get_logger(__name__)
        self.config = config_manager.get_redis_config()
        
        if not self.config:
            raise ConfigurationError("Redis configuration not found")
        
        self.connection_pool = None
        self.redis_client = None
        self.key_prefix = self.config.get('key_prefix', 'mqtt_data:')
        self.default_ttl = self.config.get('ttl', 3600)
        
        self._setup_connection_pool()
        self._setup_client()
    
    def _setup_connection_pool(self):
        """Set up Redis connection pool."""
        try:
            pool_config = self.config.get('connection_pool', {})
            
            self.connection_pool = ConnectionPool(
    host=self.config.get('host', 'localhost'),
    port=self.config.get('port', 6379),
    db=self.config.get('db', 0),
    username=self.config.get('username'),  # <-- Add this line
    password=self.config.get('password'),
    max_connections=pool_config.get('max_connections', 10),
    retry_on_timeout=pool_config.get('retry_on_timeout', True),
    socket_connect_timeout=pool_config.get('socket_connect_timeout', 5),
    socket_timeout=pool_config.get('socket_timeout', 5)
)

            
            self.logger.info("Redis connection pool configured")
            
        except Exception as e:
            self.logger.error(f"Failed to setup Redis connection pool: {e}")
            raise CustomRedisConnectionError(f"Connection pool setup failed: {e}")
    
    def _setup_client(self):
        """Set up Redis client using connection pool."""
        try:
            self.redis_client = redis.Redis(
                connection_pool=self.connection_pool,
                decode_responses=True,
                retry_on_error=[RedisConnectionError, RedisTimeoutError]
            )
            
            # Test connection
            self.redis_client.ping()
            self.logger.info("Redis client connected successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to setup Redis client: {e}")
            raise CustomRedisConnectionError(f"Redis client setup failed: {e}")
    
    @contextmanager
    def get_connection(self):
        """
        Context manager for getting Redis connection.
        
        Yields:
            Redis connection instance
        """
        connection = None
        try:
            connection = self.connection_pool.get_connection('default')
            yield connection
        except Exception as e:
            self.logger.error(f"Error with Redis connection: {e}")
            raise RedisOperationError(f"Connection error: {e}")
        finally:
            if connection:
                self.connection_pool.release(connection)
    
    def _format_key(self, key: str) -> str:
        """
        Format Redis key with prefix.
        
        Args:
            key: Original key
            
        Returns:
            Formatted key with prefix
        """
        return f"{self.key_prefix}{key}"
    
    def store_message(self, topic: str, message_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Store MQTT message in Redis.
        
        Args:
            topic: MQTT topic
            message_data: Message data dictionary
            ttl: Time to live in seconds (optional)
            
        Returns:
            bool: True if stored successfully
        """
        try:
            # Create unique key for message
            timestamp = message_data.get('timestamp', time.time())
            key = self._format_key(f"{topic}:{int(timestamp * 1000000)}")  # Microsecond precision
            
            # Serialize message data
            serialized_data = json.dumps(message_data)
            
            # Store in Redis
            result = self.redis_client.set(
                key, 
                serialized_data, 
                ex=ttl or self.default_ttl
            )
            
            if result:
                self.logger.debug(f"Stored message for topic {topic} with key {key}")
                return True
            else:
                self.logger.error(f"Failed to store message for topic {topic}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error storing message: {e}")
            raise RedisOperationError(f"Failed to store message: {e}")
    
    def store_messages_batch(self, messages: List[Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """
        Store multiple MQTT messages in Redis using pipeline.
        
        Args:
            messages: List of message data dictionaries
            ttl: Time to live in seconds (optional)
            
        Returns:
            int: Number of messages stored successfully
        """
        if not messages:
            return 0
        
        try:
            pipeline = self.redis_client.pipeline()
            stored_count = 0
            
            for message_data in messages:
                try:
                    topic = message_data.get('topic', 'unknown')
                    timestamp = message_data.get('timestamp', time.time())
                    key = self._format_key(f"{topic}:{int(timestamp * 1000000)}")
                    
                    serialized_data = json.dumps(message_data)
                    pipeline.set(key, serialized_data, ex=ttl or self.default_ttl)
                    stored_count += 1
                    
                except Exception as e:
                    self.logger.error(f"Error preparing message for batch storage: {e}")
            
            # Execute pipeline
            results = pipeline.execute()
            successful_stores = sum(1 for result in results if result)
            
            self.logger.info(f"Batch stored {successful_stores}/{len(messages)} messages")
            return successful_stores
            
        except Exception as e:
            self.logger.error(f"Error in batch store operation: {e}")
            raise RedisOperationError(f"Batch store failed: {e}")
    
    def get_messages_by_topic(self, topic: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Retrieve messages for a specific topic.
        
        Args:
            topic: MQTT topic
            limit: Maximum number of messages to retrieve
            
        Returns:
            List of message data dictionaries
        """
        try:
            pattern = self._format_key(f"{topic}:*")
            keys = self.redis_client.keys(pattern)
            
            if not keys:
                return []
            
            # Sort keys to get newest first (based on timestamp in key)
            keys.sort(reverse=True)
            keys = keys[:limit]
            
            # Get messages
            messages = []
            for key in keys:
                try:
                    data = self.redis_client.get(key)
                    if data:
                        message_data = json.loads(data)
                        messages.append(message_data)
                except Exception as e:
                    self.logger.error(f"Error deserializing message from key {key}: {e}")
            
            self.logger.debug(f"Retrieved {len(messages)} messages for topic {topic}")
            return messages
            
        except Exception as e:
            self.logger.error(f"Error retrieving messages for topic {topic}: {e}")
            raise RedisOperationError(f"Failed to retrieve messages: {e}")
    
    def get_recent_messages(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get recent messages across all topics.
        
        Args:
            limit: Maximum number of messages to retrieve
            
        Returns:
            List of recent message data dictionaries
        """
        try:
            pattern = self._format_key("*")
            keys = self.redis_client.keys(pattern)
            
            if not keys:
                return []
            
            # Sort keys to get newest first
            keys.sort(reverse=True)
            keys = keys[:limit]
            
            # Get messages
            messages = []
            pipeline = self.redis_client.pipeline()
            
            for key in keys:
                pipeline.get(key)
            
            results = pipeline.execute()
            
            for key, data in zip(keys, results):
                if data:
                    try:
                        message_data = json.loads(data)
                        messages.append(message_data)
                    except Exception as e:
                        self.logger.error(f"Error deserializing message from key {key}: {e}")
            
            self.logger.debug(f"Retrieved {len(messages)} recent messages")
            return messages
            
        except Exception as e:
            self.logger.error(f"Error retrieving recent messages: {e}")
            raise RedisOperationError(f"Failed to retrieve recent messages: {e}")
    
    def delete_messages_by_topic(self, topic: str) -> int:
        """
        Delete all messages for a specific topic.
        
        Args:
            topic: MQTT topic
            
        Returns:
            int: Number of messages deleted
        """
        try:
            pattern = self._format_key(f"{topic}:*")
            keys = self.redis_client.keys(pattern)
            
            if not keys:
                return 0
            
            deleted_count = self.redis_client.delete(*keys)
            self.logger.info(f"Deleted {deleted_count} messages for topic {topic}")
            return deleted_count
            
        except Exception as e:
            self.logger.error(f"Error deleting messages for topic {topic}: {e}")
            raise RedisOperationError(f"Failed to delete messages: {e}")
    
    def get_topic_statistics(self) -> Dict[str, int]:
        """
        Get statistics about messages per topic.
        
        Returns:
            Dictionary with topic names as keys and message counts as values
        """
        try:
            pattern = self._format_key("*")
            keys = self.redis_client.keys(pattern)
            
            topic_counts = {}
            
            for key in keys:
                # Extract topic from key (remove prefix and timestamp)
                key_without_prefix = key[len(self.key_prefix):]
                topic = key_without_prefix.rsplit(':', 1)[0]  # Remove timestamp part
                
                if topic in topic_counts:
                    topic_counts[topic] += 1
                else:
                    topic_counts[topic] = 1
            
            self.logger.debug(f"Topic statistics: {topic_counts}")
            return topic_counts
            
        except Exception as e:
            self.logger.error(f"Error getting topic statistics: {e}")
            raise RedisOperationError(f"Failed to get statistics: {e}")
    
    def health_check(self) -> bool:
        """
        Perform health check on Redis connection.
        
        Returns:
            bool: True if healthy, False otherwise
        """
        try:
            response = self.redis_client.ping()
            if response:
                self.logger.debug("Redis health check passed")
                return True
            else:
                self.logger.warning("Redis health check failed - no response")
                return False
        except Exception as e:
            self.logger.error(f"Redis health check failed: {e}")
            return False
    
    def close(self):
        """Close Redis connections."""
        try:
            if self.connection_pool:
                self.connection_pool.disconnect()
            self.logger.info("Redis connections closed")
        except Exception as e:
            self.logger.error(f"Error closing Redis connections: {e}")
    
    @property
    def connection_info(self) -> Dict[str, Any]:
        """Get Redis connection information."""
        return {
            'host': self.config.get('host', 'localhost'),
            'port': self.config.get('port', 6379),
            'db': self.config.get('db', 0),
            'pool_max_connections': self.config.get('connection_pool', {}).get('max_connections', 10)
        }