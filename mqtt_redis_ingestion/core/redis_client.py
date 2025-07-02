"""
Enhanced Redis Client Module for Universal Sensor Data Ingestion

This module provides Redis client functionality with connection pooling,
pipeline support, type conversion, and optimized storage for heterogeneous
sensor data from both industrial simulators and real sensors.
"""

import json
import time
from typing import Dict, Any, List, Optional, Union
from contextlib import contextmanager
from datetime import datetime
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


class EnhancedRedisClient:
    """
    Enhanced Redis client with universal sensor data support and pipeline operations.
    """

    def __init__(self):
        """Initialize Redis client with connection pool and enhanced features."""
        self.logger = get_logger(__name__)
        self.config = config_manager.get_redis_config()
        if not self.config:
            raise ConfigurationError("Redis configuration not found")

        self.connection_pool = None
        self.redis_client = None
        self.key_prefix = self.config.get('key_prefix', 'sensor_data:')
        self.default_ttl = self.config.get('ttl', 3600)
        
        # Type conversion settings
        self.boolean_conversion = self.config.get('boolean_conversion', 'string')  # 'string' or 'integer'

        self._setup_connection_pool()
        self._setup_client()

    def _setup_connection_pool(self):
        """Set up Redis connection pool with enhanced configuration."""
        try:
            pool_config = self.config.get('connection_pool', {})
            self.connection_pool = ConnectionPool(
                host=self.config.get('host', 'localhost'),
                port=self.config.get('port', 6379),
                db=self.config.get('db', 0),
                username=self.config.get('username'),
                password=self.config.get('password'),
                max_connections=pool_config.get('max_connections', 10),
                retry_on_timeout=pool_config.get('retry_on_timeout', True),
                socket_connect_timeout=pool_config.get('socket_connect_timeout', 5),
                socket_timeout=pool_config.get('socket_timeout', 5),
                decode_responses=True  # Automatically decode responses
            )
            self.logger.info("Enhanced Redis connection pool configured")

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
            self.logger.info("Enhanced Redis client connected successfully")

        except Exception as e:
            self.logger.error(f"Failed to setup Redis client: {e}")
            raise CustomRedisConnectionError(f"Redis client setup failed: {e}")

    def pipeline(self, transaction=True):
        """
        Get Redis pipeline for batch operations.
        
        Args:
            transaction: Whether to use transaction (default: True)
            
        Returns:
            Redis pipeline object
        """
        try:
            return self.redis_client.pipeline(transaction=transaction)
        except Exception as e:
            self.logger.error(f"Error creating Redis pipeline: {e}")
            raise RedisOperationError(f"Failed to create pipeline: {e}")

    def execute_pipeline(self, pipeline):
        """
        Execute a Redis pipeline.
        
        Args:
            pipeline: Redis pipeline object
            
        Returns:
            List of results from pipeline execution
        """
        try:
            return pipeline.execute()
        except Exception as e:
            self.logger.error(f"Error executing Redis pipeline: {e}")
            raise RedisOperationError(f"Failed to execute pipeline: {e}")

    @contextmanager
    def get_connection(self):
        """Context manager for getting Redis connection."""
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
        """Format Redis key with prefix."""
        return f"{self.key_prefix}{key}"

    def _convert_value_for_redis(self, value: Any) -> Union[str, int, float, bytes]:
        """
        Convert value to Redis-compatible type.
        
        Args:
            value: Value to convert
            
        Returns:
            Redis-compatible value
        """
        if isinstance(value, bool):
            if self.boolean_conversion == 'integer':
                return 1 if value else 0
            else:  # string
                return 'true' if value else 'false'
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        elif value is None:
            return 'null'
        elif isinstance(value, (str, int, float, bytes)):
            return value
        else:
            return str(value)

    def _convert_data_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert all values in a dictionary to Redis-compatible formats.
        
        Args:
            data: Dictionary with potentially incompatible types
            
        Returns:
            Redis-compatible dictionary
        """
        converted = {}
        for key, value in data.items():
            converted[key] = self._convert_value_for_redis(value)
        return converted

    # Enhanced Storage Methods for Universal Sensor Data
    def store_device_hash(self, device_key: str, data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Store device data as Redis hash with type conversion.
        
        Args:
            device_key: Redis key for the device
            data: Data to store as hash
            ttl: Time to live in seconds (optional)
            
        Returns:
            bool: True if stored successfully
        """
        try:
            # Convert all values to Redis-compatible types
            redis_safe_data = self._convert_data_for_redis(data)

            result = self.redis_client.hset(device_key, mapping=redis_safe_data)
            
            if ttl:
                self.redis_client.expire(device_key, ttl)
                
            self.logger.debug(f"Stored device hash with key {device_key}")
            return True

        except Exception as e:
            self.logger.error(f"Error storing device hash: {e}")
            raise RedisOperationError(f"Failed to store device hash: {e}")

    def store_archive_message(self, archive_key: str, message_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Store archived message with type conversion.
        
        Args:
            archive_key: Redis key for the archive
            message_data: Complete message data
            ttl: Time to live in seconds (optional)
            
        Returns:
            bool: True if stored successfully
        """
        try:
            # Convert all values to Redis-compatible types
            redis_safe_data = self._convert_data_for_redis(message_data)
            serialized_data = json.dumps(redis_safe_data)

            result = self.redis_client.set(archive_key, serialized_data, ex=ttl or self.default_ttl)
            
            self.logger.debug(f"Stored archive message with key {archive_key}")
            return bool(result)

        except Exception as e:
            self.logger.error(f"Error storing archive message: {e}")
            raise RedisOperationError(f"Failed to store archive message: {e}")

    # Query Methods for Simplified Storage (Archive + Latest Only)
    def get_latest_device_reading(self, device_id: str) -> Optional[Dict[str, Any]]:
        """
        Get the latest reading for a specific device.
        
        Args:
            device_id: Device identifier
            
        Returns:
            Latest device reading or None if not found
        """
        try:
            key = f"device:{device_id}:latest"
            data = self.redis_client.hgetall(key)
            return dict(data) if data else None
        except Exception as e:
            self.logger.error(f"Error getting latest reading for {device_id}: {e}")
            return None

    def get_device_history(self, device_id: str, 
                          start_time: Optional[float] = None, 
                          end_time: Optional[float] = None, 
                          limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get historical messages for a device.
        
        Args:
            device_id: Device identifier
            start_time: Start timestamp (Unix seconds)
            end_time: End timestamp (Unix seconds)
            limit: Maximum number of messages to return
            
        Returns:
            List of historical messages
        """
        try:
            if start_time:
                start_ms = int(start_time * 1000)
            else:
                start_ms = 0
                
            if end_time:
                end_ms = int(end_time * 1000)
            else:
                end_ms = int(time.time() * 1000)

            # Find archive keys in time range
            pattern = f"archive:{device_id}:*"
            keys = self.redis_client.keys(pattern)
            
            # Filter by timestamp
            filtered_keys = []
            for key in keys:
                timestamp_str = key.split(':')[-1]
                try:
                    timestamp_ms = int(timestamp_str)
                    if start_ms <= timestamp_ms <= end_ms:
                        filtered_keys.append((key, timestamp_ms))
                except ValueError:
                    continue
            
            # Sort by timestamp (newest first) and limit
            filtered_keys.sort(key=lambda x: x[1], reverse=True)
            filtered_keys = filtered_keys[:limit]
            
            # Retrieve messages
            messages = []
            for key, _ in filtered_keys:
                data = self.redis_client.get(key)
                if data:
                    try:
                        message = json.loads(data)
                        messages.append(message)
                    except json.JSONDecodeError as e:
                        self.logger.error(f"Error parsing message from {key}: {e}")
            
            return messages
            
        except Exception as e:
            self.logger.error(f"Error getting device history for {device_id}: {e}")
            return []

    def get_all_devices(self) -> List[str]:
        """
        Get all device IDs that have latest readings.
        
        Returns:
            List of device IDs
        """
        try:
            pattern = "device:*:latest"
            keys = self.redis_client.keys(pattern)
            
            # Extract device IDs from keys
            device_ids = []
            for key in keys:
                # key format: device:{device_id}:latest
                parts = key.split(':')
                if len(parts) >= 3:
                    device_id = parts[1]
                    device_ids.append(device_id)
            
            return device_ids
            
        except Exception as e:
            self.logger.error(f"Error getting all devices: {e}")
            return []

    def search_devices(self, query: str = None, has_errors: bool = None) -> List[Dict[str, Any]]:
        """
        Search devices with optional filters.
        
        Args:
            query: Search query for device names
            has_errors: Filter by error status
            
        Returns:
            List of matching devices with their latest readings
        """
        try:
            results = []
            device_ids = self.get_all_devices()
            
            for device_id in device_ids:
                # Apply query filter
                if query and query.lower() not in device_id.lower():
                    continue
                    
                # Get latest reading
                latest = self.get_latest_device_reading(device_id)
                if latest:
                    # Apply error filter
                    if has_errors is not None:
                        device_has_error = latest.get('has_error', 'false') == 'true'
                        if has_errors != device_has_error:
                            continue
                            
                    results.append({
                        'device_id': device_id,
                        'latest_reading': latest
                    })
            
            return results
            
        except Exception as e:
            self.logger.error(f"Error searching devices: {e}")
            return []

    def cleanup_old_archives(self, device_id: str = None, older_than_hours: int = 168) -> int:
        """
        Clean up old archive data.
        
        Args:
            device_id: Optional device filter
            older_than_hours: Remove data older than this many hours (default: 7 days)
            
        Returns:
            Number of keys cleaned up
        """
        try:
            cutoff_time = time.time() - (older_than_hours * 3600)
            cutoff_ms = int(cutoff_time * 1000)
            
            if device_id:
                pattern = f"archive:{device_id}:*"
            else:
                pattern = "archive:*"
                
            keys = self.redis_client.keys(pattern)
            keys_to_delete = []
            
            for key in keys:
                timestamp_str = key.split(':')[-1]
                try:
                    timestamp_ms = int(timestamp_str)
                    if timestamp_ms < cutoff_ms:
                        keys_to_delete.append(key)
                except ValueError:
                    continue
            
            if keys_to_delete:
                deleted_count = self.redis_client.delete(*keys_to_delete)
                self.logger.info(f"Cleaned up {deleted_count} old archive entries")
                return deleted_count
            
            return 0
            
        except Exception as e:
            self.logger.error(f"Error cleaning up old archives: {e}")
            return 0

    # Legacy Methods (for backward compatibility with existing code)
    def store_message(self, topic: str, message_data: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Store MQTT message in Redis (legacy format).

        Args:
            topic: MQTT topic
            message_data: Message data dictionary
            ttl: Time to live in seconds (optional)

        Returns:
            bool: True if stored successfully
        """
        try:
            timestamp = message_data.get('timestamp', time.time())
            key = self._format_key(f"{topic}:{int(timestamp * 1000000)}")

            # Convert all values to Redis-compatible types
            redis_safe_data = self._convert_data_for_redis(message_data)
            serialized_data = json.dumps(redis_safe_data)

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
        Store multiple messages in Redis using pipeline with type conversion.

        Args:
            messages: List of message data dictionaries
            ttl: Time to live in seconds (optional)

        Returns:
            int: Number of messages stored successfully
        """
        if not messages:
            return 0

        try:
            pipeline = self.pipeline()
            stored_count = 0

            for message_data in messages:
                try:
                    # Handle both legacy and new message formats
                    if 'device_id' in message_data:
                        # New simplified format
                        device_id = message_data['device_id']
                        timestamp = message_data.get('timestamp', time.time())
                        key = self._format_key(f"device:{device_id}:{int(timestamp * 1000000)}")
                    else:
                        # Legacy format
                        topic = message_data.get('topic', 'unknown')
                        timestamp = message_data.get('timestamp', time.time())
                        key = self._format_key(f"{topic}:{int(timestamp * 1000000)}")

                    # Convert all values to Redis-compatible types
                    redis_safe_data = self._convert_data_for_redis(message_data)
                    serialized_data = json.dumps(redis_safe_data)
                    
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
        Retrieve messages for a specific topic (legacy method).

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
        Get recent messages across all topics (legacy method).

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

    # Utility Methods
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

    def get_connection_info(self) -> Dict[str, Any]:
        """Get Redis connection information."""
        return {
            'host': self.config.get('host', 'localhost'),
            'port': self.config.get('port', 6379),
            'db': self.config.get('db', 0),
            'pool_max_connections': self.config.get('connection_pool', {}).get('max_connections', 10),
            'key_prefix': self.key_prefix,
            'boolean_conversion': self.boolean_conversion
        }

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
        """Get Redis connection information (property for backward compatibility)."""
        return self.get_connection_info()


# Backward compatibility alias
RedisClient = EnhancedRedisClient
