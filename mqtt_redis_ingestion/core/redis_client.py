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

    def __init__(self):
        """Initialize Redis client with connection pool and module support."""
        self.logger = get_logger(__name__)
        self.config = config_manager.get_redis_config()
        if not self.config:
            raise ConfigurationError("Redis configuration not found")

        self.connection_pool = None
        self.redis_client = None
        self.key_prefix = self.config.get('key_prefix', 'sensor_data:')
        self.default_ttl = self.config.get('ttl', 3600)
        
        self.boolean_conversion = self.config.get('boolean_conversion', 'string')

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
                decode_responses=True
            )
            self.logger.info("Enhanced Redis connection pool configured")

        except Exception as e:
            self.logger.error(f"Failed to setup Redis connection pool: {e}")
            raise CustomRedisConnectionError(f"Connection pool setup failed: {e}")

    def _setup_client(self):
 
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
        """Get Redis pipeline for batch operations."""
        try:
            return self.redis_client.pipeline(transaction=transaction)
        except Exception as e:
            self.logger.error(f"Error creating Redis pipeline: {e}")
            raise RedisOperationError(f"Failed to create pipeline: {e}")

    def execute_pipeline(self, pipeline):
        """Execute a Redis pipeline."""
        try:
            return pipeline.execute()
        except Exception as e:
            self.logger.error(f"Error executing Redis pipeline: {e}")
            raise RedisOperationError(f"Failed to execute pipeline: {e}")

    def _convert_value_for_redis(self, value: Any) -> Union[str, int, float, bytes]:
        """Convert value to Redis-compatible type."""
        if isinstance(value, bool):
            if self.boolean_conversion == 'integer':
                return 1 if value else 0
            else: 
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

        converted = {}
        for key, value in data.items():
            converted[key] = self._convert_value_for_redis(value)
        return converted

    def get_timeseries_data(self, device_id: str, metric: str, 
                           start_time: Optional[float] = None, 
                           end_time: Optional[float] = None,
                           aggregation: str = None,
                           bucket_size: int = 60000) -> List[Dict[str, Any]]:
        """Get time series data for a specific device and metric."""
        try:
            ts_key = f"ts:{metric}:{device_id}"
            
            # Convert timestamps to milliseconds
            start_ms = int(start_time * 1000) if start_time else '-'
            end_ms = int(end_time * 1000) if end_time else '+'
            
            # Build TS.RANGE command
            if aggregation:
                result = self.redis_client.execute_command(
                    'TS.RANGE', ts_key, start_ms, end_ms,
                    'AGGREGATION', aggregation, bucket_size
                )
            else:
                result = self.redis_client.execute_command(
                    'TS.RANGE', ts_key, start_ms, end_ms
                )
            
            # Format results
            data_points = []
            for timestamp_ms, value in result:
                data_points.append({
                    'timestamp': timestamp_ms / 1000,
                    'datetime': datetime.fromtimestamp(timestamp_ms / 1000).isoformat(),
                    'value': float(value)
                })
            
            return data_points
            
        except Exception as e:
            self.logger.error(f"Error getting timeseries data: {e}")
            return []

    def get_multi_timeseries_data(self, filters: Dict[str, str] = None,
                                 start_time: Optional[float] = None,
                                 end_time: Optional[float] = None,
                                 aggregation: str = None,
                                 bucket_size: int = 60000) -> Dict[str, List[Dict[str, Any]]]:
        """Get time series data for multiple metrics using filters."""
        try:
            start_ms = int(start_time * 1000) if start_time else '-'
            end_ms = int(end_time * 1000) if end_time else '+'
            
            # Build filter string
            filter_parts = []
            if filters:
                for key, value in filters.items():
                    filter_parts.append(f"{key}={value}")
            
            # Build TS.MRANGE command
            cmd = ['TS.MRANGE', start_ms, end_ms]
            if aggregation:
                cmd.extend(['AGGREGATION', aggregation, bucket_size])
            if filter_parts:
                cmd.extend(['FILTER'] + filter_parts)
            
            result = self.redis_client.execute_command(*cmd)
            
            # Format results
            series_data = {}
            for series_info in result:
                series_name = series_info[0]
                labels = dict(series_info[1])
                data_points = []
                
                for timestamp_ms, value in series_info[2]:
                    data_points.append({
                        'timestamp': timestamp_ms / 1000,
                        'datetime': datetime.fromtimestamp(timestamp_ms / 1000).isoformat(),
                        'value': float(value)
                    })
                
                series_data[series_name] = {
                    'labels': labels,
                    'data': data_points
                }
            
            return series_data
            
        except Exception as e:
            self.logger.error(f"Error getting multi timeseries data: {e}")
            return {}

    # JSON Query Methods
    def get_device_document(self, device_id: str) -> Optional[Dict[str, Any]]:
        """Get complete device document from JSON storage."""
        try:
            doc_key = f"device:{device_id}:doc"
            result = self.redis_client.execute_command('JSON.GET', doc_key)
            
            if result:
                return json.loads(result)
            return None
            
        except Exception as e:
            self.logger.error(f"Error getting device document: {e}")
            return None

    def search_devices(self, query: str = "*", 
                      filters: Dict[str, str] = None,
                      limit: int = 100) -> List[Dict[str, Any]]:
        """Search device documents using RedisSearch."""
        try:
            # Build search query
            search_parts = [query]
            
            if filters:
                for field, value in filters.items():
                    search_parts.append(f"@{field}:{value}")
            
            search_query = " ".join(search_parts)
            
            # Execute search
            result = self.redis_client.execute_command(
                'FT.SEARCH', 'device_search_idx', search_query,
                'LIMIT', '0', str(limit)
            )
            
            # Parse results
            devices = []
            if len(result) > 1:
                for i in range(1, len(result), 2):
                    doc_key = result[i]
                    doc_fields = result[i + 1]
                    
                    # Get full JSON document
                    doc_data = self.redis_client.execute_command('JSON.GET', doc_key)
                    if doc_data:
                        devices.append(json.loads(doc_data))
            
            return devices
            
        except Exception as e:
            self.logger.error(f"Error searching devices: {e}")
            return []

    def get_device_analytics(self, device_id: str = None) -> Dict[str, Any]:
        """Get analytics and aggregations for devices."""
        try:
            analytics = {}
            
            # Build base query
            query = "*"
            if device_id:
                query = f"@device_id:{device_id}"
            
            # Get error summary
            try:
                error_agg = self.redis_client.execute_command(
                    'FT.AGGREGATE', 'device_search_idx', query,
                    'GROUPBY', '1', '@has_error',
                    'REDUCE', 'COUNT', '0', 'AS', 'count'
                )
                
                analytics['error_summary'] = {}
                if len(error_agg) > 1:
                    for i in range(1, len(error_agg)):
                        row = error_agg[i]
                        if len(row) >= 4:
                            analytics['error_summary'][row[1]] = int(row[3])
            except Exception as e:
                self.logger.warning(f"Error getting error aggregation: {e}")
            
            # Get active device count
            try:
                active_agg = self.redis_client.execute_command(
                    'FT.AGGREGATE', 'device_search_idx', query,
                    'GROUPBY', '1', '@active',
                    'REDUCE', 'COUNT', '0', 'AS', 'count'
                )
                
                analytics['active_summary'] = {}
                if len(active_agg) > 1:
                    for i in range(1, len(active_agg)):
                        row = active_agg[i]
                        if len(row) >= 4:
                            analytics['active_summary'][row[1]] = int(row[3])
            except Exception as e:
                self.logger.warning(f"Error getting active aggregation: {e}")
            
            return analytics
            
        except Exception as e:
            self.logger.error(f"Error getting device analytics: {e}")
            return {}

    # Archive Query Methods
    def get_device_archive(self, device_id: str,
                          start_time: Optional[float] = None,
                          end_time: Optional[float] = None,
                          limit: int = 100) -> List[Dict[str, Any]]:
        """Get archived messages for a device."""
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
            
            # Retrieve messages using JSON.GET
            messages = []
            for key, _ in filtered_keys:
                try:
                    data = self.redis_client.execute_command('JSON.GET', key)
                    if data:
                        messages.append(json.loads(data))
                except Exception as e:
                    self.logger.error(f"Error parsing archive message from {key}: {e}")
            
            return messages
            
        except Exception as e:
            self.logger.error(f"Error getting device archive: {e}")
            return []

    # Utility Methods
    def get_all_devices(self) -> List[str]:
        """Get all device IDs."""
        try:
            result = self.redis_client.execute_command(
                'FT.SEARCH', 'device_search_idx', '*',
                'RETURN', '1', '$.device_info.id'
            )
            
            devices = []
            if len(result) > 1:
                for i in range(1, len(result), 2):
                    doc_fields = result[i + 1]
                    if doc_fields and len(doc_fields) >= 2:
                        device_id = doc_fields[1]
                        devices.append(device_id)
            
            return devices
            
        except Exception as e:
            self.logger.error(f"Error getting all devices: {e}")
            return []

    def get_device_metrics(self, device_id: str) -> List[str]:
        """Get all available metrics for a device."""
        try:
            pattern = f"ts:*:{device_id}"
            keys = self.redis_client.keys(pattern)
            
            metrics = []
            for key in keys:
                # Extract metric from key: ts:{metric}:{device_id}
                parts = key.split(':')
                if len(parts) >= 3:
                    metric = parts[1]
                    metrics.append(metric)
            
            return metrics
            
        except Exception as e:
            self.logger.error(f"Error getting device metrics: {e}")
            return []

    def health_check(self) -> bool:
        """Perform health check on Redis connection."""
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

    # Legacy methods for backward compatibility
    def store_messages_batch(self, messages: List[Dict[str, Any]], ttl: Optional[int] = None) -> int:
        """Store multiple messages in Redis using pipeline (legacy support)."""
        if not messages:
            return 0

        try:
            pipeline = self.pipeline()
            stored_count = 0

            for message_data in messages:
                try:
                    device_id = message_data.get('device_id', 'unknown')
                    timestamp = message_data.get('timestamp', time.time())
                    key = f"legacy:{device_id}:{int(timestamp * 1000000)}"

                    # Convert to Redis-safe format
                    safe_data = self._convert_data_for_redis(message_data)
                    pipeline.set(key, json.dumps(safe_data), ex=ttl or self.default_ttl)
                    stored_count += 1

                except Exception as e:
                    self.logger.error(f"Error preparing message for batch storage: {e}")

            # Execute pipeline
            results = pipeline.execute()
            successful_stores = sum(1 for result in results if result)

            self.logger.info(f"Legacy batch stored {successful_stores}/{len(messages)} messages")
            return successful_stores

        except Exception as e:
            self.logger.error(f"Error in legacy batch store operation: {e}")
            return 0

    @property
    def connection_info(self) -> Dict[str, Any]:
        """Get Redis connection information (property for backward compatibility)."""
        return self.get_connection_info()


