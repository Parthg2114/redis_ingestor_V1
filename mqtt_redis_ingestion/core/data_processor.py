import json
import time
import threading
import hashlib
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    DataProcessingError,
    DataValidationError,
    ConfigurationError
)


class EnhancedDataProcessor:

    def __init__(self, redis_client, batch_callback: Optional[Callable] = None):
        
        self.logger = get_logger(__name__)
        self.redis_client = redis_client
        self.config = config_manager.get_data_processing_config()
        if not self.config:
            raise ConfigurationError("Data processing configuration not found")

        # Configuration
        self.batch_size = self.config.get('batch_size', 100)
        self.flush_interval = self.config.get('flush_interval', 30)
        self.data_validation = self.config.get('data_validation', True)
        
        # Enhanced storage configuration
        self.storage_config = {
            'timeseries_retention': self.config.get('timeseries_retention', 604800000),  # 7 days in ms
            'archive_ttl': self.config.get('archive_ttl', 86400 * 30),  # 30 days
            'bloom_capacity': self.config.get('bloom_capacity', 1000000),
            'bloom_error_rate': self.config.get('bloom_error_rate', 0.01),
            'boolean_mode': self.config.get('boolean_mode', 'string')  # 'string' or 'integer'
        }

        # Batch processing
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        self.batch_callback = batch_callback

        # Enhanced processing stats
        self.stats = {
            'messages_processed': 0,
            'messages_validated': 0,
            'messages_stored': 0,
            'timeseries_points_stored': 0,
            'json_documents_stored': 0,
            'duplicates_filtered': 0,
            'validation_errors': 0,
            'processing_errors': 0,
            'batches_processed': 0,
            'devices_processed': set(),
            'metrics_tracked': set(),
            'boolean_conversions': 0
        }

        # Initialize Redis modules
        self._initialize_redis_modules()

        # Start flush timer
        self.stop_event = threading.Event()
        self.flush_thread = threading.Thread(target=self._flush_timer, daemon=True)
        self.flush_thread.start()
        self.logger.info("Enhanced data processor initialized with Redis modules")

    def _initialize_redis_modules(self):
        """Initialize Redis modules and create necessary structures."""
        try:
            # Initialize Bloom filter for deduplication
            try:
                self.redis_client.redis_client.execute_command(
                    'BF.RESERVE', 'msg_dedup_filter', 
                    self.storage_config['bloom_error_rate'], 
                    self.storage_config['bloom_capacity']
                )
                self.logger.info("Bloom filter initialized for deduplication")
            except Exception as e:
                if "item exists" not in str(e).lower():
                    self.logger.warning(f"Bloom filter initialization warning: {e}")

            # Create simplified search index for device documents
            try:
                self.redis_client.redis_client.execute_command(
                    'FT.CREATE', 'device_search_idx', 'ON', 'JSON',
                    'PREFIX', '1', 'device:',
                    'SCHEMA',
                    '$.device_info.id', 'AS', 'device_id', 'TEXT',
                    '$.latest_reading.status.active', 'AS', 'active', 'TAG',
                    '$.latest_reading.status.error', 'AS', 'has_error', 'TAG',
                    '$.latest_reading.timestamp', 'AS', 'last_update', 'NUMERIC'
                )
                self.logger.info("Simplified search index created for device documents")
            except Exception as e:
                if "index already exists" not in str(e).lower():
                    self.logger.warning(f"Search index creation warning: {e}")

        except Exception as e:
            self.logger.error(f"Error initializing Redis modules: {e}")

    def _convert_boolean_for_redis(self, value: Any) -> Any:
        
        if isinstance(value, bool):
            self.stats['boolean_conversions'] += 1
            if self.storage_config['boolean_mode'] == 'integer':
                return 1 if value else 0
            else:  # string mode
                return 'true' if value else 'false'
        elif isinstance(value, (dict, list)):
            return json.dumps(value)
        elif value is None:
            return 'null'
        elif isinstance(value, (str, int, float)):
            return value
        else:
            return str(value)

    def _convert_data_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert all values in a dictionary to Redis-compatible formats."""
        converted = {}
        for key, value in data.items():
            converted[key] = self._convert_boolean_for_redis(value)
        return converted

    def process_message(self, message_data: Dict[str, Any]) -> bool:
       
        try:
            self.stats['messages_processed'] += 1

            # Validate message
            if self.data_validation:
                if not self._validate_message(message_data):
                    self.stats['validation_errors'] += 1
                    return False
                self.stats['messages_validated'] += 1

            # Check for duplicates using Bloom filter
            if self._is_duplicate_message(message_data):
                self.stats['duplicates_filtered'] += 1
                return True

            # Prepare for enhanced storage
            processed_message = self._prepare_for_storage(message_data)

            # Track statistics
            device_id = processed_message.get('device_id', 'unknown')
            self.stats['devices_processed'].add(device_id)
            
            # Track metrics
            for metric in processed_message.get('numerical_metrics', {}):
                self.stats['metrics_tracked'].add(f"{device_id}:{metric}")

            # Add to batch buffer
            with self.buffer_lock:
                self.message_buffer.append(processed_message)
                if len(self.message_buffer) >= self.batch_size:
                    self._flush_buffer()

            return True

        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.stats['processing_errors'] += 1
            raise DataProcessingError(f"Message processing failed: {e}")

    def _validate_message(self, message_data: Dict[str, Any]) -> bool:
        """Enhanced message validation."""
        try:
            # Handle different message formats
            if 'deviceDisplayName' in message_data:
                device_id = message_data['deviceDisplayName']
                sensor_data = message_data.get('data', {})
                timestamp_raw = message_data.get('timestamp')
            elif 'device_id' in message_data:
                device_id = message_data['device_id']
                sensor_data = message_data.get('sensor_data', {})
                timestamp_raw = message_data.get('timestamp')
            else:
                device_id = message_data.get('topic', 'unknown_device').replace('/', '_')
                timestamp_raw = message_data.get('timestamp')
                sensor_data = {k: v for k, v in message_data.items() 
                             if k not in ['timestamp', 'topic', 'payload']}

            # Validate device_id
            if not isinstance(device_id, str) or not device_id.strip():
                self.logger.warning("Invalid device identifier")
                return False

            # Validate timestamp
            if isinstance(timestamp_raw, str):
                try:
                    if timestamp_raw.endswith('Z'):
                        timestamp_raw = timestamp_raw[:-1] + '+00:00'
                    dt = datetime.fromisoformat(timestamp_raw)
                    timestamp = dt.timestamp()
                except:
                    self.logger.warning("Invalid timestamp format")
                    return False
            elif not isinstance(timestamp_raw, (int, float)) or timestamp_raw <= 0:
                self.logger.warning("Invalid timestamp")
                return False
            else:
                timestamp = float(timestamp_raw)

            # Check timestamp reasonableness
            current_time = time.time()
            if abs(current_time - timestamp) > 86400:
                self.logger.warning("Timestamp outside acceptable range")
                return False

            # Validate sensor data
            if not isinstance(sensor_data, dict):
                self.logger.warning("Sensor data must be a dictionary")
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error during message validation: {e}")
            return False

    def _is_duplicate_message(self, message_data: Dict[str, Any]) -> bool:
        """Check for message duplicates using Bloom filter."""
        try:
            # Create message hash
            device_id = (message_data.get('deviceDisplayName') or 
                        message_data.get('device_id') or 
                        message_data.get('topic', 'unknown'))
            
            timestamp = message_data.get('timestamp', time.time())
            sensor_data = (message_data.get('data') or 
                          message_data.get('sensor_data') or {})
            
            message_signature = f"{device_id}:{timestamp}:{json.dumps(sensor_data, sort_keys=True)}"
            message_hash = hashlib.md5(message_signature.encode()).hexdigest()

            # Check if message exists in bloom filter
            exists = self.redis_client.redis_client.execute_command(
                'BF.EXISTS', 'msg_dedup_filter', message_hash
            )

            if not exists:
                # Add to bloom filter
                self.redis_client.redis_client.execute_command(
                    'BF.ADD', 'msg_dedup_filter', message_hash
                )
                return False

            return True

        except Exception as e:
            self.logger.error(f"Error checking duplicates: {e}")
            return False

    def _prepare_for_storage(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare message for enhanced Redis storage."""
        try:
            # Extract device information
            if 'deviceDisplayName' in message_data:
                device_id = message_data['deviceDisplayName']
                sensor_data = message_data.get('data', {})
                timestamp_raw = message_data.get('timestamp')
                has_error = message_data.get('error', False)
                error_desc = message_data.get('errorDescription', '')
            elif 'device_id' in message_data:
                device_id = message_data['device_id']
                sensor_data = message_data.get('sensor_data', {})
                timestamp_raw = message_data.get('timestamp')
                metadata = message_data.get('metadata', {})
                has_error = metadata.get('has_error', False)
                error_desc = metadata.get('error_description', '')
            else:
                device_id = message_data.get('topic', 'unknown_device').replace('/', '_')
                timestamp_raw = message_data.get('timestamp')
                sensor_data = {k: v for k, v in message_data.items() 
                             if k not in ['timestamp', 'topic', 'payload']}
                has_error = False
                error_desc = ''

            # Normalize timestamp
            if isinstance(timestamp_raw, str):
                try:
                    if timestamp_raw.endswith('Z'):
                        timestamp_raw = timestamp_raw[:-1] + '+00:00'
                    dt = datetime.fromisoformat(timestamp_raw)
                    timestamp = dt.timestamp()
                    datetime_str = timestamp_raw
                except:
                    timestamp = time.time()
                    datetime_str = datetime.now().isoformat()
            else:
                timestamp = float(timestamp_raw) if timestamp_raw else time.time()
                datetime_str = datetime.fromtimestamp(timestamp).isoformat()

            # Clean device ID
            device_id_clean = self._clean_device_id(device_id)

            # Separate numerical and categorical data
            numerical_metrics = {}
            categorical_metrics = {}
            
            for key, value in sensor_data.items():
                if isinstance(value, (int, float)):
                    numerical_metrics[key] = value
                else:
                    categorical_metrics[key] = value

            # Create enhanced storage structure
            storage_message = {
                'device_id': device_id_clean,
                'original_device_name': device_id,
                'timestamp': timestamp,
                'datetime': datetime_str,
                'has_error': has_error,
                'error_description': error_desc,
                'sensor_data': sensor_data,
                'numerical_metrics': numerical_metrics,
                'categorical_metrics': categorical_metrics,
                'processed_at': time.time()
            }

            return storage_message

        except Exception as e:
            self.logger.error(f"Error preparing message for storage: {e}")
            return message_data

    def _clean_device_id(self, device_id: str) -> str:
        """Clean device ID for use in Redis keys."""
        return device_id.lower().replace(' ', '_').replace('-', '_')

    def _flush_buffer(self):
        """Flush message buffer using enhanced storage strategy."""
        if not self.message_buffer:
            return

        try:
            messages_to_store = self.message_buffer.copy()
            self.message_buffer.clear()

            # Store using enhanced strategy
            stored_count = self._store_enhanced_batch(messages_to_store)

            self.stats['messages_stored'] += stored_count
            self.stats['batches_processed'] += 1
            self.last_flush_time = time.time()

            if self.batch_callback:
                self.batch_callback(messages_to_store, stored_count)

            self.logger.info(f"Enhanced batch stored: {stored_count}/{len(messages_to_store)} messages")

        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}")
            with self.buffer_lock:
                self.message_buffer.extend(messages_to_store)

    def _store_enhanced_batch(self, messages: List[Dict[str, Any]]) -> int:
        """Store batch using enhanced Redis modules with proper boolean conversion."""
        stored_count = 0

        try:
            pipe = self.redis_client.pipeline()

            for msg in messages:
                try:
                    device_id = msg['device_id']
                    timestamp = msg['timestamp']
                    timestamp_ms = int(timestamp * 1000)

                    # 1. Store numerical metrics in TimeSeries with boolean conversion
                    for metric, value in msg.get('numerical_metrics', {}).items():
                        # Convert boolean to Redis-safe format
                        safe_value = self._convert_boolean_for_redis(value)
                        if isinstance(safe_value, (int, float)):
                            ts_key = f"ts:{metric}:{device_id}"
                            pipe.execute_command(
                                'TS.ADD', ts_key, timestamp_ms, safe_value,
                                'RETENTION', self.storage_config['timeseries_retention'],
                                'LABELS', 'device_id', device_id, 'metric', metric
                            )
                            self.stats['timeseries_points_stored'] += 1

                    # 2. Store simplified device document in JSON
                    device_doc = {
                        'device_info': {
                            'id': device_id,
                            'original_name': msg.get('original_device_name', device_id)
                        },
                        'latest_reading': {
                            'timestamp': timestamp,
                            'datetime': msg['datetime'],
                            'metrics': self._convert_data_for_redis(msg.get('sensor_data', {})),
                            'status': {
                                'active': self._convert_boolean_for_redis(True),
                                'error': self._convert_boolean_for_redis(msg.get('has_error', False)),
                                'error_description': msg.get('error_description', '')
                            }
                        }
                    }

                    device_doc_key = f"device:{device_id}:doc"
                    pipe.execute_command(
                        'JSON.SET', device_doc_key, '$', json.dumps(device_doc)
                    )
                    self.stats['json_documents_stored'] += 1

                    # 3. Store complete message in archive with boolean conversion
                    archive_key = f"archive:{device_id}:{timestamp_ms}"
                    safe_message = self._convert_data_for_redis(msg)
                    pipe.execute_command(
                        'JSON.SET', archive_key, '$', json.dumps(safe_message)
                    )
                    pipe.expire(archive_key, self.storage_config['archive_ttl'])

                    stored_count += 1

                except Exception as e:
                    self.logger.error(f"Error preparing message for enhanced storage: {e}")

            # Execute all operations
            pipe.execute()

        except Exception as e:
            self.logger.error(f"Error storing enhanced batch: {e}")
            raise

        return stored_count

    def _flush_timer(self):
        """Background thread for periodic buffer flushing."""
        while not self.stop_event.is_set():
            try:
                time.sleep(1)
                if time.time() - self.last_flush_time >= self.flush_interval:
                    with self.buffer_lock:
                        if self.message_buffer:
                            self._flush_buffer()
            except Exception as e:
                self.logger.error(f"Error in flush timer: {e}")

    def force_flush(self):
        """Force flush of message buffer."""
        with self.buffer_lock:
            if self.message_buffer:
                self._flush_buffer()

    def get_stats(self) -> Dict[str, Any]:
        """Get enhanced processing statistics."""
        stats = self.stats.copy()
        stats['buffer_size'] = len(self.message_buffer)
        stats['time_since_last_flush'] = time.time() - self.last_flush_time
        stats['devices_processed'] = list(stats['devices_processed'])
        stats['metrics_tracked'] = list(stats['metrics_tracked'])
        return stats

    def shutdown(self):
        """Shutdown the enhanced data processor."""
        self.logger.info("Shutting down enhanced data processor...")
        self.stop_event.set()
        self.force_flush()
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        self.logger.info("Enhanced data processor shutdown complete")

    @property
    def stop(self) -> threading.Event:
        """Get the stop event for backward compatibility."""
        return self.stop_event

    @stop.setter
    def stop(self, value):
        """Set the stop event for backward compatibility."""
        if value:
            self.stop_event.set()
        else:
            self.stop_event.clear()

    @property
    def buffer_size(self) -> int:
        return len(self.message_buffer)
