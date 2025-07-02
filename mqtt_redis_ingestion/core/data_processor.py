"""
Simplified Data Processor Module - Archive and Latest Only

Handles sensor data storage using only latest readings and message archive.
No category-based organization.
"""

import json
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    DataProcessingError,
    DataValidationError,
    ConfigurationError
)


class SimplifiedDataProcessor:
    """
    Simplified data processor with archive and latest storage only.
    """

    def __init__(self, redis_client, batch_callback: Optional[Callable] = None):
        """
        Initialize simplified data processor.

        Args:
            redis_client: Redis client instance for storage
            batch_callback: Optional callback for batch processing
        """
        self.logger = get_logger(__name__)
        self.redis_client = redis_client
        self.config = config_manager.get_data_processing_config()
        if not self.config:
            raise ConfigurationError("Data processing configuration not found")

        # Configuration
        self.batch_size = self.config.get('batch_size', 100)
        self.flush_interval = self.config.get('flush_interval', 30)
        self.data_validation = self.config.get('data_validation', True)

        # Storage configuration
        self.storage_config = {
            'latest_readings_ttl': self.config.get('latest_readings_ttl', 3600),  # 1 hour
            'message_archive_ttl': self.config.get('message_archive_ttl', 86400 * 7),  # 7 days
        }

        # Batch processing
        self.message_buffer = []
        self.buffer_lock = threading.Lock()
        self.last_flush_time = time.time()
        self.batch_callback = batch_callback

        # Processing stats
        self.stats = {
            'messages_processed': 0,
            'messages_validated': 0,
            'messages_stored': 0,
            'validation_errors': 0,
            'processing_errors': 0,
            'batches_processed': 0,
            'devices_processed': set()
        }

        # Start flush timer
        self.stop_event = threading.Event()
        self.flush_thread = threading.Thread(target=self._flush_timer, daemon=True)
        self.flush_thread.start()
        self.logger.info("Simplified data processor initialized (archive + latest only)")

    def process_message(self, message_data: Dict[str, Any]) -> bool:
        """
        Process a sensor message with simplified storage.

        Args:
            message_data: Message data (can be from simulator or real sensors)

        Returns:
            bool: True if processed successfully
        """
        try:
            self.stats['messages_processed'] += 1

            # Validate message if validation is enabled
            if self.data_validation:
                if not self._validate_message(message_data):
                    self.stats['validation_errors'] += 1
                    return False
                self.stats['messages_validated'] += 1

            # Prepare for simplified storage
            processed_message = self._prepare_for_storage(message_data)

            # Track device statistics
            device_id = processed_message.get('device_id', 'unknown')
            self.stats['devices_processed'].add(device_id)

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
        """Validate sensor message data."""
        try:
            # Handle both simulator and real sensor formats
            if 'deviceDisplayName' in message_data:
                # Industrial simulator format
                device_id = message_data['deviceDisplayName']
                sensor_data = message_data.get('data', {})
                timestamp_raw = message_data.get('timestamp')
            elif 'device_id' in message_data:
                # Universal format
                device_id = message_data['device_id']
                sensor_data = message_data.get('sensor_data', {})
                timestamp_raw = message_data.get('timestamp')
            else:
                # Fallback - extract from topic or use default
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

            # Check timestamp reasonableness (within 24 hours)
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

    def _prepare_for_storage(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """Prepare message for simplified Redis storage."""
        try:
            # Extract device information based on format
            if 'deviceDisplayName' in message_data:
                # Industrial simulator format
                device_id = message_data['deviceDisplayName']
                sensor_data = message_data.get('data', {})
                timestamp_raw = message_data.get('timestamp')
                has_error = message_data.get('error', False)
                error_desc = message_data.get('errorDescription', '')
                source_format = 'industrial_simulator'
            elif 'device_id' in message_data:
                # Universal format
                device_id = message_data['device_id']
                sensor_data = message_data.get('sensor_data', {})
                timestamp_raw = message_data.get('timestamp')
                metadata = message_data.get('metadata', {})
                has_error = metadata.get('has_error', False)
                error_desc = metadata.get('error_description', '')
                source_format = metadata.get('source_format', 'universal')
            else:
                # Fallback format
                device_id = message_data.get('topic', 'unknown_device').replace('/', '_')
                timestamp_raw = message_data.get('timestamp')
                sensor_data = {k: v for k, v in message_data.items() 
                             if k not in ['timestamp', 'topic', 'payload']}
                has_error = False
                error_desc = ''
                source_format = 'fallback'

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

            # Clean device ID for Redis keys
            device_id_clean = self._clean_device_id(device_id)

            # Convert sensor data to Redis-safe formats
            redis_safe_data = self._convert_for_redis(sensor_data)

            # Create simplified storage structure
            storage_message = {
                'device_id': device_id_clean,
                'original_device_name': device_id,
                'timestamp': timestamp,
                'datetime': datetime_str,
                'source_format': source_format,
                'has_error': has_error,
                'error_description': error_desc,
                'sensor_data': redis_safe_data,
                'processed_at': time.time(),
                'processor_version': config_manager.get('application.version', '1.0.0')
            }

            return storage_message

        except Exception as e:
            self.logger.error(f"Error preparing message for storage: {e}")
            return message_data

    def _convert_for_redis(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert data types to Redis-compatible formats."""
        converted = {}
        
        for key, value in data.items():
            if isinstance(value, bool):
                converted[key] = 'true' if value else 'false'
            elif isinstance(value, (dict, list)):
                converted[key] = json.dumps(value)
            elif value is None:
                converted[key] = 'null'
            elif isinstance(value, (str, int, float)):
                converted[key] = str(value)
            else:
                converted[key] = str(value)
                
        return converted

    def _clean_device_id(self, device_id: str) -> str:
        """Clean device ID for use in Redis keys."""
        return device_id.lower().replace(' ', '_').replace('-', '_')

    def _flush_buffer(self):
        """Flush message buffer to Redis with simplified storage."""
        if not self.message_buffer:
            return

        try:
            messages_to_store = self.message_buffer.copy()
            self.message_buffer.clear()

            # Store using simplified strategy
            stored_count = self._store_simplified_batch(messages_to_store)

            self.stats['messages_stored'] += stored_count
            self.stats['batches_processed'] += 1
            self.last_flush_time = time.time()

            if self.batch_callback:
                self.batch_callback(messages_to_store, stored_count)

            self.logger.info(f"Stored {stored_count}/{len(messages_to_store)} messages (archive + latest)")

        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}")
            with self.buffer_lock:
                self.message_buffer.extend(messages_to_store)

    def _store_simplified_batch(self, messages: List[Dict[str, Any]]) -> int:
        """Store batch using simplified Redis strategy (archive + latest only)."""
        stored_count = 0

        try:
            pipe = self.redis_client.pipeline()

            for msg in messages:
                try:
                    device_id = msg['device_id']
                    timestamp = msg['timestamp']
                    timestamp_ms = int(timestamp * 1000)

                    # 1. Store latest device readings (hash)
                    latest_key = f"device:{device_id}:latest"
                    latest_data = {
                        'timestamp': str(timestamp),
                        'datetime': msg['datetime'],
                        'original_device_name': msg.get('original_device_name', device_id),
                        'source_format': msg['source_format'],
                        'has_error': str(msg['has_error']),
                        'processed_at': str(msg['processed_at']),
                        **msg['sensor_data']
                    }
                    pipe.hset(latest_key, mapping=latest_data)
                    if self.storage_config['latest_readings_ttl']:
                        pipe.expire(latest_key, self.storage_config['latest_readings_ttl'])

                    # 2. Store message archive (complete message)
                    archive_key = f"archive:{device_id}:{timestamp_ms}"
                    archive_data = json.dumps(msg)
                    pipe.set(archive_key, archive_data)
                    if self.storage_config['message_archive_ttl']:
                        pipe.expire(archive_key, self.storage_config['message_archive_ttl'])

                    stored_count += 1

                except Exception as e:
                    self.logger.error(f"Error preparing message for storage: {e}")

            # Execute all operations
            pipe.execute()

        except Exception as e:
            self.logger.error(f"Error storing simplified batch: {e}")
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
        """Get processing statistics."""
        stats = self.stats.copy()
        stats['buffer_size'] = len(self.message_buffer)
        stats['time_since_last_flush'] = time.time() - self.last_flush_time
        stats['devices_processed'] = list(stats['devices_processed'])
        return stats

    def shutdown(self):
        """Shutdown the data processor."""
        self.logger.info("Shutting down simplified data processor...")
        self.stop
    # Add these methods to the SimplifiedDataProcessor class

    @property
    def stop(self) -> threading.Event:
        """
    Get the stop event for backward compatibility.
    
    Returns:
        threading.Event: The stop event
        """
        return self.stop_event

    @stop.setter
    def stop(self, value):
        """
    Set the stop event for backward compatibility.
    
    Args:
        value: Boolean value to set or clear the event
    """
        if value:
            self.stop_event.set()
        else:
            self.stop_event.clear()

    def is_stopped(self) -> bool:
        """
    Check if the processor is stopped.
    
    Returns:
        bool: True if stopped, False otherwise
        """
        return self.stop_event.is_set()
