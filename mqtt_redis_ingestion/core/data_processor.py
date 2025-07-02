"""
Data Processor Module

This module handles processing, validation, and transformation of MQTT messages
before they are stored in Redis. It includes batch processing capabilities
and data validation.
"""

import json
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from queue import Queue, Empty
from datetime import datetime

from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    DataProcessingError,
    DataValidationError,
    ConfigurationError
)


class DataProcessor:
    """
    Processes MQTT messages with validation and batch capabilities.
    """
    
    def __init__(self, redis_client, batch_callback: Optional[Callable] = None):
        """
        Initialize data processor.
        
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
            'batches_processed': 0
        }
        
        # Start flush timer
        self.stop_event = threading.Event()
        self.flush_thread = threading.Thread(target=self._flush_timer, daemon=True)
        self.flush_thread.start()
        
        self.logger.info("Data processor initialized")
    
    def process_message(self, message_data: Dict[str, Any]) -> bool:
        """
        Process a single MQTT message.
        
        Args:
            message_data: Raw message data from MQTT client
            
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
            
            # Transform message
            processed_message = self._transform_message(message_data)
            
            # Add to batch buffer
            with self.buffer_lock:
                self.message_buffer.append(processed_message)
                
                # Check if batch is full
                if len(self.message_buffer) >= self.batch_size:
                    self._flush_buffer()
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing message: {e}")
            self.stats['processing_errors'] += 1
            raise DataProcessingError(f"Message processing failed: {e}")
    
    def process_messages_batch(self, messages: List[Dict[str, Any]]) -> int:
        """
        Process multiple messages in batch.
        
        Args:
            messages: List of raw message data
            
        Returns:
            int: Number of messages processed successfully
        """
        processed_count = 0
        
        for message_data in messages:
            try:
                if self.process_message(message_data):
                    processed_count += 1
            except Exception as e:
                self.logger.error(f"Error processing message in batch: {e}")
        
        return processed_count
    
    def _validate_message(self, message_data: Dict[str, Any]) -> bool:
        """
        Validate MQTT message data.
        
        Args:
            message_data: Message data to validate
            
        Returns:
            bool: True if valid, False otherwise
        """
        try:
            # Check required fields
            required_fields = ['topic', 'payload', 'timestamp']
            for field in required_fields:
                if field not in message_data or message_data[field] is None:
                    self.logger.warning(f"Missing required field: {field}")
                    return False
            
            # Validate topic
            topic = message_data['topic']
            if not isinstance(topic, str) or len(topic.strip()) == 0:
                self.logger.warning("Invalid topic format")
                return False
            
            # Validate timestamp
            timestamp = message_data['timestamp']
            if not isinstance(timestamp, (int, float)) or timestamp <= 0:
                self.logger.warning("Invalid timestamp")
                return False
            
            # Check if timestamp is reasonable (not too old or in future)
            current_time = time.time()
            if abs(current_time - timestamp) > 3600:  # 1 hour tolerance
                self.logger.warning(f"Timestamp outside acceptable range: {timestamp}")
                return False
            
            # Validate payload
            payload = message_data['payload']
            if not isinstance(payload, str):
                self.logger.warning("Payload must be string")
                return False
            
            # Try to parse payload as JSON if it looks like JSON
            if payload.strip().startswith('{') or payload.strip().startswith('['):
                try:
                    json.loads(payload)
                except json.JSONDecodeError:
                    self.logger.warning("Invalid JSON in payload")
                    return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error during message validation: {e}")
            return False
    
    def _transform_message(self, message_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform and enrich message data.
        
        Args:
            message_data: Raw message data
            
        Returns:
            Transformed message data
        """
        try:
            # Create transformed message
            transformed = message_data.copy()
            
            # Add processing metadata
            transformed['processed_at'] = time.time()
            transformed['processor_version'] = config_manager.get('application.version', '1.0.0')
            
            # Parse payload if it's JSON
            payload = transformed['payload']
            if isinstance(payload, str) and (payload.strip().startswith('{') or payload.strip().startswith('[')):
                try:
                    transformed['parsed_payload'] = json.loads(payload)
                except json.JSONDecodeError:
                    # Keep original payload if parsing fails
                    pass
            
            # Add topic hierarchy
            topic_parts = transformed['topic'].split('/')
            transformed['topic_hierarchy'] = topic_parts
            
            # Add datetime string for human readability
            dt = datetime.fromtimestamp(transformed['timestamp'])
            transformed['datetime'] = dt.isoformat()
            
            # Extract sensor type if topic follows pattern sensors/type/id
            if len(topic_parts) >= 2 and topic_parts[0] == 'sensors':
                transformed['sensor_type'] = topic_parts[1]
                if len(topic_parts) >= 3:
                    transformed['sensor_id'] = topic_parts[2]
            
            return transformed
            
        except Exception as e:
            self.logger.error(f"Error transforming message: {e}")
            # Return original message if transformation fails
            return message_data
    
    def _flush_buffer(self):
        """Flush message buffer to Redis."""
        if not self.message_buffer:
            return
        
        try:
            messages_to_store = self.message_buffer.copy()
            self.message_buffer.clear()
            
            # Store messages in Redis
            stored_count = self.redis_client.store_messages_batch(messages_to_store)
            
            self.stats['messages_stored'] += stored_count
            self.stats['batches_processed'] += 1
            self.last_flush_time = time.time()
            
            # Call batch callback if provided
            if self.batch_callback:
                self.batch_callback(messages_to_store, stored_count)
            
            self.logger.info(f"Flushed batch: {stored_count}/{len(messages_to_store)} messages stored")
            
        except Exception as e:
            self.logger.error(f"Error flushing buffer: {e}")
            # Re-add messages to buffer for retry
            with self.buffer_lock:
                self.message_buffer.extend(messages_to_store)
    
    def _flush_timer(self):
        """Background thread to flush buffer at regular intervals."""
        while not self.stop_event.is_set():
            try:
                time.sleep(1)  # Check every second
                
                current_time = time.time()
                time_since_flush = current_time - self.last_flush_time
                
                if time_since_flush >= self.flush_interval:
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
        """
        Get processing statistics.
        
        Returns:
            Dictionary with processing statistics
        """
        stats = self.stats.copy()
        stats['buffer_size'] = len(self.message_buffer)
        stats['time_since_last_flush'] = time.time() - self.last_flush_time
        return stats
    
    def reset_stats(self):
        """Reset processing statistics."""
        self.stats = {
            'messages_processed': 0,
            'messages_validated': 0,
            'messages_stored': 0,
            'validation_errors': 0,
            'processing_errors': 0,
            'batches_processed': 0
        }
        self.logger.info("Processing statistics reset")
    
    def shutdown(self):
        """Shutdown the data processor."""
        self.logger.info("Shutting down data processor...")
        
        # Stop flush timer
        self.stop_event.set()
        
        # Force flush any remaining messages
        self.force_flush()
        
        # Wait for flush thread to finish
        if self.flush_thread.is_alive():
            self.flush_thread.join(timeout=5)
        
        self.logger.info("Data processor shutdown complete")
    
    @property
    def buffer_size(self) -> int:
        """Get current buffer size."""
        return len(self.message_buffer)
    
    @property
    def is_buffer_full(self) -> bool:
        """Check if buffer is full."""
        return len(self.message_buffer) >= self.batch_size