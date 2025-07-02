"""
MQTT Client Module

This module provides MQTT client functionality with automatic reconnection,
error handling, and integration with the Redis storage system.
"""

import json
import time
import threading
from typing import Callable, Dict, Any, List, Optional
from queue import Queue, Empty

import paho.mqtt.client as mqtt

from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    MQTTConnectionError, 
    MQTTSubscriptionError, 
    TemporaryConnectionError,
    ConfigurationError
)


class MQTTClient:
    """
    MQTT Client with automatic reconnection and message queuing.
    """
    
    def __init__(self, on_message_callback: Optional[Callable] = None):
        """
        Initialize MQTT client.
        
        Args:
            on_message_callback: Callback function to handle received messages
        """
        self.logger = get_logger(__name__)
        self.config = config_manager.get_mqtt_config()
        
        if not self.config:
            raise ConfigurationError("MQTT configuration not found")
        
        # Initialize client
        self.client = None
        self.connected = False
        self.subscribed_topics = set()
        self.message_queue = Queue()
        self.stop_event = threading.Event()
        
        # Callbacks
        self.on_message_callback = on_message_callback
        
        # Connection parameters
        self.broker_config = self.config.get('broker', {})
        self.auth_config = self.config.get('auth', {})
        self.reconnect_config = self.config.get('reconnect', {})
        
        self._setup_client()
    
    def _setup_client(self):
        """Set up MQTT client with configuration."""
        try:
            # Create client instance
            client_id = f"{config_manager.get('application.name', 'mqtt_client')}_{int(time.time())}"
            self.client = mqtt.Client(
                client_id=client_id,
                clean_session=self.broker_config.get('clean_session', True)
            )
            
            # Set authentication if provided
            username = self.auth_config.get('username')
            password = self.auth_config.get('password')
            if username and password:
                self.client.username_pw_set(username, password)
            
            # Set callbacks
            self.client.on_connect = self._on_connect
            self.client.on_disconnect = self._on_disconnect
            self.client.on_message = self._on_message
            self.client.on_log = self._on_log
            
            # Configure keep alive and other options
            keepalive = self.broker_config.get('keepalive', 60)
            
            self.logger.info("MQTT client configured successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to setup MQTT client: {e}")
            raise MQTTConnectionError(f"Client setup failed: {e}")
    
    def connect(self) -> bool:
        """
        Connect to MQTT broker with retry logic.
        
        Returns:
            bool: True if connected successfully, False otherwise
        """
        if self.connected:
            self.logger.info("Already connected to MQTT broker")
            return True
        
        host = self.broker_config.get('host', 'localhost')
        port = self.broker_config.get('port', 1883)
        keepalive = self.broker_config.get('keepalive', 60)
        
        max_retries = self.reconnect_config.get('max_retries', 5)
        retry_delay = self.reconnect_config.get('retry_delay', 5)
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"Connecting to MQTT broker {host}:{port} (attempt {attempt + 1}/{max_retries})")
                
                result = self.client.connect(host, port, keepalive)
                
                if result == mqtt.MQTT_ERR_SUCCESS:
                    # Start the network loop
                    self.client.loop_start()
                    
                    # Wait for connection confirmation
                    start_time = time.time()
                    while not self.connected and time.time() - start_time < 10:
                        time.sleep(0.1)
                    
                    if self.connected:
                        self.logger.info("Successfully connected to MQTT broker")
                        return True
                    else:
                        self.logger.warning("Connection timeout - no confirmation received")
                
            except Exception as e:
                self.logger.error(f"Connection attempt {attempt + 1} failed: {e}")
            
            if attempt < max_retries - 1:
                self.logger.info(f"Waiting {retry_delay} seconds before retry...")
                time.sleep(retry_delay)
        
        raise MQTTConnectionError(f"Failed to connect after {max_retries} attempts")
    
    def disconnect(self):
        """Disconnect from MQTT broker."""
        if self.client and self.connected:
            self.logger.info("Disconnecting from MQTT broker...")
            self.stop_event.set()
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False
    
    def subscribe_to_topics(self, topics: Optional[List[str]] = None) -> bool:
        """
        Subscribe to MQTT topics.
        
        Args:
            topics: List of topics to subscribe to. If None, uses config topics.
            
        Returns:
            bool: True if all subscriptions successful
        """
        if not self.connected:
            raise MQTTConnectionError("Not connected to MQTT broker")
        
        if topics is None:
            topics = self.config.get('topics', [])
        
        if not topics:
            raise ConfigurationError("No topics specified for subscription")
        
        qos = self.config.get('qos', 1)
        success_count = 0
        
        for topic in topics:
            try:
                result, mid = self.client.subscribe(topic, qos)
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self.subscribed_topics.add(topic)
                    success_count += 1
                    self.logger.info(f"Subscribed to topic: {topic}")
                else:
                    self.logger.error(f"Failed to subscribe to topic {topic}: {result}")
            except Exception as e:
                self.logger.error(f"Exception subscribing to topic {topic}: {e}")
        
        if success_count == 0:
            raise MQTTSubscriptionError("Failed to subscribe to any topics")
        elif success_count < len(topics):
            self.logger.warning(f"Subscribed to {success_count}/{len(topics)} topics")
        
        return success_count == len(topics)
    
    def _on_connect(self, client, userdata, flags, rc):
        """Callback for successful connection."""
        if rc == 0:
            self.connected = True
            self.logger.info(f"Connected to MQTT broker with result code {rc}")
        else:
            self.logger.error(f"Failed to connect to MQTT broker with result code {rc}")
    
    def _on_disconnect(self, client, userdata, rc):
        """Callback for disconnection."""
        self.connected = False
        self.subscribed_topics.clear()
        
        if rc != 0:
            self.logger.warning(f"Unexpected disconnection with result code {rc}")
            # Attempt to reconnect
            threading.Thread(target=self._reconnect_loop, daemon=True).start()
        else:
            self.logger.info("Disconnected from MQTT broker")
    
    def _on_message(self, client, userdata, msg):
        """Callback for received messages."""
        try:
            # Decode message
            topic = msg.topic
            payload = msg.payload.decode('utf-8')
            qos = msg.qos
            timestamp = time.time()
            
            message_data = {
                'topic': topic,
                'payload': payload,
                'qos': qos,
                'timestamp': timestamp
            }
            
            # Add to internal queue
            self.message_queue.put(message_data)
            
            # Call external callback if provided
            if self.on_message_callback:
                self.on_message_callback(message_data)
            
            self.logger.debug(f"Received message on topic {topic}: {payload}")
            
       
        
        
        except Exception as e:
            self.logger.error(f"Error processing received message: {e}")
    
    def _on_log(self, client, userdata, level, buf):
        """Callback for MQTT client logs."""
        self.logger.debug(f"MQTT Log: {buf}")
    
    def _reconnect_loop(self):
        """Background thread for reconnection attempts."""
        retry_delay = self.reconnect_config.get('retry_delay', 5)
        max_retries = self.reconnect_config.get('max_retries', 5)
        
        for attempt in range(max_retries):
            if self.connected or self.stop_event.is_set():
                break
            
            self.logger.info(f"Attempting to reconnect... (attempt {attempt + 1}/{max_retries})")
            
            try:
                result = self.client.reconnect()
                if result == mqtt.MQTT_ERR_SUCCESS:
                    self.logger.info("Reconnection successful")
                    # Re-subscribe to topics
                    if self.subscribed_topics:
                        self.subscribe_to_topics(list(self.subscribed_topics))
                    break
            except Exception as e:
                self.logger.error(f"Reconnection attempt failed: {e}")
            
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
        
        if not self.connected:
            self.logger.error("Failed to reconnect after maximum attempts")
    
    def get_message(self, timeout: float = 1.0) -> Optional[Dict[str, Any]]:
        """
        Get message from internal queue.
        
        Args:
            timeout: Timeout in seconds
            
        Returns:
            Message data or None if no message available
        """
        try:
            return self.message_queue.get(timeout=timeout)
        except Empty:
            return None
    
    def get_messages(self, max_messages: int = 100, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """
        Get multiple messages from internal queue.
        
        Args:
            max_messages: Maximum number of messages to retrieve
            timeout: Timeout in seconds for the first message
            
        Returns:
            List of message data
        """
        messages = []
        
        # Get first message with timeout
        first_message = self.get_message(timeout)
        if first_message:
            messages.append(first_message)
        
        # Get additional messages without blocking
        while len(messages) < max_messages:
            message = self.get_message(timeout=0.01)
            if message is None:
                break
            messages.append(message)
        
        return messages
    
    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.connected
    
    @property
    def queue_size(self) -> int:
        """Get current message queue size."""
        return self.message_queue.qsize()