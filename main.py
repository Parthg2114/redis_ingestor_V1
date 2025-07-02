"""
Main Application Module

This is the main entry point for the MQTT-Redis data ingestion system.
It orchestrates all components and handles the main application lifecycle.
"""

import signal
import sys
import time
import threading
from typing import Dict, Any

from mqtt_redis_ingestion.config.config_manager import config_manager
from mqtt_redis_ingestion.core.mqtt_client import MQTTClient
from mqtt_redis_ingestion.core.redis_client import RedisClient
from mqtt_redis_ingestion.core.data_processor import DataProcessor
from mqtt_redis_ingestion.utils.logger import configure_logging, get_logger
from mqtt_redis_ingestion.utils.exceptions import (
    MQTTRedisIngestionError,
    ConfigurationError,
    MQTTConnectionError,
    RedisConnectionError
)


class MQTTRedisIngestionApp:
    """
    Main application class that coordinates MQTT-Redis data ingestion.
    """
    
    def __init__(self):
        """Initialize the application."""
        # Configure logging first
        configure_logging()
        self.logger = get_logger(__name__)
        
        self.logger.info("Initializing MQTT-Redis Ingestion Application")
        
        # Application components
        self.mqtt_client = None
        self.redis_client = None
        self.data_processor = None
        
        # Application state
        self.running = False
        self.shutdown_event = threading.Event()
        
        # Statistics
        self.start_time = None
        self.stats_thread = None
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
        
        self.logger.info("Application initialized")
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals."""
        self.logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown()
    
    def _on_mqtt_message(self, message_data: Dict[str, Any]):
        """
        Callback for handling MQTT messages.
        
        Args:
            message_data: Message data from MQTT client
        """
        try:
            if self.data_processor:
                self.data_processor.process_message(message_data)
        except Exception as e:
            self.logger.error(f"Error processing MQTT message: {e}")
    
    def _on_batch_processed(self, messages: list, stored_count: int):
        """
        Callback for batch processing completion.
        
        Args:
            messages: List of processed messages
            stored_count: Number of messages stored successfully
        """
        self.logger.debug(f"Batch processed: {stored_count}/{len(messages)} messages stored")
    
    def initialize_components(self):
        """Initialize all application components."""
        try:
            self.logger.info("Initializing application components...")
            
            # Initialize Redis client
            self.logger.info("Initializing Redis client...")
            self.redis_client = RedisClient()
            
            # Test Redis connection
            if not self.redis_client.health_check():
                raise RedisConnectionError("Redis health check failed")
            
            # Initialize data processor
            self.logger.info("Initializing data processor...")
            self.data_processor = DataProcessor(
                redis_client=self.redis_client,
                batch_callback=self._on_batch_processed
            )
            
            # Initialize MQTT client
            self.logger.info("Initializing MQTT client...")
            self.mqtt_client = MQTTClient(on_message_callback=self._on_mqtt_message)
            
            self.logger.info("All components initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize components: {e}")
            raise
    
    def start(self):
        """Start the application."""
        try:
            if self.running:
                self.logger.warning("Application is already running")
                return
            
            self.logger.info("Starting MQTT-Redis Ingestion Application")
            self.start_time = time.time()
            
            # Initialize components if not already done
            if not self.mqtt_client:
                self.initialize_components()
            
            # Connect to MQTT broker
            self.logger.info("Connecting to MQTT broker...")
            self.mqtt_client.connect()
            
            # Subscribe to topics
            self.logger.info("Subscribing to MQTT topics...")
            self.mqtt_client.subscribe_to_topics()
            
            # Start statistics reporting
            self.stats_thread = threading.Thread(target=self._stats_reporter, daemon=True)
            self.stats_thread.start()
            
            self.running = True
            self.logger.info("Application started successfully")
            
            # Print startup information
            self._print_startup_info()
            
        except Exception as e:
            self.logger.error(f"Failed to start application: {e}")
            self.shutdown()
            raise
    
    def run(self):
        """Run the application main loop."""
        try:
            self.start()
            
            self.logger.info("Application is running. Press Ctrl+C to stop.")
            
            # Main loop - just wait for shutdown signal
            while self.running and not self.shutdown_event.is_set():
                time.sleep(1)
                
                # Check component health
                self._health_check()
            
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Error in main loop: {e}")
        finally:
            self.shutdown()
    
    def shutdown(self):
        """Shutdown the application gracefully."""
        if not self.running:
            return
        
        self.logger.info("Shutting down application...")
        self.running = False
        self.shutdown_event.set()
        
        try:
            # Shutdown data processor first (flush remaining messages)
            if self.data_processor:
                self.data_processor.shutdown()
            
            # Disconnect MQTT client
            if self.mqtt_client:
                self.mqtt_client.disconnect()
            
            # Close Redis connections
            if self.redis_client:
                self.redis_client.close()
            
            # Print final statistics
            self._print_final_stats()
            
            self.logger.info("Application shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during shutdown: {e}")
    
    def _health_check(self):
        """Perform periodic health checks on components."""
        try:
            # Check MQTT connection
            if self.mqtt_client and not self.mqtt_client.is_connected:
                self.logger.warning("MQTT client is disconnected")
            
            # Check Redis connection
            if self.redis_client and not self.redis_client.health_check():
                self.logger.warning("Redis health check failed")
                
        except Exception as e:
            self.logger.error(f"Error during health check: {e}")
    
    def _stats_reporter(self):
        """Background thread for reporting statistics."""
        while self.running and not self.shutdown_event.is_set():
            try:
                time.sleep(60)  # Report every minute
                self._log_statistics()
            except Exception as e:
                self.logger.error(f"Error in stats reporter: {e}")
    
    def _log_statistics(self):
        """Log current application statistics."""
        try:
            if not self.data_processor:
                return
            
            stats = self.data_processor.get_stats()
            
            uptime = time.time() - self.start_time if self.start_time else 0
            
            self.logger.info(
                f"Stats - Uptime: {uptime:.0f}s, "
                f"Processed: {stats['messages_processed']}, "
                f"Stored: {stats['messages_stored']}, "
                f"Errors: {stats['processing_errors']}, "
                f"Buffer: {stats['buffer_size']}"
            )
            
        except Exception as e:
            self.logger.error(f"Error logging statistics: {e}")
    
    def _print_startup_info(self):
        """Print startup information."""
        try:
            app_config = config_manager.get_app_config()
            mqtt_config = config_manager.get_mqtt_config()
            redis_config = config_manager.get_redis_config()
            
            print("\n" + "="*60)
            print(f"🚀 {app_config.get('name', 'MQTT-Redis Ingestion')} v{app_config.get('version', '1.0.0')}")
            print("="*60)
            print(f"📡 MQTT Broker: {mqtt_config.get('broker', {}).get('host')}:{mqtt_config.get('broker', {}).get('port')}")
            print(f"🗃️  Redis Server: {redis_config.get('host')}:{redis_config.get('port')}")
            print(f"📋 Topics: {', '.join(mqtt_config.get('topics', []))}")
            print(f"⚙️  Environment: {app_config.get('environment', 'unknown')}")
            print("="*60)
            print("✅ Application is running successfully!")
            print("💡 Press Ctrl+C to stop")
            print("="*60 + "\n")
            
        except Exception as e:
            self.logger.error(f"Error printing startup info: {e}")
    
    def _print_final_stats(self):
        """Print final statistics before shutdown."""
        try:
            if not self.data_processor or not self.start_time:
                return
            
            stats = self.data_processor.get_stats()
            uptime = time.time() - self.start_time
            
            print("\n" + "="*60)
            print("📊 FINAL STATISTICS")
            print("="*60)
            print(f"⏱️  Total Uptime: {uptime:.1f} seconds")
            print(f"📨 Messages Processed: {stats['messages_processed']}")
            print(f"💾 Messages Stored: {stats['messages_stored']}")
            print(f"✅ Messages Validated: {stats['messages_validated']}")
            print(f"📦 Batches Processed: {stats['batches_processed']}")
            print(f"❌ Processing Errors: {stats['processing_errors']}")
            print(f"⚠️  Validation Errors: {stats['validation_errors']}")
            
            if stats['messages_processed'] > 0:
                success_rate = (stats['messages_stored'] / stats['messages_processed']) * 100
                print(f"📈 Success Rate: {success_rate:.1f}%")
                
                if uptime > 0:
                    throughput = stats['messages_processed'] / uptime
                    print(f"🚀 Average Throughput: {throughput:.1f} messages/second")
            
            print("="*60 + "\n")
            
        except Exception as e:
            self.logger.error(f"Error printing final stats: {e}")


def main():
    """Main entry point."""
    try:
        app = MQTTRedisIngestionApp()
        app.run()
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()