import signal
import sys
import time
import threading
from mqtt_redis_ingestion.config.config_manager import config_manager
from mqtt_redis_ingestion.core.mqtt_client import UniversalMQTTClient
from mqtt_redis_ingestion.core.redis_client import EnhancedRedisClient
from mqtt_redis_ingestion.core.data_processor import EnhancedDataProcessor
from mqtt_redis_ingestion.utils.logger import configure_logging, get_logger

class EnhancedIngestionApp:
   
    
    def __init__(self):
        configure_logging()
        self.logger = get_logger(__name__)
        self.logger.info("Initializing Enhanced Ingestion App with Redis modules")
        
        # Initialize enhanced components
        self.redis = EnhancedRedisClient()
        self.processor = EnhancedDataProcessor(self.redis)
        self.mqtt = UniversalMQTTClient(on_message_callback=self.processor.process_message)
        
        self.running = False
        self.shutdown_evt = threading.Event()
        
        # Signal handlers
        signal.signal(signal.SIGINT, self._sig)
        signal.signal(signal.SIGTERM, self._sig)
        
        self.logger.info("Enhanced ingestion app initialized with TimeSeries, JSON, and Bloom support")

    def _sig(self, *_):
        self.logger.info("Signal received, shutting down...")
        self.shutdown()

    def start(self):
        """Start the enhanced ingestion service."""
        try:
            # Test Redis modules availability
            self._test_redis_modules()
            
            self.logger.info("Connecting to MQTT...")
            self.mqtt.connect()
            self.mqtt.subscribe_to_topics()
            
            self.running = True
            self.start_time = time.time()
            
            # Start enhanced statistics reporting
            threading.Thread(target=self._enhanced_stats, daemon=True).start()
            
            self.logger.info("Enhanced system up with Redis modules - Ctrl+C to stop")
            self._print_startup_info()
            
        except Exception as e:
            self.logger.error(f"Failed to start enhanced ingestion: {e}")
            raise

    def _test_redis_modules(self):
        """Test availability of Redis modules."""
        try:
            # Test TimeSeries module
            self.redis.redis_client.execute_command('TS.INFO', 'test_key_that_does_not_exist')
        except Exception as e:
            if "key does not exist" not in str(e).lower():
                self.logger.warning("TimeSeries module may not be available")
        
        try:
            # Test JSON module
            self.redis.redis_client.execute_command('JSON.GET', 'test_key_that_does_not_exist')
        except Exception as e:
            if "key does not exist" not in str(e).lower():
                self.logger.warning("RedisJSON module may not be available")
        
        try:
            # Test Bloom module
            self.redis.redis_client.execute_command('BF.EXISTS', 'test_filter', 'test_item')
        except Exception as e:
            if "key does not exist" not in str(e).lower():
                self.logger.warning("Bloom filter module may not be available")

    def _enhanced_stats(self):
        """Enhanced statistics reporting with module-specific metrics."""
        while self.running and not self.shutdown_evt.is_set():
            time.sleep(60)  # Report every minute
            
            try:
                st = self.processor.get_stats()
                uptime = int(time.time() - self.start_time)
                
                self.logger.info(
                    f"Enhanced Stats - Uptime: {uptime}s | "
                    f"Processed: {st['messages_processed']} | "
                    f"Stored: {st['messages_stored']} | "
                    f"TimeSeries: {st.get('timeseries_points_stored', 0)} | "
                    f"JSON Docs: {st.get('json_documents_stored', 0)} | "
                    f"Duplicates: {st.get('duplicates_filtered', 0)} | "
                    f"Boolean Conversions: {st.get('boolean_conversions', 0)} | "
                    f"Errors: {st['processing_errors']} | "
                    f"Buffer: {st['buffer_size']} | "
                    f"Devices: {len(st.get('devices_processed', []))} | "
                    f"Metrics: {len(st.get('metrics_tracked', []))}"
                )
                
            except Exception as e:
                self.logger.error(f"Error in enhanced stats reporting: {e}")

    def _print_startup_info(self):
        """Print enhanced startup information."""
        try:
            print("\n" + "="*80)
            print("🚀 Enhanced MQTT-Redis Ingestion System v2.0 (Simplified)")
            print("="*80)
            print("📊 Redis Modules Enabled:")
            print("   • TimeSeries: Compressed numerical data storage")
            print("   • RedisJSON: Native JSON document storage and querying")
            print("   • Bloom Filters: Efficient duplicate detection")
            print("\n🔧 Boolean Conversion: Automatic conversion for Redis compatibility")
            print("📋 Simplified Device Documents: Removed type, category, and metadata")
            print("🔍 Enhanced Query Capabilities:")
            print("   • Time-series analytics and aggregations")
            print("   • Basic JSON document searches")
            print("   • Real-time device monitoring")
            print("   • Historical data analysis")
            print("\n💾 Storage Architecture:")
            print("   • TimeSeries: ts:{metric}:{device_id}")
            print("   • JSON Documents: device:{device_id}:doc (simplified)")
            print("   • Message Archive: archive:{device_id}:{timestamp}")
            print("   • Search Index: device_search_idx (basic fields)")
            print("   • Deduplication: msg_dedup_filter (Bloom)")
            print("="*80 + "\n")
            
        except Exception as e:
            self.logger.error(f"Error printing startup info: {e}")

    def shutdown(self):
        """Enhanced shutdown with module-specific cleanup."""
        if not self.running:
            return
            
        self.logger.info("Shutting down enhanced ingestion system...")
        self.running = False
        self.shutdown_evt.set()
        
        try:
            # Enhanced shutdown sequence
            self.processor.shutdown()
            self.mqtt.disconnect()
            self.redis.close()
            
            # Print final enhanced statistics
            self._print_final_stats()
            self.logger.info("Enhanced shutdown complete")
            
        except Exception as e:
            self.logger.error(f"Error during enhanced shutdown: {e}")

    def _print_final_stats(self):
        """Print comprehensive final statistics."""
        try:
            st = self.processor.get_stats()
            uptime = int(time.time() - self.start_time)
            
            print("\n" + "="*70)
            print("📊 ENHANCED INGESTION - FINAL STATISTICS")
            print("="*70)
            print(f"⏱️  Total Uptime: {uptime} seconds")
            print(f"📨 Messages Processed: {st['messages_processed']}")
            print(f"💾 Messages Stored: {st['messages_stored']}")
            print(f"📈 TimeSeries Points: {st.get('timeseries_points_stored', 0)}")
            print(f"📄 JSON Documents: {st.get('json_documents_stored', 0)}")
            print(f"🔄 Duplicates Filtered: {st.get('duplicates_filtered', 0)}")
            print(f"🔧 Boolean Conversions: {st.get('boolean_conversions', 0)}")
            print(f"❌ Processing Errors: {st['processing_errors']}")
            print(f"✅ Validation Errors: {st['validation_errors']}")
            print(f"📦 Batches Processed: {st['batches_processed']}")
            print(f"🔧 Devices Tracked: {len(st.get('devices_processed', []))}")
            print(f"📊 Metrics Tracked: {len(st.get('metrics_tracked', []))}")
            
            if st['messages_processed'] > 0:
                success_rate = (st['messages_stored'] / st['messages_processed']) * 100
                print(f"🎯 Success Rate: {success_rate:.1f}%")
                
            print("="*70 + "\n")
            
        except Exception as e:
            self.logger.error(f"Error printing final stats: {e}")

    def run(self):
        """Run the enhanced ingestion application."""
        try:
            self.start()
            
            # Main loop
            while self.running and not self.shutdown_evt.is_set():
                time.sleep(1)
                
        except KeyboardInterrupt:
            self.logger.info("Keyboard interrupt received")
        except Exception as e:
            self.logger.error(f"Runtime error in enhanced ingestion: {e}")
        finally:
            self.shutdown()

def main():
    """Main entry point for enhanced ingestion."""
    try:
        EnhancedIngestionApp().run()
    except Exception as exc:
        print(f"Fatal error in enhanced ingestion: {exc}")
        sys.exit(1)

if __name__ == "__main__":
    main()
