import signal
import sys
import time
import threading
from mqtt_redis_ingestion.config.config_manager import config_manager
from mqtt_redis_ingestion.core.mqtt_client import UniversalMQTTClient
from mqtt_redis_ingestion.core.redis_client import RedisClient  # existing client
from mqtt_redis_ingestion.core.data_processor import SimplifiedDataProcessor
from mqtt_redis_ingestion.utils.logger import configure_logging, get_logger


class SimpleIngestionApp:
    def __init__(self):
        configure_logging()
        self.logger = get_logger(__name__)
        self.logger.info("Initialising Simple Ingestion App")

        self.redis = RedisClient()
        self.processor = SimplifiedDataProcessor(self.redis)
        self.mqtt = UniversalMQTTClient(on_message_callback=self.processor.process_message)

        self.running = False
        self.shutdown_evt = threading.Event()
        signal.signal(signal.SIGINT, self._sig)
        signal.signal(signal.SIGTERM, self._sig)

    def _sig(self, *_):
        self.logger.info("Signal received, shutting down …")
        self.shutdown()

    def start(self):
        self.logger.info("Connecting to MQTT …")
        self.mqtt.connect()
        self.mqtt.subscribe_to_topics()
        self.running = True
        self.start_time = time.time()
        threading.Thread(target=self._stats, daemon=True).start()
        self.logger.info("System up – Ctrl+C to stop")

    def _stats(self):
        while self.running and not self.shutdown_evt.is_set():
            time.sleep(60)
            st = self.processor.get_stats()
            self.logger.info(
                f"Uptime {int(time.time()-self.start_time)}s | "
                f"Processed {st['messages_processed']} | "
                f"Stored {st['messages_stored']} | "
                f"Errors {st['processing_errors']} | "
                f"Buffer {st['buffer_size']}"
            )

    def shutdown(self):
        if not self.running:
            return
        self.running = False
        self.shutdown_evt.set()
        self.processor.shutdown()
        self.mqtt.disconnect()
        self.redis.close()
        self.logger.info("Shutdown complete")

    def run(self):
        try:
            self.start()
            while self.running and not self.shutdown_evt.is_set():
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            self.shutdown()


def main():
    try:
        SimpleIngestionApp().run()
    except Exception as exc:
        print(f"Fatal error: {exc}")
        sys.exit(1)


if __name__ == "__main__":
    main()
