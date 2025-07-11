import json
import time
import threading
import re
from typing import Callable, Dict, Any, List, Optional, Union
from queue import Queue, Empty
from datetime import datetime
import paho.mqtt.client as mqtt

from ..config.config_manager import config_manager
from ..utils.logger import get_logger
from ..utils.exceptions import (
    MQTTConnectionError,
    MQTTSubscriptionError,
    ConfigurationError
)


class UniversalMQTTClient:

    def __init__(self, on_message_callback: Optional[Callable] = None):
        self.logger = get_logger(__name__)
        self.config = config_manager.get_mqtt_config()
        if not self.config:
            raise ConfigurationError("MQTT configuration not found")

        self.format_detectors = {
            'industrial_simulator': self._detect_industrial_simulator,
            'flat_device_id': self._detect_flat_device_id,
            'nested_data': self._detect_nested_data,
            'topic_based': self._detect_topic_based,
            'array_format': self._detect_array_format,
            'minimal_sensor': self._detect_minimal_sensor
        }

        self.category_patterns = {
            'motor': ['motor', 'rpm', 'vibration', 'torque'],
            'water': ['water', 'flow', 'pressure', 'ph'],
            'energy': ['power', 'voltage', 'current', 'solar'],
            'building': ['hvac', 'climate', 'temp', 'humidity'],
            'packaging': ['packaging', 'conveyor', 'line'],
            'test': ['test']
        }

        self.client = None
        self.connected = False
        self.subscribed_topics = set()
        self.message_queue = Queue()
        self.stop_event = threading.Event()
        self.on_message_callback = on_message_callback

        self.broker_cfg = self.config.get('broker', {})
        self.auth_cfg = self.config.get('auth', {})
        self.reconnect_cfg = self.config.get('reconnect', {})

        self.stats = {
            'messages_received': 0,
            'messages_processed': 0,
            'processing_errors': 0,
            'devices_seen': set(),
            'formats_seen': {},
            'categories_seen': {}
        }

        self._setup_client()

    def _setup_client(self):
        client_id = f"{config_manager.get('application.name', 'universal_mqtt')}_{int(time.time())}"
        self.client = mqtt.Client(client_id=client_id, clean_session=self.broker_cfg.get('clean_session', True))
        
        if self.auth_cfg.get('username') and self.auth_cfg.get('password'):
            self.client.username_pw_set(self.auth_cfg['username'], self.auth_cfg['password'])
        
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_message
        self.client.on_log = self._on_log
        
        self.logger.info("Universal MQTT client configured")

    def connect(self):
        if self.connected:
            return True
        
        host = self.broker_cfg.get('host', 'localhost')
        port = self.broker_cfg.get('port', 1883)
        keepalive = self.broker_cfg.get('keepalive', 60)
        retries = self.reconnect_cfg.get('max_retries', 5)
        delay = self.reconnect_cfg.get('retry_delay', 5)
        
        for attempt in range(retries):
            try:
                self.logger.info(f"Connecting to MQTT broker {host}:{port} (attempt {attempt+1}/{retries})")
                res = self.client.connect(host, port, keepalive)
                if res == mqtt.MQTT_ERR_SUCCESS:
                    self.client.loop_start()
                    for _ in range(100):
                        if self.connected:
                            return True
                        time.sleep(0.1)
            except Exception as e:
                self.logger.error(f"Connect attempt failed: {e}")
            
            time.sleep(delay)
        
        raise MQTTConnectionError("Failed to connect to MQTT broker")

    def subscribe_to_topics(self, topics: Optional[List[str]] = None):
        if not self.connected:
            raise MQTTConnectionError("Not connected")
        
        if topics is None:
            topics = self.config.get('topics', [])
        
        if not topics:
            raise ConfigurationError("No topics configured")
        
        qos = self.config.get('qos', 1)
        for t in topics:
            res, _ = self.client.subscribe(t, qos)
            if res == mqtt.MQTT_ERR_SUCCESS:
                self.subscribed_topics.add(t)
                self.logger.info(f"Subscribed to {t}")
            else:
                self.logger.error(f"Failed subscribing to {t}")

    def disconnect(self):
        if self.client and self.connected:
            self.stop_event.set()
            self.client.loop_stop()
            self.client.disconnect()
            self.connected = False

    def _on_connect(self, client, userdata, flags, rc):
        self.connected = rc == 0
        if self.connected:
            self.logger.info("MQTT connected")
        else:
            self.logger.error(f"MQTT connect failed rc={rc}")

    def _on_disconnect(self, client, userdata, rc):
        self.connected = False
        if rc != 0:
            self.logger.warning("Unexpected disconnect, will reconnect")
            threading.Thread(target=self._reconnect_loop, daemon=True).start()

    def _on_message(self, client, userdata, msg):
        try:
            self.stats['messages_received'] += 1
            
            payload_str = msg.payload.decode('utf-8')
            normalized = self._normalize_payload(payload_str, msg.topic, time.time())
            
            if normalized and self.on_message_callback:
                # Update stats
                device_id = normalized.get('device_id', 'unknown')
                format_type = normalized.get('metadata', {}).get('source_format', 'unknown')
                category = normalized.get('device_category', 'unknown')
                
                self.stats['devices_seen'].add(device_id)
                self.stats['formats_seen'][format_type] = self.stats['formats_seen'].get(format_type, 0) + 1
                self.stats['categories_seen'][category] = self.stats['categories_seen'].get(category, 0) + 1
                
                self.on_message_callback(normalized)
                self.stats['messages_processed'] += 1
                
        except Exception as e:
            self.stats['processing_errors'] += 1
            self.logger.error(f"Message processing error: {e}")

    def _on_log(self, client, userdata, level, buf):
        self.logger.debug(buf)

    def _reconnect_loop(self):
        delay = self.reconnect_cfg.get('retry_delay', 5)
        while not self.connected and not self.stop_event.is_set():
            try:
                self.client.reconnect()
            except Exception:
                pass
            time.sleep(delay)

    def _normalize_payload(self, payload: str, topic: str, mqtt_ts: float) -> Optional[Dict[str, Any]]:
        try:
            data = json.loads(payload)
        except json.JSONDecodeError:
            self.logger.warning("Non-JSON payload ignored")
            return None
        
        fmt = self._detect_payload_format(data, topic)
        norm = self._apply_normalization(data, topic, mqtt_ts, fmt)
        return norm

    def _detect_payload_format(self, data: Union[Dict, List], topic: str) -> str:
        for name, fn in self.format_detectors.items():
            try:
                if fn(data, topic):
                    return name
            except Exception:
                continue
        return 'unknown'

    def _detect_industrial_simulator(self, d, topic):
        return isinstance(d, dict) and 'deviceDisplayName' in d and 'data' in d

    def _detect_flat_device_id(self, d, topic):
        return isinstance(d, dict) and any(k in d for k in ['deviceId', 'device_id', 'deviceID']) and 'data' not in d

    def _detect_nested_data(self, d, topic):
        return isinstance(d, dict) and any(k in d for k in ['deviceId', 'device_id', 'deviceID']) and 'data' in d

    def _detect_topic_based(self, d, topic):
        return isinstance(d, dict) and 'deviceDisplayName' not in d and not any(k.startswith('device') for k in d)

    def _detect_array_format(self, d, topic):
        return isinstance(d, list) and d and all(isinstance(i, dict) and 'field' in i and 'value' in i for i in d)

    def _detect_minimal_sensor(self, d, topic):
        return isinstance(d, dict)

    def _apply_normalization(self, data: Union[Dict, List], topic: str, mqtt_ts: float, fmt: str) -> Dict[str, Any]:
        base = {
            'metadata': {
                'source_format': fmt,
                'mqtt_topic': topic,
                'mqtt_received_at': mqtt_ts
            }
        }

        if fmt == 'industrial_simulator':
            base.update({
                'device_id': self._clean(data['deviceDisplayName']),
                'timestamp': self._parse_ts(data['timestamp']) or mqtt_ts,
                'sensor_data': data.get('data', {})
            })
        elif fmt == 'flat_device_id':
            dev = data.get('deviceId') or data.get('device_id') or data.get('deviceID')
            ts = data.pop('timestamp', None) or data.pop('time', None) or data.pop('ts', None)
            sensor = {k: v for k, v in data.items() if not k.startswith('device')}
            base.update({
                'device_id': self._clean(str(dev)), 
                'timestamp': self._parse_ts(ts) or mqtt_ts, 
                'sensor_data': sensor
            })
        elif fmt == 'nested_data':
            dev = data.get('deviceId') or data.get('device_id') or data.get('deviceID')
            ts = data.get('timestamp')
            base.update({
                'device_id': self._clean(str(dev)), 
                'timestamp': self._parse_ts(ts) or mqtt_ts, 
                'sensor_data': data.get('data', {})
            })
        elif fmt == 'topic_based':
            dev = self._device_from_topic(topic)
            ts = data.pop('timestamp', None)
            base.update({
                'device_id': dev, 
                'timestamp': self._parse_ts(ts) or mqtt_ts, 
                'sensor_data': data
            })
        elif fmt == 'array_format':
            dev = self._device_from_topic(topic)
            sensor = {item['field']: item['value'] for item in data}
            ts = sensor.pop('timestamp', None) if 'timestamp' in sensor else None
            base.update({
                'device_id': dev, 
                'timestamp': self._parse_ts(ts) or mqtt_ts, 
                'sensor_data': sensor
            })
        else:  # minimal or unknown
            dev = self._device_from_topic(topic)
            ts = data.pop('timestamp', None)
            base.update({
                'device_id': dev, 
                'timestamp': self._parse_ts(ts) or mqtt_ts, 
                'sensor_data': data
            })

        # Add ISO datetime and category
        base['datetime'] = datetime.fromtimestamp(base['timestamp']).isoformat()
        base['device_category'] = self._detect_category(base['device_id'], base['sensor_data'])
        return base

    def _parse_ts(self, ts):
        if not ts:
            return None
        try:
            if isinstance(ts, (int, float)):
                return float(ts) if ts > 0 else None
            if isinstance(ts, str):
                if ts.endswith('Z'):
                    ts = ts[:-1] + '+00:00'
                return datetime.fromisoformat(ts).timestamp()
        except Exception:
            return None
        return None

    def _clean(self, s):
        return re.sub(r'[^a-zA-Z0-9_-]', '_', s.lower())

    def _device_from_topic(self, topic):
        return self._clean(topic.split('/')[-1])

    def _detect_category(self, dev, data):
        dev_l = dev.lower()
        for cat, pats in self.category_patterns.items():
            if any(p in dev_l for p in pats):
                return cat
        
        keys = ' '.join(data.keys()).lower()
        for cat, pats in self.category_patterns.items():
            if any(p in keys for p in pats):
                return cat
        
        return 'general'

    def get_stats(self) -> Dict[str, Any]:
        """Get client statistics."""
        stats = self.stats.copy()
        stats['devices_seen'] = list(stats['devices_seen'])
        stats['is_connected'] = self.connected
        return stats

    @property
    def is_connected(self) -> bool:
        """Check if client is connected."""
        return self.connected
