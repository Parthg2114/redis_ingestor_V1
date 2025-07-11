# MQTT-Redis Data Ingestion System

A modular, production-ready Python application for subscribing to MQTT broker messages and ingesting data into Redis database with comprehensive error handling, connection pooling, and batch processing capabilities.

## 🏗️ Project Structure

```
mqtt_redis_ingestion/
├── mqtt_redis_ingestion/           # Main package
│   ├── __init__.py
│   ├── core/                       # Core functionality modules
│   │   ├── __init__.py
│   │   ├── mqtt_client.py          # MQTT client with auto-reconnect
│   │   ├── redis_client.py         # Redis client with connection pooling
│   │   └── data_processor.py       # Data processing and validation
│   ├── config/                     # Configuration management
│   │   ├── __init__.py
│   │   └── config_manager.py       # JSON config loader
│   └── utils/                      # Utility modules
│       ├── __init__.py
│       ├── logger.py               # Logging configuration
│       └── exceptions.py           # Custom exceptions
├── config/                         # Configuration files
│   ├── config.json                 # Main application config
│   └── logging_config.json         # Logging configuration
├── tests/                          # Test modules
│   ├── __init__.py
│   ├── test_mqtt_client.py
│   └── test_redis_client.py
├── logs/                           # Log files (created automatically)
├── main.py                         # Application entry point
├── requirements.txt                # Python dependencies
└── README.md                       # This file
```

## ✨ Features

- **🔄 Automatic Reconnection**: MQTT client with robust reconnection logic
- **🏊 Connection Pooling**: Redis client with connection pooling for optimal performance
- **📦 Batch Processing**: Configurable batch processing for efficient data storage
- **🔍 Data Validation**: Comprehensive message validation and transformation
- **📊 Statistics**: Real-time processing statistics and monitoring
- **🗂️ Modular Design**: Clean separation of concerns with well-defined modules
- **⚙️ Configuration-Driven**: JSON-based configuration management
- **📝 Comprehensive Logging**: Structured logging with multiple output formats
- **🛡️ Error Handling**: Robust error handling with custom exception hierarchy
- **🔧 Production-Ready**: Signal handling, graceful shutdown, and health checks

## 🚀 Quick Start

### Prerequisites

- Python 3.7 or higher
- MQTT broker (e.g., Mosquitto, EMQX, or cloud service)
- Redis server (local or cloud)

### Installation

1. **Clone or download the project**
2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application**:
   Edit `config/config.json` to match your environment:
   ```json
   {
     "mqtt": {
       "broker": {
         "host": "your-mqtt-broker-host",
         "port": 1883
       },
       "topics": [
         "your/topic/here"
       ]
     },
     "redis": {
       "host": "your-redis-host",
       "port": 6379
     }
   }
   ```

4. **Run the application**:
   ```bash
   python main.py
   ```

## ⚙️ Configuration

### Main Configuration (`config/config.json`)

```json
{
  "mqtt": {
    "broker": {
      "host": "localhost",
      "port": 1883,
      "keepalive": 60,
      "clean_session": true
    },
    "auth": {
      "username": null,
      "password": null
    },
    "topics": [
      "sensors/temperature",
      "sensors/humidity"
    ],
    "qos": 1,
    "reconnect": {
      "max_retries": 5,
      "retry_delay": 5
    }
  },
  "redis": {
    "host": "localhost",
    "port": 6379,
    "db": 0,
    "password": null,
    "connection_pool": {
      "max_connections": 10
    },
    "key_prefix": "mqtt_data:",
    "ttl": 3600
  },
  "data_processing": {
    "batch_size": 100,
    "flush_interval": 30,
    "data_validation": true
  }
}
```

### Logging Configuration (`config/logging_config.json`)

The logging configuration supports multiple handlers:
- Console output for real-time monitoring
- File logging with rotation
- Separate error log files

## 🔧 Usage Examples

### Basic Usage

```python
from mqtt_redis_ingestion.main import MQTTRedisIngestionApp

# Create and run the application
app = MQTTRedisIngestionApp()
app.run()
```

### Using Individual Components

```python
from mqtt_redis_ingestion.core.mqtt_client import MQTTClient
from mqtt_redis_ingestion.core.redis_client import RedisClient
from mqtt_redis_ingestion.core.data_processor import DataProcessor

# Initialize Redis client
redis_client = RedisClient()

# Initialize data processor
processor = DataProcessor(redis_client)

# Initialize MQTT client with callback
def on_message(message_data):
    processor.process_message(message_data)

mqtt_client = MQTTClient(on_message_callback=on_message)
mqtt_client.connect()
mqtt_client.subscribe_to_topics()
```

## 📊 Data Flow

1. **MQTT Messages** → MQTT Client receives messages from broker
2. **Message Processing** → Data Processor validates and transforms messages
3. **Batch Accumulation** → Messages are batched for efficient storage
4. **Redis Storage** → Batches are stored in Redis with automatic key generation
5. **Monitoring** → Statistics and health checks provide system visibility

## 🧪 Testing

```bash
# Install test dependencies
pip install pytest pytest-cov

# Run tests
pytest tests/

# Run with coverage
pytest --cov=mqtt_redis_ingestion tests/
```

## 📈 Monitoring and Statistics

The application provides comprehensive statistics:

- **Messages processed**: Total number of messages received
- **Messages stored**: Successfully stored in Redis
- **Processing errors**: Failed processing attempts
- **Validation errors**: Messages that failed validation
- **Batch statistics**: Batch processing metrics
- **Connection health**: MQTT and Redis connection status

## 🔍 Troubleshooting

### Common Issues

1. **Connection Failures**:
   - Check MQTT broker and Redis server connectivity
   - Verify configuration in `config.json`
   - Review log files in `logs/` directory

2. **Message Processing Errors**:
   - Enable debug logging
   - Check message format and validation rules
   - Review data transformation logic

3. **Performance Issues**:
   - Adjust batch size and flush interval
   - Monitor Redis connection pool settings
   - Check system resources

### Log Files

- `logs/mqtt_redis_ingestion.log`: Main application log
- `logs/errors.log`: Error-specific log entries



---

**Built with ❤️ for robust IoT data ingestion**
