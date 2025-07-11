class MQTTRedisIngestionError(Exception):
    """Base exception for all MQTT-Redis ingestion errors."""
    
    def __init__(self, message: str, error_code: str = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class ConfigurationError(MQTTRedisIngestionError):
    """Raised when there are configuration-related errors."""
    pass


class MQTTConnectionError(MQTTRedisIngestionError):
    """Raised when MQTT connection fails."""
    pass


class MQTTSubscriptionError(MQTTRedisIngestionError):
    """Raised when MQTT subscription fails."""
    pass


class RedisConnectionError(MQTTRedisIngestionError):
    """Raised when Redis connection fails."""
    pass


class RedisOperationError(MQTTRedisIngestionError):
    """Raised when Redis operations fail."""
    pass


class DataProcessingError(MQTTRedisIngestionError):
    """Raised when data processing fails."""
    pass


class DataValidationError(MQTTRedisIngestionError):
    """Raised when data validation fails."""
    pass


class RetryableError(MQTTRedisIngestionError):
    """Base class for errors that can be retried."""
    
    def __init__(self, message: str, retry_after: int = None, error_code: str = None):
        super().__init__(message, error_code)
        self.retry_after = retry_after


class TemporaryConnectionError(RetryableError):
    """Raised for temporary connection issues that can be retried."""
    pass


class RateLimitError(RetryableError):
    """Raised when rate limits are exceeded."""
    pass


class CircuitBreakerError(MQTTRedisIngestionError):
    """Raised when circuit breaker is open."""
    pass
