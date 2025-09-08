from enum import Enum


class CollectionStatusNames(Enum):
    NOT_COLLECTED = "not_collected"
    IN_PROGRESS = "in_progress"
    COLLECTED = "collected"
    FAILED = "failed"
    NEEDS_RETRY = "needs_retry"
    SKIPPED = "skipped"
    DEBUG_FAIL = "debug_failed"


class AttemptStatusNames(Enum):
    SUCCESS = "success"
    FAILED = "failed"
    PENDING = "pending"
    TIMED_OUT = "timed out"
    CANCELLED = "cancelled"


class ErrorTypes(Enum):
    PAGE_NOT_FOUND = "page_not_found"
    DISAMBIGUATION_ERROR = "disambiguation_error"
    NETWORK_TIMEOUT = "network_timeout"
    API_RATE_LIMIT = "api_rate_limit"
    INVALID_LANGUAGE_CODE = "invalid_language_code"
    CONTENT_TOO_SHORT = "content_too_short"
    ACCESS_DENIED_ERROR = "access_denied_error"
    SERVER_ERROR = "server_error"
    PARSING_ERROR = "parsing_error"
    ENCODING_ERROR = "encoding_error"
    UNKNOWN_ERROR = "unknown_error"


class ValidationStatusNames(Enum):
    VALID = "valid"
    INVALID = "invalid"
    PENDING = "pending"
    WARNING = "warning"
    SKIPPED = "skipped"
