SUCCESS = "success"
WARNING = "warning"
CANCELLED = "cancelled"
FAILED = "failed"
PENDING = "pending"
WARNING = "warning"
SUCCESS = "success"
QUERYING = "querying"
PROCESSING = "processing"
ABORTED = "aborted"
QUEUED = "queued"
INTERRUPTED = "interrupted"
REPORTING = "reporting"

TERMINAL_STATUSES = [CANCELLED, FAILED, SUCCESS, WARNING, INTERRUPTED]
PENDING_STATUSES = [
    QUEUED,
    QUERYING,
    PROCESSING,
    REPORTING,
    PENDING,
]

# The new Hightouch API is confusingly called v1, where the old one is called v2.
HIGHTOUCH_API_BASE_V3 = "api/v1/"
HIGHTOUCH_API_BASE_V2 = "api/v2/rest/"
HIGHTOUCH_API_BASE_V1 = "api/v2/rest/"

DEFAULT_POLL_INTERVAL = 3
