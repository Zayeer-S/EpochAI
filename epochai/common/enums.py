from enum import Enum


class CollectionStatusNames(Enum):
    NOT_COLLECTED = "not_collected"
    IN_PROGRESS = "in_progress"
    COLLECTED = "collected"
    FAILED = "failed"
    NEEDS_RETRY = "needs_retry"
    SKIPPED = "skipped"
    DEBUG_FAIL = "debug_failed"
