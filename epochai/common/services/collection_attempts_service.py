from typing import Any, Dict, Optional

from epochai.common.database.dao.attempt_statuses_dao import AttemptStatusesDAO
from epochai.common.database.dao.collection_attempts_dao import CollectionAttemptsDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class CollectionAttemptsService:
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self):
        self._logger = get_logger(__name__)

        self.collection_attempts_dao = CollectionAttemptsDAO()
        self.attempt_statuses_dao = AttemptStatusesDAO()

        self._logger.debug(f"{__name__} Initialized")

    @handle_generic_errors_gracefully("while creating collection attempt", {})
    def create_collection_attempt(
        self,
        item: Dict[str, Any],
        collection_target_id: int,
        language_code: str,
        new_status_name: str,
        error_message: str = "",
    ) -> Optional[int]:
        status_obj = self.attempt_statuses_dao.get_by_name(new_status_name)
        if not status_obj:
            self._logger.error(f"Error getting collection_status_id for {new_status_name}")
            return None

        attempt_id = self.collection_attempts_dao.create_attempt(
            collection_target_id=collection_target_id,
            language_code=language_code,
            search_term_used=item.get("title"),
            attempt_status_id=status_obj.id,
            error_type_id=None,
            error_message=error_message,
        )

        return attempt_id if attempt_id else None
