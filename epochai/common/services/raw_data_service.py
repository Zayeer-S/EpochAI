from typing import Any, Dict, Optional

from epochai.common.database.dao.raw_data_dao import RawDataDAO
from epochai.common.database.dao.validation_statuses_dao import ValidationStatusesDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class RawDataService:
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self):
        self._logger = get_logger(__name__)
        self._raw_data_dao = RawDataDAO()
        self._validation_statuses_dao = ValidationStatusesDAO()
        self._logger.debug("RawDataService Initialized")

    handle_generic_errors_gracefully("while creating raw data database object", None)

    def create_raw_data(
        self,
        collection_attempt_id: int,
        raw_data_metadata_schema_id: int,
        item: Dict[str, Any],
        language_code: str,
        metadata: Dict[str, Any],
        validation_status_name: str,
        validation_error: Dict[str, Any],
        filepath_of_save: Optional[str],
    ):
        validation_status_obj = self._validation_statuses_dao.get_by_name(validation_status_name)
        if not validation_status_obj:
            self._logger.error(f"Validation status ID returning as None for: {validation_status_name}")
            return None

        title = item.get("title")
        url = item.get("url")

        result = self._raw_data_dao.create_raw_data(
            collection_attempt_id=collection_attempt_id,
            raw_data_metadata_schema_id=raw_data_metadata_schema_id,
            title=title,
            language_code=language_code,
            url=url,
            metadata=metadata,
            validation_status_id=validation_status_obj.id,
            validation_error=validation_error,
            filepath_of_save=filepath_of_save,
        )

        if not result:
            self._logger.error(f"Error while creating raw data for attempt '{collection_attempt_id}' and title '{title}'")

        return result if result else None
