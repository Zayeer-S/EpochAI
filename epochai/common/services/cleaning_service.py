from datetime import datetime
from typing import Any, Dict, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.cleaned_data_dao import CleanedDataDAO
from epochai.common.database.dao.raw_data_dao import RawDataDAO
from epochai.common.database.dao.validation_statuses_dao import ValidationStatusesDAO
from epochai.common.database.models import RawData
from epochai.common.logging_config import get_logger


class CleaningService:
    def __init__(
        self,
        cleaner_name: str,
        cleaner_version: str,
    ):
        self.logger = get_logger(__name__)

        # CONFIG
        self.data_config = ConfigLoader.get_data_config()

        # ASSIGN PARAMETERS TO INSTANCE VARS
        self._cleaner_name = cleaner_name
        self._cleaner_version = cleaner_version

        # DAOs
        self.cleaned_data_dao = CleanedDataDAO()
        self._validation_statuses_dao = ValidationStatusesDAO()
        self.raw_data_dao = RawDataDAO()

        # VALIDATION STATUS CACHING
        self._validation_status_cache = self._load_validation_statuses()

        self.logger.debug(f"Initialized {__name__} for {cleaner_name} v{cleaner_version}")

    def _load_validation_statuses(self) -> Dict[str, int]:
        """Loads and caches validation status ids"""
        try:
            statuses = self._validation_statuses_dao.get_all()
            return {status.validation_status_name: status.id for status in statuses if status.id}
        except Exception as general_error:
            self.logger.error(f"Failed to load validation statuses: {general_error}")
            return {}

    def get_validation_status_id(
        self,
        status_name: str,
    ) -> Optional[int]:
        """Gets validation status ID"""
        status_id = self._validation_status_cache.get(status_name)
        if status_id is not None:
            return int(status_id)

        self.logger.warning(f"Validation status '{status_name}' not found")
        return None

    def save_cleaned_content(
        self,
        raw_data: RawData,
        transformed_metadata: Dict[str, Any],
        is_valid: bool,
        validation_error: Optional[Dict[str, Any]],
        cleaning_time_ms: int,
        schema_id: int,
    ) -> Optional[int]:
        """Saves cleaned content"""
        try:
            validation_status_id: int = self.get_validation_status_id("valid" if is_valid else "invalid")

            if not schema_id:
                self.logger.error(f"No metadata schema id available for {self._cleaner_name}")
                return None

            cleaned_data_id: Optional[int] = self.cleaned_data_dao.create_cleaned_data(
                raw_data_id=raw_data.id,
                cleaned_data_metadata_schema_id=schema_id,
                title=raw_data.title,
                language_code=raw_data.language_code,
                cleaner_used=self._cleaner_name,
                cleaner_version=self._cleaner_version,
                cleaning_time_ms=cleaning_time_ms,
                url=raw_data.url,
                metadata=transformed_metadata,
                validation_status_id=validation_status_id,
                validation_error=validation_error,
                cleaned_at=datetime.now(),
            )

            if cleaned_data_id:
                status_msg = "valid" if is_valid else "invalid"
                self.logger.info(
                    f"Successfully saved cleaned data {cleaned_data_id}. ({status_msg}, {cleaning_time_ms}ms)",
                )

            return cleaned_data_id

        except Exception as general_error:
            self.logger.error(f"Error saving cleaned data: {general_error}")
            return None

    def save_error_record(
        self,
        raw_data: RawData,
        error: Exception,
        cleaning_time_ms: int,
        schema_id: int,
    ) -> Optional[int]:
        """Save error information when cleaning fails"""
        try:
            if not schema_id:
                return None

            error_dict = {
                "error": str(error),
                "error_type": type(error).__name__,
                "cleaning_failed": True,
            }

            return self.cleaned_data_dao.create_cleaned_data(
                raw_data_id=raw_data.id,
                cleaned_data_metadata_schema_id=schema_id,
                title=raw_data.title,
                language_code=raw_data.language_code,
                url=raw_data.url,
                metadata={},
                validation_status_id=self.get_validation_status_id("invalid"),
                validation_error=error_dict,
                cleaner_used=self._cleaner_name,
                cleaner_version=self._cleaner_version,
                cleaning_time_ms=cleaning_time_ms,
                cleaned_at=datetime.now(),
            )

        except Exception as general_error:
            self.logger.error(f"Failed to save error information: {general_error}")
            return None
