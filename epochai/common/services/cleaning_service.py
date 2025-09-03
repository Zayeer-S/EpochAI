from datetime import datetime
from typing import Any, Dict, Optional, Tuple

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.cleaned_data_dao import CleanedDataDAO
from epochai.common.database.dao.cleaned_data_metadata_schemas_dao import CleanedDataMetadataSchemasDAO
from epochai.common.database.dao.validation_statuses_dao import ValidationStatusesDAO
from epochai.common.database.models import RawData
from epochai.common.logging_config import get_logger
from epochai.common.utils.decorators import handle_generic_errors_gracefully
from epochai.common.utils.dynamic_schema_utils import DynamicSchemaUtils


class CleaningService:
    def __init__(
        self,
        cleaner_name: str,
        cleaner_version: str,
    ):
        # CONFIG
        self.config = ConfigLoader.get_data_config()

        # BASIC
        self.cleaner_name = cleaner_name
        self.cleaner_version = cleaner_version
        no_suffix_name = cleaner_name.replace("_cleaner", "") if "_cleaner" in cleaner_name else cleaner_name
        self.cleaner_config: Dict[str, Any] = self.config.get("cleaners").get(no_suffix_name)
        self.logger = get_logger(__name__)

        # DAOs
        self.cleaned_data_dao = CleanedDataDAO()
        self.validation_statuses_dao = ValidationStatusesDAO()

        # UTILS
        self.dynamic_schema_utils = DynamicSchemaUtils(
            name=self.cleaner_name,
            version=self.cleaner_version,
            config=self.cleaner_config,
            metadata_schema_dao_class=CleanedDataMetadataSchemasDAO(),
        )

        # VALIDATION STATUS CACHING
        self._validation_status_cache = self._load_validation_statuses()

        self.logger.info(f"Cleaning Service initialized for {cleaner_name} v{cleaner_version}")

    def _load_validation_statuses(self) -> Dict[str, int]:
        """Loads and caches validation status ids"""
        try:
            statuses = self.validation_statuses_dao.get_all()
            return {status.validation_status_name: status.id for status in statuses if status.id}
        except Exception as general_error:
            self.logger.error(f"Failed to load validation statuses: {general_error}")
            return {}

    @handle_generic_errors_gracefully("while getting metadata schema ID", None)
    def get_metadata_schema_id(self) -> Optional[int]:
        """Convenience method to get metadata schema ID"""
        return self.dynamic_schema_utils.get_metadata_schema_id()

    @handle_generic_errors_gracefully("while handling schema management", None)
    def handle_schema_management(
        self,
        cleaned_metadata: Dict[str, Any],
    ) -> None:
        """Convenience method to handle schema generation and caching logic"""
        self.dynamic_schema_utils.handle_schema_management(cleaned_metadata)

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

    def validate_cleaned_content(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Convenience method to validate cleaned content"""
        return self.dynamic_schema_utils.validate_cleaned_content(cleaned_data)

    def reload_schema_from_database(self) -> bool:
        """Convenience method to reload schema from database"""
        return self.dynamic_schema_utils.reload_schema_from_database()

    def get_schema_info(self) -> Dict[str, Any]:
        """Convenience method to get schema info"""
        return self.dynamic_schema_utils.get_schema_info()

    def save_cleaned_content(
        self,
        raw_data: RawData,
        cleaned_metadata: Dict[str, Any],
        is_valid: bool,
        validation_error: Optional[Dict[str, Any]],
        cleaning_time_ms: int,
    ) -> Optional[int]:
        """Saves cleaned content"""
        try:
            validation_status_id: int = self.get_validation_status_id("valid" if is_valid else "invalid")
            schema_id = self.dynamic_schema_utils.get_metadata_schema_id()
            if not schema_id:
                self.logger.error(f"No metadata schema id available for {self.cleaner_name}")
                return None

            cleaned_data_id: Optional[int] = self.cleaned_data_dao.create_cleaned_data(
                raw_data_id=raw_data.id,
                cleaned_data_metadata_schema_id=schema_id,
                title=raw_data.title,
                language_code=raw_data.language_code,
                cleaner_used=self.cleaner_name,
                cleaner_version=self.cleaner_version,
                cleaning_time_ms=cleaning_time_ms,
                url=raw_data.url,
                metadata=cleaned_metadata,
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
    ) -> Optional[int]:
        """Save error information when cleaning fails"""
        try:
            schema_id = self.dynamic_schema_utils.get_metadata_schema_id()
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
                cleaner_used=self.cleaner_name,
                cleaner_version=self.cleaner_version,
                cleaning_time_ms=cleaning_time_ms,
                cleaned_at=datetime.now(),
            )

        except Exception as general_error:
            self.logger.error(f"Failed to save error information: {general_error}")
            return None
