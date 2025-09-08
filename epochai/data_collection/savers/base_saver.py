from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.enums import AttemptStatusNames, CollectionStatusNames, ValidationStatusNames
from epochai.common.logging_config import get_logger
from epochai.common.services.collection_attempts_service import CollectionAttemptsService
from epochai.common.services.raw_data_service import RawDataService
from epochai.common.services.target_status_management_service import TargetStatusManagementService
from epochai.common.utils.data_utils import DataUtils
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors
from epochai.common.utils.schema_utils import SchemaUtils


class BaseSaver(ABC):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(
        self,
        collector_name: str,
        collector_version: str,
    ):
        # BASIC
        self._logger = get_logger(__name__)

        self._data_config = ConfigLoader.get_data_config()
        self._data_utils = DataUtils(self._data_config)

        self._validate_before_save = self._data_config.get("data_validator").get("validate_before_save")
        self._save_to_database = self._data_config.get("data_output").get("database").get("save_to_database")

        if self._save_to_database:
            # SERVICES
            self._target_status_management_service = TargetStatusManagementService()
            self._collection_attempts_service = CollectionAttemptsService()
            self._raw_data_service = RawDataService()

            # ASSIGN PARAMS TO INSTANCE VARS
            self._collector_name = collector_name
            self._collector_version = collector_version

            # UTILS
            from epochai.common.database.dao.raw_data_metadata_schemas_dao import RawDataMetadataSchemasDAO

            self._schema_utils = SchemaUtils(
                name=self._collector_name,
                version=self._collector_version,
                metadata_schema_dao_class=RawDataMetadataSchemasDAO(),
                schema_name_field="collector_name",
                schema_version_field="current_schema_version",
                custom_validation_function=self._get_custom_validation_function,
            )

            self._logger.debug(f"Initialized {self.__class__.__name__} for {collector_name} v{collector_version}")

        else:
            self._logger.debug(f"Initialized {self.__class__.__name__} for {collector_name} in Test Mode")

    @abstractmethod
    def _get_custom_validation_function(self) -> Any:
        """Returns the custom validatio function for this data source"""
        raise NotImplementedError(f"Subclasses must implement {self._get_custom_validation_function.__name__} function")

    @abstractmethod
    def _prepare_metadata_for_storage(
        self,
        collected_item: Dict[str, Any],
        language_code: str,
    ) -> Dict[str, Any]:
        """Prepare collected item for storage with proper field mapping"""
        raise NotImplementedError(f"Subclasses must implement {self._prepare_metadata_for_storage.__name__} function")

    def save_locally_at_end(
        self,
        collected_data: List[Dict[str, Any]],
        data_type: str,
    ) -> Optional[str]:
        """Convenience method to save collected data to local file at the end of a collection"""
        return self._data_utils.save_at_end(
            collected_data=collected_data,
            data_type=data_type,
        )

    def log_data_summary(
        self,
        collected_data: List[Dict[str, Any]],
    ) -> None:
        """Convenience method to log summary statistics of collected data"""
        self._data_utils.log_data_summary(collected_data)

    @handle_generic_errors_gracefully("while creating raw data record", None)
    def _create_raw_data_record(
        self,
        attempt_id: int,
        item: Dict[str, Any],
        language_code: str,
        metadata: Dict[str, Any],
        validation_status_name: str,
        validation_error: Optional[Dict[str, Any]],
    ) -> Optional[int]:
        """Create a raw data record in the database"""

        schema_id = self._schema_utils.get_metadata_schema_id()
        if not schema_id:
            self._logger.error(f"No schema ID available for '{item.get('title', 'unknown')}', skipping...")
            return None

        content_id = self._raw_data_service.create_raw_data(
            collection_attempt_id=attempt_id,
            raw_data_metadata_schema_id=schema_id,
            item=item,
            language_code=language_code,
            metadata=metadata,
            validation_status_name=validation_status_name,
            validation_error=validation_error,
            filepath_of_save=None,
        )

        return content_id

    @handle_generic_errors_gracefully("while validating content", None)
    def _validate_content(self, metadata) -> Tuple[str, Optional[Dict[str, Any]]]:
        is_valid, validation_error = self._schema_utils.validate_content(metadata)

        if is_valid:
            validation_status_name = ValidationStatusNames.VALID.value
        else:
            validation_status_name = ValidationStatusNames.INVALID.value
            self._logger.warning(f"Validation failed for '{metadata.get('title', 'failed to get title')}': {validation_error}")

        return validation_status_name, validation_error

    @handle_generic_errors_gracefully("while processing and saving a single item to the database", None)
    def _process_single_item(
        self,
        item: Dict[str, Any],
        collection_target_id: int,
        language_code: str,
    ) -> Optional[int]:
        """Process a single collected item and save to the database"""

        title, content = item.get("title"), item.get("content")
        if not title or not content:
            self._logger.error(f"Skipping due to missing title or content in item: {item}")
            return None

        attempt_id = self._collection_attempts_service.create_collection_attempt(
            item=item,
            collection_target_id=collection_target_id,
            language_code=language_code,
            new_status_name=AttemptStatusNames.SUCCESS.value,
        )

        if not attempt_id:
            self._logger.error(f"Failed to create attempt for '{title}' - Not saving metadata for this")
            return None

        metadata = self._prepare_metadata_for_storage(collected_item=item, language_code=language_code)

        if self._validate_before_save:
            validation_status_name, validation_error = self._validate_content(metadata)
        else:
            validation_status_name, validation_error = ValidationStatusNames.PENDING.value, None

        content_id = self._create_raw_data_record(
            attempt_id=attempt_id,
            item=item,
            language_code=language_code,
            metadata=metadata,
            validation_status_name=validation_status_name,
            validation_error=validation_error,
        )

        if content_id:
            self._logger.info(f"Successfully saved '{title}' to database with status '{validation_status_name}'")
            return content_id

        self._logger.error(f"Failed to save '{title}' to database")
        return None

    @handle_generic_errors_gracefully("while saving to database incrementally", None)
    def save_incrementally_to_database(
        self,
        collected_data: List[Dict[str, Any]],
        collection_target_id: int,
        language_code: str,
    ) -> Optional[int]:
        """
        Save collected data incrementally to database

        Returns:
            Number of successfully saved items, or None if failed
        """
        if not self._save_to_database:
            self._logger.warning("Database saving is disabled in config.yml")
            return None

        self._logger.info(f"Saving {len(collected_data)} items to database for target {collection_target_id}...")

        success_count = 0

        for item in collected_data:
            content_id = self._process_single_item(item, collection_target_id, language_code)
            if content_id:
                success_count += 1

        new_status_name = CollectionStatusNames.COLLECTED.value if success_count else CollectionStatusNames.FAILED.value
        update_mark = self._target_status_management_service.update_target_collection_status(
            collection_target_id=collection_target_id,
            collection_status_name=new_status_name,
        )

        if update_mark:
            self._logger.info(f"Marked target {collection_target_id} as {new_status_name}")
        else:
            self._logger.error(f"Failed to mark {collection_target_id} as {new_status_name}")

        return success_count if success_count else None
