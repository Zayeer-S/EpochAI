from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.collection_attempts_dao import CollectionAttemptsDAO
from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.raw_data_dao import RawDataDAO
from epochai.common.database.dao.raw_data_metadata_schemas_dao import RawDataMetadataSchemasDAO
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger
from epochai.common.utils.data_utils import DataUtils


class WikipediaSaver:
    def __init__(self):
        try:
            self.logger = get_logger(__name__)

            self.data_config = ConfigLoader.get_data_config()
            self.data_utils = DataUtils(self.data_config)

            self.save_to_database = (
                self.data_config.get("data_output").get("database").get("save_to_database")
            )
            if self.save_to_database:
                self.collection_attempts_dao = CollectionAttemptsDAO()
                self.raw_data_dao = RawDataDAO()
                self.collection_targets_dao = CollectionTargetsDAO()
                self.collection_statuses_dao = CollectionStatusesDAO()
                self.collection_types_dao = CollectionTypesDAO()
                self.metadata_schemas_dao = RawDataMetadataSchemasDAO()

                self.ATTEMPT_STATUS_ID = 1
                self.VALIDATION_STATUS_ID = 1
                self.METADATA_SCHEMA_ID = 1

        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}") from import_error

        except Exception as general_error:
            if self.logger:
                self.logger.error(f"General error in __init__: {general_error}")
            else:
                print(f"General error in __init__ and self.logger returning false: {general_error}")

    def _build_metadata_schema(
        self,
        schema_id: int,
        collected_item: Dict[str, Any],
        language_code: str,
    ) -> Dict[str, Any]:
        """Create metadata schema based off of database's"""

        try:
            schema_obj = self.metadata_schemas_dao.get_by_id(schema_id)
            if not schema_obj or not schema_obj.metadata_schema:
                self.logger.warning(f"No metadata schema found for ID {schema_id}")
                return {}

            schema_properties = schema_obj.metadata_schema.get("properties", {})
            metadata = {}

            # TODO: Change this to dynamic and move to data_util
            for property_name, property_config in schema_properties.items():
                if property_name in collected_item:
                    metadata[property_name] = collected_item[property_name]
                elif property_name == "language":
                    metadata[property_name] = language_code
                elif property_name == "page_id" and "pageid" in collected_item:
                    metadata[property_name] = collected_item["pageid"]
                elif property_name == "word_count":
                    content = collected_item.get("content", "")
                    if content:
                        metadata[property_name] = len(content.split())
                    else:
                        metadata[property_name] = 0
                elif property_name == "last_modified" and "timestamp" in collected_item:
                    metadata[property_name] = collected_item["timestamp"]
                else:
                    # Handle the missing optional fields based on schema
                    property_type = property_config.get("type")
                    if property_name in schema_obj.metadata_schema.get("required", []):
                        self.logger.warning(f"Required field '{property_name}' missing from collected item")
                        if property_type == "string":
                            metadata[property_name] = ""
                        elif property_type == "integer":
                            metadata[property_name] = 0
                        elif property_type == "array":
                            metadata[property_name] = []

            return metadata

        except Exception as general_error:
            self.logger.error(f"Error building metadata schema: {general_error}")
            return {}

    def save_locally_at_end(
        self,
        collected_data: List[Dict[str, Any]],
        data_type: str,
    ) -> Optional[str]:
        return self.data_utils.save_at_end(
            collected_data=collected_data,
            data_type=data_type,
        )

    def save_incrementally_to_database(
        self,
        collected_data: List[Dict[str, Any]],
        collection_target_id: int,
        language_code: str,
    ) -> Optional[int]:
        self.logger.info(
            f"Saving {len(collected_data)} Wikipedia articles to database for target {collection_target_id}...",  # noqa
        )

        success_count = 0

        for item in collected_data:
            title = item.get("title")
            content = item.get("content")
            url = item.get("url")

            if not title or not content:
                self.logger.warning(f"Skipping item due to missing title or content: {item}")
                continue

            try:
                attempt_id = self.collection_attempts_dao.create_attempt(
                    collection_target_id=collection_target_id,
                    language_code=language_code,
                    search_term_used=title,
                    attempt_status_id=self.ATTEMPT_STATUS_ID,
                    error_type_id=None,
                    error_message="",
                )

                if not attempt_id:
                    self.logger.error(
                        f"Failed to create attempt for '{title}' - Not saving metadata for this",
                    )
                    continue

                metadata = self._build_metadata_schema(
                    schema_id=self.METADATA_SCHEMA_ID,
                    collected_item=item,
                    language_code=language_code,
                )

                content_id = self.raw_data_dao.create_raw_data(
                    collection_attempt_id=attempt_id,
                    raw_data_metadata_schema_id=self.METADATA_SCHEMA_ID,
                    title=title,
                    language_code=language_code,
                    url=url,
                    metadata=metadata,
                    validation_status_id=self.VALIDATION_STATUS_ID,
                    validation_error=None,
                    filepath_of_save="",
                )

                if not content_id:
                    self.logger.error(f"Failed to insert content for '{title}'")
                else:
                    success_count += 1
                    self.logger.info(f"Successfully saved '{title}' to database with metadata")

            except Exception as general_error:
                self.logger.error(f"Database error while saving '{title}': {general_error}")

        if success_count > 0:
            mark_as_collected = self.collection_statuses_dao.update_collection_status(
                collection_target_id,
                CollectionStatusNames.COLLECTED.value,
            )
            if mark_as_collected:
                self.logger.info(
                    f"Marked target {collection_target_id} as collected after saving {success_count} items",
                )
            else:
                self.logger.error(f"Failed to mark target {collection_target_id} as collected")

            self.logger.info(
                f"Successfully saved {success_count} of {len(collected_data)} Wikipedia articles to the database",  # noqa
            )
            return success_count

        return None

    def log_data_summary(
        self,
        collected_data: List[Dict[str, Any]],
    ) -> None:
        self.data_utils.log_data_summary(collected_data)

    def get_collection_target_id(
        self,
        collection_type: str,
        language_code: str,
        collection_name: str,
    ) -> Optional[int]:
        """Gets the collection_target_id for the current collection"""
        try:
            status_id = self.collection_statuses_dao.get_id_by_name(CollectionStatusNames.NOT_COLLECTED.value)
            if not status_id:
                raise ValueError(
                    f"Failed to get status_id for type '{collection_type}' and name '{collection_name}' in '{language_code}'",  # noqa: E501
                )

            type_obj = self.collection_types_dao.get_by_name(collection_type)
            if not type_obj:
                raise ValueError(
                    f"Failed to get collection_type_id for type '{collection_type}' and name '{collection_name}' in '{language_code}'",  # noqa: E501
                )
            type_id: int = type_obj.id

            targets = self.collection_targets_dao.get_by_type_and_language(
                type_id,
                language_code,
                status_id,
            )

            for target in targets:
                if target.collection_name == collection_name:
                    self.logger.debug(f"Found exact target match id {target.id} for name {collection_name}")
                    return target.id

            self.logger.warning(f"No existing target found for '{collection_name}'")
            return None

        except Exception as e:
            self.logger.error(f"Error getting collection target id: {e}")
            return None
