from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from genson import SchemaBuilder
import jsonschema
from jsonschema import ValidationError

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.cleaned_data_dao import CleanedDataDAO
from epochai.common.database.dao.cleaned_data_metadata_schemas_dao import CleanedDataMetadataSchemasDAO
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
        # CONFIG
        self.config = ConfigLoader.get_data_config()

        # BASIC
        self.cleaner_name = cleaner_name
        self.cleaner_version = cleaner_version
        no_suffix_name = cleaner_name.replace("_cleaner", "") if "_cleaner" in cleaner_name else cleaner_name
        self.cleaner_config: Dict[str, Any] = self.config.get("cleaners").get(no_suffix_name)
        self.schema_cache_limit = int(self.cleaner_config.get("schema_cache_limit"))
        self.schema_check_interval = int(self.cleaner_config.get("schema_check_interval"))
        self.logger = get_logger(__name__)

        # DAOs
        self.raw_data_dao = RawDataDAO()
        self.cleaned_data_dao = CleanedDataDAO()
        self.cleaned_data_metadata_schema_dao = CleanedDataMetadataSchemasDAO()
        self.validation_statuses_dao = ValidationStatusesDAO()

        # VALIDATION STATUS CACHING
        self._validation_status_cache = self._load_validation_statuses()

        # SCHEMA MANAGEMENT
        self.metadata_schema_cache: Optional[Dict[str, Any]] = None
        self.schema_validator: Optional[jsonschema.protocols.Validator] = None
        self.metadata_schema_id: Optional[int] = None
        self.temp_schemas: List[Dict[str, Any]] = []

        # SCHEMA MANAGEMENT CHECKS
        self.schema_generation_count: int = 0
        self.records_processed_count: int = 0

        # LOAD EXISTING METADATA SCHEMA
        self._load_schema_from_database()

        self.logger.info(f"Cleaning Service initialized for {cleaner_name} v{cleaner_version}")

    def _load_validation_statuses(self) -> Dict[str, int]:
        """Loads and caches validation status ids"""
        try:
            statuses = self.validation_statuses_dao.get_all()
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

    def _load_schema_from_database(self) -> None:
        """Load current metadata schema ffrom the the database"""

        try:
            all_schemas = self.cleaned_data_metadata_schema_dao.get_all()

            for each_schema in all_schemas:
                schema_content = each_schema.metadata_schema
                if (
                    isinstance(schema_content, dict)
                    and schema_content.get("cleaner_name") == self.cleaner_name
                    and schema_content.get("cleaner_version") == self.cleaner_version
                ):
                    self.metadata_schema_cache = schema_content
                    self.metadata_schema_id = each_schema.id

                    self._create_validator_using_schema(schema_content)

                    self.logger.info(f"Loaded existing schema from database (id: {self.metadata_schema_id})")
                    return

            self.logger.info("No existing schema found in database")

        except Exception as general_error:
            self.logger.error(f"Error loading schema from database: {general_error}")

        return

    def _create_validator_using_schema(
        self,
        schema_content: Dict[str, Any],
    ) -> None:
        """Creates json schema validator using database schema"""
        try:
            only_schema = schema_content.get("schema")

            self.schema_validator = jsonschema.Draft7Validator(only_schema)

        except Exception as general_error:
            self.logger.error(f"Error creating validator from schema: {general_error}")
            self.schema_validator = None

    def _generate_initial_schema_from_metadata(
        self,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        builder = SchemaBuilder()
        builder.add_object(metadata)
        generated_schema = builder.to_schema()

        schema_with_metadata = {
            "cleaner_name": self.cleaner_name,
            "cleaner_version": self.cleaner_version,
            "generated_at": datetime.now().isoformat(),
            "schema": generated_schema,
        }

        return schema_with_metadata

    def _should_cache_schema(
        self,
        new_schema: Dict[str, Any],
    ) -> bool:
        """Determines if we should cache schema if we reach enough consecutive schemas"""

        self.temp_schemas.append(new_schema)

        if len(self.temp_schemas) < self.schema_cache_limit:
            return False

        self.temp_schemas = self.temp_schemas[-self.schema_cache_limit :]

        first_schema = self.temp_schemas[0].get("schema", {})  # get schema structure but set contents as null
        return all(schema.get("schema", {}) == first_schema for schema in self.temp_schemas)

    def _cache_schema_to_database(
        self,
        new_schema: Dict[str, Any],
    ) -> Optional[int]:
        """
        Caches schema to database

        Note:
            Can create new schema in database. Fallback for if automatic python schema building makes
            a schema not found in the database.
        """

        try:
            existing_schema = self.cleaned_data_metadata_schema_dao.find_schema_by_content(new_schema)
            if existing_schema and existing_schema.id:
                self.logger.info(f"Schema already exists in the database (id: {existing_schema.id})")
                return existing_schema.id

            new_schema_id = self.cleaned_data_metadata_schema_dao.create_schema(new_schema)
            if new_schema_id:
                self.logger.info(f"Cached new schema to database (id: {new_schema_id})")
                return new_schema_id

            self.logger.error("Failed to cache schema to database")
            return None

        except Exception as general_error:
            self.logger.error(f"Error caching schema to database: {general_error}")
            return None

    def _check_for_schema_updates(self) -> bool:
        """Checks if schema has been updated in database and reloads if necessary"""
        try:
            current_schema_id = self.metadata_schema_id
            all_schemas = self.cleaned_data_metadata_schema_dao.get_all()

            for each_schema in all_schemas:
                schema_content = each_schema.metadata_schema
                if (
                    isinstance(schema_content, Dict)
                    and schema_content.get("cleaner_name") == self.cleaner_name
                    and schema_content.get("cleaner_version") == self.cleaner_version
                    and each_schema.id != current_schema_id
                ):
                    self.logger.info(f"Found updated schema (id: {each_schema.id}), reloading...")

                    self.metadata_schema_id = each_schema.id
                    self.metadata_schema_cache = schema_content
                    self._create_validator_using_schema(schema_content)

                    self.logger.info(f"Schema updated: {current_schema_id} -> {each_schema.id}")
                    return True

            return False

        except Exception as general_error:
            self.logger.error(f"Error checking for schema updates: {general_error}")
            return False

    def _validate_with_json_schema(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates using JSON schema"""

        try:
            self.schema_validator.validate(cleaned_data)
            return True, None

        except ValidationError as validation_error:
            validation_errors: List[Any] = []

            validation_errors.append(f"Schema validation failed: {validation_error.message}")

            if validation_error.absolute_path:
                field_path = ".".join(str(p) for p in validation_error.absolute_path)
                validation_errors.append(f"Field path: {field_path}")

            for sub_error in validation_error.context:
                validation_errors.append(f"Sub error: {sub_error.message}")

            error_dict = {
                "validation_errors": validation_errors,
                "schema_validation_error": str(validation_error),
                "failed_value": validation_error.instance if hasattr(validation_error, "instance") else None,
            }

            return False, error_dict

        except Exception as general_error:
            error_dict = {
                "validation_errors": [f"Validation system error: {general_error!s}"],
                "validation_system_error": True,
            }
            return False, error_dict

    def _validate_basic_requirements(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Basic validation used during schema generation phase"""
        validation_errors = []

        required_fields = ["cleaned_content", "cleaned_title", "language", "page_id"]

        for field in required_fields:
            if field not in cleaned_data:
                validation_errors.append(f"Missing required field(s): {field}")
            elif not cleaned_data[field]:
                validation_errors.append(f"Empty required field(s): {field}")

        if "cleaned_content" in cleaned_data:
            content = cleaned_data["cleaned_content"]
            if len(content.strip()) < 10:
                validation_errors.append(f"Content too short: {len(content)} char")

        is_valid = len(validation_errors) == 0
        error_dict = {"validation_errors": validation_errors} if validation_errors else None

        return is_valid, error_dict

    def get_metadata_schema_id(self) -> Optional[int]:
        if self.metadata_schema_id is None:
            self.logger.error("No metadata schema id available - check schema generation logs")
        return self.metadata_schema_id

    def handle_schema_management(
        self,
        cleaned_metadata: Dict[str, Any],
    ) -> None:
        """Handles schema generation and caching logic"""

        self.records_processed_count += 1
        if self.records_processed_count % self.schema_check_interval == 0:
            self.logger.info(
                f"Checking for schema updates (processed {self.records_processed_count} records)",
            )
            schema_updated = self._check_for_schema_updates()
            if schema_updated:
                self.logger.info("Schema was updated from database")

        if self.metadata_schema_cache is None:
            generated_schema = self._generate_initial_schema_from_metadata(cleaned_metadata)

            if self._should_cache_schema(generated_schema):
                schema_id = self._cache_schema_to_database(generated_schema)
                if schema_id:
                    self.metadata_schema_cache = generated_schema
                    self.metadata_schema_id = schema_id
                    self._create_validator_using_schema(generated_schema)
                else:
                    raise RuntimeError("Failed to cache schema to database")
            else:
                self.logger.info(f"Schema generation in progress ({len(self.temp_schemas)} out of 3 matches)")

    def validate_cleaned_content(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates cleaned wikipedia content"""

        if self.schema_validator is not None:
            return self._validate_with_json_schema(cleaned_data)
        self.logger.info("Falling back to basic requirement validator")
        return self._validate_basic_requirements(cleaned_data)

    def save_cleaned_content(
        self,
        raw_data: RawData,
        cleaned_metadata: Dict[str, Any],
        is_valid: bool,
        validation_error: Optional[Dict[str, Any]],
        cleaning_time_ms: int,
    ) -> Optional[int]:
        try:
            validation_status_id: int = self.get_validation_status_id("valid" if is_valid else "invalid")
            schema_id = self.get_metadata_schema_id()
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
            schema_id = self.get_metadata_schema_id()
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

    def reload_schema_from_database(self) -> bool:
        """Reload schema from database (call this when it changes externally)"""
        try:
            old_schema_id = self.metadata_schema_id
            self.metadata_schema_cache = None
            self.schema_validator = None
            self.metadata_schema_id = None

            self._load_schema_from_database()

            if self.metadata_schema_id != old_schema_id:
                self.logger.info(f"Schema reloaded: {old_schema_id} -> {self.metadata_schema_id}")
                return True
            return False

        except Exception as general_error:
            self.logger.error(f"Error reloading schema: {general_error}")
            return False

    def get_schema_info(self) -> Dict[str, Any]:
        """Gets info about the current schema"""
        return {
            "schema_cached": self.metadata_schema_cache is not None,
            "schema_id": self.metadata_schema_id,
            "validator_available": self.schema_validator is not None,
            "temp_schemas_count": len(self.temp_schemas),
            "cleaner_name": self.cleaner_name,
            "cleaner_version": self.cleaner_version,
            "using_json_schema_validation": self.schema_validator is not None,
            "records_processed": self.records_processed_count,
            "schema_check_interval": self.schema_check_interval,
            "next_schema_check_at": self.schema_check_interval - (self.records_processed_count % self.schema_check_interval),
        }
