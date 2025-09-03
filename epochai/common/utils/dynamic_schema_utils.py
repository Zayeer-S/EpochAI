from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from genson import SchemaBuilder
import jsonschema
from jsonschema import ValidationError

from epochai.common.logging_config import get_logger
from epochai.common.protocols.metadata_schema_dao_protocol import MetadataSchemaDAOProtocol
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class DynamicSchemaUtils:
    @handle_initialization_errors(f"{__name__} initialization")
    def __init__(
        self,
        name: str,
        version: str,
        config: Any,
        metadata_schema_dao_class: MetadataSchemaDAOProtocol,
    ):
        self._logger = get_logger(__name__)

        # GET CONFIG VALUES
        self._schema_cache_limit = int(config.get("schema_cache_limit"))
        self._schema_check_interval = int(config.get("schema_check_interval"))

        # SET PARAMETERS TO INSTANCE VARS
        self._name = name
        self._version = version
        self._dao = metadata_schema_dao_class

        # SCHEMA MANAGEMENT
        self._metadata_schema_cache: Optional[Dict[str, Any]] = None
        self._schema_validator: Optional[jsonschema.protocols.Validator] = None
        self._metadata_schema_id: Optional[int] = None
        self._temp_schemas: List[Dict[str, Any]] = []

        # SCHEMA MANAGEMENT CHECKS
        self._schema_generation_count: int = 0
        self._records_processed_count: int = 0

        # LOAD EXISTING METADATA SCHEMA
        self._load_schema_from_database()

        self._logger.debug(f"{__name__} Initialized")

    @handle_generic_errors_gracefully("while loading schema from database", None)
    def _load_schema_from_database(self) -> None:
        """Load current metadata schema ffrom the the database"""
        all_schemas = self._dao.get_all()

        for each_schema in all_schemas:
            schema_content = each_schema.metadata_schema
            if (
                isinstance(schema_content, dict)
                and schema_content.get("cleaner_name") == self._name
                and schema_content.get("cleaner_version") == self._version
            ):
                self._metadata_schema_cache = schema_content
                self._metadata_schema_id = each_schema.id

                self._create_validator_using_schema(schema_content)

                self._logger.info(f"Using Schema with ID '{self._metadata_schema_id} from Database'")
                return

        self._logger.info("No Schema found in Database")

    @handle_generic_errors_gracefully("during schema validator creation", None)
    def _create_validator_using_schema(
        self,
        schema_content: Dict[str, Any],
    ) -> None:
        """Creates JSON schema validator using database schema"""
        only_schema = schema_content.get("schema")
        self._schema_validator = jsonschema.Draft7Validator(only_schema)

    @handle_generic_errors_gracefully("during initial schema generation via metadata", {})
    def _generate_initial_schema_from_metadata(
        self,
        metadata: Dict[str, Any],
    ) -> Dict[str, Any]:
        """Generates initial schema using metadata of initial collections"""
        builder = SchemaBuilder()
        builder.add_object(metadata)
        generated_schema = builder.to_schema()

        schema_with_metadata = {
            "cleaner_name": self._name,
            "cleaner_version": self._version,
            "generated_at": datetime.now().isoformat(),
            "schema": generated_schema,
        }

        return schema_with_metadata

    def _should_cache_schema(
        self,
        new_schema: Dict[str, Any],
    ) -> bool:
        """Determines if we should cache schema if we reach enough consecutive schemas"""

        self._temp_schemas.append(new_schema)

        if len(self._temp_schemas) < self._schema_cache_limit:
            return False

        self._temp_schemas = self._temp_schemas[-self._schema_cache_limit :]

        first_schema = self._temp_schemas[0].get("schema", {})  # get schema structure but set contents as null
        return all(schema.get("schema", {}) == first_schema for schema in self._temp_schemas)

    @handle_generic_errors_gracefully("while caching schema to database", None)
    def _cache_schema_to_database(
        self,
        new_schema: Dict[str, Any],
    ) -> Optional[int]:
        """
        Caches schema to database

        Note:
            Can create new schema in database. This is a fallback for if automatic python schema building
            makes a schema not found in the database.
        """

        existing_schema = self._dao.find_schema_by_content(new_schema)
        if existing_schema and existing_schema.id:
            self._logger.info(f"Schema already exists in the database (ID: {existing_schema.id})")
            return existing_schema.id

        new_schema_id = self._dao.create_schema(new_schema)
        if new_schema_id:
            self._logger.info(f"Cached new schema to database (id: {new_schema_id})")
            return new_schema_id

        self._logger.error("Failed to cache schema to database")
        return None

    @handle_generic_errors_gracefully("while checking if schema's been updated in database", False)
    def _check_for_schema_updates(self) -> bool:
        """Checks if schema has been updated in database and reloads if necessary"""
        current_schema_id = self._metadata_schema_id
        all_schemas = self._dao.get_all()

        for each_schema in all_schemas:
            schema_content = each_schema.metadata_schema
            if (
                isinstance(schema_content, dict)
                and schema_content.get("cleaner_name") == self._name
                and schema_content.get("cleaner_version") == self._version
                and each_schema.id != current_schema_id
            ):
                self._logger.info(f"Found updated schema (id: {each_schema.id}), reloading...")

                self._metadata_schema_id = each_schema.id
                self._metadata_schema_cache = schema_content
                self._create_validator_using_schema(schema_content)

                self._logger.info(f"Schema updated: {current_schema_id} -> {each_schema.id}")
                return True

        return False

    def _validate_with_json_schema(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates using JSON schema"""
        try:
            self._schema_validator.validate(cleaned_data)
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
                validation_errors.append(f"Missing required field: {field}")
            elif not cleaned_data[field]:
                validation_errors.append(f"Empty required field: {field}")

        if "cleaned_content" in cleaned_data:
            content = cleaned_data["cleaned_content"]
            if len(content.strip()) < 10:
                validation_errors.append(f"Content too short: {len(content)} char")

        is_valid = len(validation_errors) == 0
        error_dict = {"validation_errors": validation_errors} if validation_errors else None

        return is_valid, error_dict

    def get_metadata_schema_id(self) -> Optional[int]:
        if self._metadata_schema_id is None:
            self._logger.error("No metadata schema id available - check schema generation logs")
        return self._metadata_schema_id

    def handle_schema_management(
        self,
        cleaned_metadata: Dict[str, Any],
    ) -> None:
        """Handles schema generation and caching logic"""

        self._records_processed_count += 1
        if self._records_processed_count % self._schema_check_interval == 0:
            self._logger.info(
                f"Checking for schema updates (processed {self._records_processed_count})",
            )
            schema_updated = self._check_for_schema_updates()
            if schema_updated:
                self._logger.info("Schema was updated from database")

        if self._metadata_schema_cache is None:
            generated_schema = self._generate_initial_schema_from_metadata(cleaned_metadata)

            if self._should_cache_schema(generated_schema):
                schema_id = self._cache_schema_to_database(generated_schema)
                if schema_id:
                    self._metadata_schema_cache = generated_schema
                    self._metadata_schema_id = schema_id
                    self._create_validator_using_schema(generated_schema)
                else:
                    raise RuntimeError("Failed to cache schema to database")
            else:
                self._logger.info(
                    f"Schema generation in progress ({len(self._temp_schemas)} out of {self._schema_cache_limit} matches)",
                )

    def validate_cleaned_content(
        self,
        cleaned_data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates cleaned wikipedia content"""
        if self._schema_validator is not None:
            return self._validate_with_json_schema(cleaned_data)
        self._logger.info("Falling back to basic requirement validator")
        return self._validate_basic_requirements(cleaned_data)

    def reload_schema_from_database(self) -> bool:
        """Reload schema from database (call this when it changes externally)"""
        try:
            old_schema_id = self._metadata_schema_id
            self._metadata_schema_cache = None
            self._schema_validator = None
            self._metadata_schema_id = None

            self._load_schema_from_database()

            if self._metadata_schema_id != old_schema_id:
                self._logger.info(f"Schema reloaded: {old_schema_id} -> {self._metadata_schema_id}")
                return True
            return False

        except Exception as general_error:
            self._logger.error(f"Error reloading schema: {general_error}")
            return False

    def get_schema_info(self) -> Dict[str, Any]:  # TODO DELETE
        """Gets info about the current schema"""
        return {
            "schema_cached": self._metadata_schema_cache is not None,
            "schema_id": self._metadata_schema_id,
            "validator_available": self._schema_validator is not None,
            "temp_schemas_count": len(self._temp_schemas),
            "cleaner_name": self._name,
            "cleaner_version": self._version,
            "using_json_schema_validation": self._schema_validator is not None,
            "records_processed": self._records_processed_count,
            "schema_check_interval": self._schema_check_interval,
            "next_schema_check_at": self._schema_check_interval - (self._records_processed_count % self._schema_check_interval),
        }
