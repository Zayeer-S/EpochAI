from typing import Any, Dict, List, Optional, Tuple

import jsonschema
from jsonschema import ValidationError

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.logging_config import get_logger
from epochai.common.protocols.metadata_schema_dao_protocol import MetadataSchemaDAOProtocol
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class SchemaUtils:
    @handle_initialization_errors(f"{__name__} initialization")
    def __init__(
        self,
        name: str,
        version: str,
        metadata_schema_dao_class: MetadataSchemaDAOProtocol,
        schema_name_field: str,
        schema_version_field: str,
    ):
        self._logger = get_logger(__name__)

        # GET CONFIG VALUESs
        self._schema_config = ConfigLoader.get_metadata_schema_config()

        # SET PARAMETERS TO INSTANCE VARS
        self._name = name
        self._version = version
        self._dao = metadata_schema_dao_class
        self._schema_name_field = schema_name_field
        self._schema_version_field = schema_version_field

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
        """Load current metadata schema from the the database"""
        all_schemas = self._dao.get_all()

        for each_schema in all_schemas:
            schema_content = each_schema.metadata_schema
            if (
                isinstance(schema_content, dict)
                and schema_content.get(self._schema_name_field) == self._name
                and schema_content.get(self._schema_version_field) == self._version
            ):
                self._metadata_schema_cache = schema_content
                self._metadata_schema_id = each_schema.id

                self._create_validator_using_schema(schema_content)

                self._logger.info(f"Using Schema with ID '{self._metadata_schema_id} from Database'")
                return

        raise ValueError(f"No schema found for {self._name} v{self._version}")

    @handle_generic_errors_gracefully("during schema validator creation", None)
    def _create_validator_using_schema(
        self,
        schema_content: Dict[str, Any],
    ) -> None:
        """Creates JSON schema validator using database schema"""
        only_schema = schema_content.get("schema")
        if not only_schema:
            raise ValueError("Schema content missing 'schema' section")
        self._schema_validator = jsonschema.Draft7Validator(only_schema)

    def _validate_with_json_schema(
        self,
        data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates using JSON schema"""
        try:
            self._schema_validator.validate(data)
            return True, None

        except ValidationError as validation_error:
            validation_errors: List[str] = []

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

    @handle_generic_errors_gracefully("while getting metadata schema ID", None)
    def get_metadata_schema_id(self) -> Optional[int]:
        if self._metadata_schema_id is None:
            self._logger.error("No metadata schema id available - schema not loaded!")
        return self._metadata_schema_id

    def validate_content(
        self,
        data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates content using schema or passed in function (if available)"""
        if self._schema_validator is not None:
            return self._validate_with_json_schema(data)
        error_dict = {
            "validation_errors": ["No JSON schema validator available"],
            "validation_system_error": True,
        }
        return False, error_dict

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

    def get_schema_info(self) -> Dict[str, Any]:
        """Gets info about the current schema"""
        return {
            "schema_cached": self._metadata_schema_cache is not None,
            "schema_id": self._metadata_schema_id,
            "validator_available": self._schema_validator is not None,
            self._schema_name_field: self._name,
            self._schema_version_field: self._version,
            "using_json_schema_validation": self._schema_validator is not None,
        }
