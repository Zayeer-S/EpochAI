from datetime import datetime
import re
from typing import Any, Dict, List, Optional, Tuple

from genson import SchemaBuilder
import jsonschema
from jsonschema import ValidationError

from epochai.common.database.models import RawData
from epochai.data_processing.cleaning.base_cleaner import BaseCleaner


class WikipediaCleaner(BaseCleaner):
    def __init__(self):
        super().__init__(
            cleaner_name=self.config.get("cleaners").get("wikipedia").get("cleaner_name"),
            cleaner_version=self.config.get("cleaners").get("wikipedia").get("current_version"),
        )
        self.metadata_schema_cache: Optional[Dict[str, Any]] = None
        self.schema_validator: Optional[jsonschema.protocols.Validator] = None
        self.schema_generation_count: int = 0
        self.temp_schemas: List[Dict[str, Any]] = []
        self.metadata_schema_id: Optional[int] = None

        self._multiple_whitespace_pattern = re.compile(r"\s+")
        self._citation_pattern = re.compile(r"\[\d+\]|\[citation needed\]|\[clarification needed\]")
        self._unicode_quotes_pattern = re.compile(r'[""]')
        self._unicode_dashes_pattern = re.compile(r"[–—]")  # noqa
        self._multiple_newlines_pattern = re.compile(r"\n{3,}")

        self.schema_cache_limit = int(self.config.get("cleaners").get("wikipedia").get("schema_cache_limit"))

        self._load_schema_from_database()

        self.logger.info(f"Wikipedia Cleaner Initialized ({self.metadata_schema_id})")

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

        self.temp_schemas = self.temp_schemas[-3:]

        first_schema = self.temp_schemas[0].get("schema", {})  # get schema structure but set contents as null
        return all(schema.get("schema", {}) == first_schema for schema in self.temp_schemas)

    def _cache_schema_to_database(
        self,
        schema: Dict[str, Any],
    ) -> Optional[int]:
        """
        Caches schema to database

        Note:
            Can create new schema in database. Fallback for if automatic python schema building makes
            a schema not found in the database.
        """

        try:
            existing_schema = self.cleaned_data_metadata_schema_dao.find_schema_by_content(schema)
            if existing_schema and existing_schema.id:
                self.logger.info(f"Schema already exists in the database (id: {existing_schema.id})")
                return existing_schema.id

            new_schema_id = self.cleaned_data_metadata_schema_dao.create_schema(schema)
            if new_schema_id:
                self.logger.info(f"Cached new schema to database (id: {new_schema_id})")
                return new_schema_id

            self.logger.error("Failed to cache schema to database")
            return None

        except Exception as general_error:
            self.logger.error(f"Error caching schema to database: {general_error}")
            return None

    def get_metadata_schema_id(self) -> Optional[int]:
        if self.metadata_schema_id is None:
            self.logger.error("No metadata schema id available - check schema generation logs")
        return self.metadata_schema_id

    def clean_content(self, raw_data: RawData) -> Dict[str, Any]:
        """
        Cleans Wikipedia Raw Data

        Returns:
            Dict containing cleaned metadata
        """

        if not raw_data.metadata:
            raise ValueError(f"Raw data ({raw_data.id}) has no metadata to clean")

        metadata = raw_data.metadata.copy()

        if "content" in metadata:
            metadata["cleaned_content"] = self._clean_text_content(metadata["content"])
            metadata["content_word_count"] = self._count_words(metadata["cleaned_content"])
            metadata["content_char_count"] = len(metadata["cleaned_content"])

        if "summary" in metadata:
            metadata["cleaned_summary"] = self._clean_text_content(metadata["summary"])
            metadata["summary_word_count"] = self._count_words(metadata["cleaned_summary"])

        if "title" in metadata:
            metadata["cleaned_title"] = self._clean_title(metadata["title"])

        if "categories" in metadata:
            metadata["cleaned_categories"] = self._clean_categories(metadata["categories"])
            metadata["category_count"] = len(metadata["cleaned_categories"])

        if "links" in metadata:
            metadata["cleaned_links"] = self._clean_links(metadata["links"])
            metadata["internal_link_count"] = len(metadata["cleaned_links"])

        metadata["cleaned_at"] = datetime.now().isoformat()
        metadata["original_content_length"] = len(metadata.get("content", ""))
        metadata["cleaning_operations_applied"] = self._get_cleaning_operations_list()

        if self.metadata_schema_cache is None:
            generated_schema = self._generate_initial_schema_from_metadata(metadata)

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

        return metadata

    def validate_cleaned_content(self, cleaned_data: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Validates cleaned wikipedia content"""

        if self.schema_validator is not None:
            return self._validate_with_json_schema(cleaned_data)

        self.logger.info("Falling back to basic requirement validator")
        return self._validate_basic_requirements(cleaned_data)

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

            validation_error.append(f"Schema validation failed: {validation_error.message}")

            if validation_error.absolute_path:
                field_path = ".".join(str(p) for p in validation_error.absolute_path)
                validation_error.append(f"Field path: {field_path}")

            for suberror in validation_error.context:
                validation_error.append(f"Sub error: {suberror.message}")

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
            if len(content.strip() < 10):
                validation_errors.append(f"Content too short: {len(content)} char")

        is_valid = len(validation_errors) == 0
        error_dict = {"validation_errors": validation_errors} if validation_errors else None

        return is_valid, error_dict

    def _clean_text_content(self, text: str) -> str:
        """Cleans and normalizes text content"""

        if not text:
            return ""

        text = self._citation_pattern.sub("", text)
        text = self._unicode_quotes_pattern.sub('"', text)
        text = self._unicode_dashes_pattern.sub("-", text)
        text = self._multiple_whitespace_pattern.sub(" ", text)
        text = self._multiple_newlines_pattern.sub("\n\n", text)

        return text.strip()

    def _clean_title(self, title: str) -> str:
        """Cleans and normalizes the title content"""
        if not title:
            return ""

        title = self._citation_pattern.sub("", title)
        title = self._multiple_whitespace_pattern.sub(" ", title)

        return title.strip()

    def _clean_categories(self, categories: List) -> List:
        """Cleans and deduplicates categories"""
        if not categories:
            return []

        cleaned_categories = []
        seen_categories = set()

        for category in categories:
            if not isinstance(category, str):
                continue

            cleaned_category = self._multiple_whitespace_pattern.sub(" ", category.strip())

            if cleaned_category and cleaned_category not in seen_categories:
                cleaned_categories.append(cleaned_category)
                seen_categories.add(cleaned_category)

        return cleaned_categories

    def _clean_links(self, links: List) -> List:
        """Cleans and deduplicates internal links"""

        if not links:
            return []

        cleaned_links = []
        seen_links = set()

        for link in links:
            if not isinstance(link, str):
                continue

            cleaned_link = self._multiple_whitespace_pattern.sub(" ", link.strip())

            if cleaned_link and cleaned_link not in seen_links:
                cleaned_links.append(cleaned_link)
                seen_links.add(cleaned_link)

        return cleaned_links

    def _count_words(self, text: str) -> int:
        """Counts words in text"""
        if not text:
            return 0
        return len(text.split())

    def _get_cleaning_operations_list(self) -> list:
        """Return list of cleaning operations applied."""
        return [
            "citation_removal",
            "unicode_normalization",
            "whitespace_normalization",
            "newline_normalization",
            "category_deduplication",
            "link_deduplication",
            "content_validation",
        ]

    def clean_wikipedia_batch(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """Cleans raw wikipedia data with 'valid' status"""
        self.logger.info("Starting Wikipedia batch cleaning")

        raw_data_records = self.raw_data_dao.get_by_validation_status("valid")

        if limit:
            raw_data_records = raw_data_records[:limit]
            self.logger.info(f"Limited batch to {limit} records")

        if not raw_data_records:
            self.logger.info("No valid raw Wikipedia data found to clean")
            return {"success_count": 0, "error_count": 0, "cleaned_ids": [], "error_ids": []}

        raw_data_ids = [record.id for record in raw_data_records if record.id]
        return self.clean_multiple_records(raw_data_ids)

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
        }
