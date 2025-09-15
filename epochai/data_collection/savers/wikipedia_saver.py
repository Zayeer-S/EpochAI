from typing import Any, Dict, Optional, Tuple

from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors
from epochai.data_collection.savers.base_saver import BaseSaver


class WikipediaSaver(BaseSaver):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(
        self,
        collector_name: str,
        collector_version: str,
    ):
        super().__init__(collector_name, collector_version)

        self._min_content_length = self._data_config.get("data_validator").get("min_content_length")
        self._required_fields = self._data_config.get("data_validator").get("required_fields_wikipedia")

    @handle_generic_errors_gracefully("while preparing metadata for storage", {})
    def _prepare_metadata_for_storage(
        self,
        collected_item: Dict[str, Any],
        language_code: str,
    ) -> Dict[str, Any]:
        """Prepares collected Wikipedia item for storage with proper field mapping"""
        metadata = {}

        field_mappings = {
            "title": "title",
            "content": "content",
            "url": "url",
            "language": language_code,
            "page_id": "page_id",
            "word_count": lambda: len(collected_item.get("content", "").split()) if collected_item.get("content") else 0,
            "timestamp": "timestamp",
            "collected_at": "collected_at",
        }

        for our_field, source_field in field_mappings.items():
            if callable(source_field):
                metadata[our_field] = source_field()
            elif source_field in collected_item:
                metadata[our_field] = collected_item[source_field]  # type: ignore
            elif our_field in collected_item:
                metadata[our_field] = collected_item[our_field]

        return metadata

    @handle_generic_errors_gracefully("during Wikipedia validation function operation", (False, None))
    def wikipedia_validation_function(
        self,
        data: Dict[str, Any],
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        """Custom validation function for collected Wikipedia data"""
        validation_errors = []

        for field in self._required_fields:
            if field not in data:
                validation_errors.append(f"Missing required field: {field}")
            elif not data[field]:
                validation_errors.append(f"Empty required field: {field}")

        if "content" in data:
            content = data["content"]
            if len(content.strip()) < self._min_content_length:
                validation_errors.append(f"Content too short: {len(content)} char")

        is_valid = len(validation_errors) == 0
        error_dict = {"validation_errors": validation_errors} if validation_errors else None

        return is_valid, error_dict
