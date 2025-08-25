from datetime import datetime
import re
from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.models import RawData
from epochai.data_processing.cleaners.base_cleaner import BaseCleaner


class WikipediaCleaner(BaseCleaner):
    def __init__(self):
        self.config = ConfigLoader.get_data_config()

        super().__init__(
            cleaner_name=self.config.get("cleaners").get("wikipedia").get("cleaner_name"),
            cleaner_version=self.config.get("cleaners").get("wikipedia").get("current_version"),
        )

        # FALL BACK CLEANING PROCESS VARS
        self._multiple_whitespace_pattern = re.compile(r"[ \t]+")
        self._citation_pattern = re.compile(r"\[\d+\]|\[citation needed\]|\[clarification needed\]")
        self._unicode_quotes_pattern = re.compile(r"[\u201c\u201d\u2018\u2019]")
        self._unicode_dashes_pattern = re.compile(r"[–—]")  # noqa
        self._multiple_newlines_pattern = re.compile(r"\n{3,}")

        self.logger.info(f"Wikipedia Cleaner Initialized ({self.get_metadata_schema_id()})")

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

        return metadata

    def _clean_text_content(self, text: str) -> str:
        """Cleans and normalizes text content"""

        if not text or not isinstance(text, str):
            return ""

        text = self._citation_pattern.sub("", text)
        text = re.sub(r"[\u201c\u201d]", '"', text)
        text = re.sub(r"[\u2018\u2019]", "'", text)
        text = self._unicode_dashes_pattern.sub("-", text)
        text = self._multiple_newlines_pattern.sub("\n\n", text)
        text = self._multiple_whitespace_pattern.sub(" ", text)

        return text.strip()

    def _clean_title(self, title: str) -> str:
        """Cleans and normalizes the title content"""
        if not title or not isinstance(title, str):
            return ""

        title = self._citation_pattern.sub("", title)
        title = self._multiple_whitespace_pattern.sub(" ", title)

        return title.strip()

    def _clean_categories(self, categories: List) -> List:
        """Cleans and deduplicates categories"""
        if not categories or not isinstance(categories, List):
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

        if not links or not isinstance(links, List):
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

        raw_data_records = self.service.raw_data_dao.get_by_validation_status("valid")

        if limit is not None and not limit < 0:
            raw_data_records = raw_data_records[:limit]
            self.logger.info(f"Limited batch to {limit} records")

        if not raw_data_records:
            self.logger.info("No valid raw Wikipedia data found to clean")
            return {"success_count": 0, "error_count": 0, "cleaned_ids": [], "error_ids": []}

        raw_data_ids = [record.id for record in raw_data_records if record.id]
        return self.clean_multiple_records(raw_data_ids)
