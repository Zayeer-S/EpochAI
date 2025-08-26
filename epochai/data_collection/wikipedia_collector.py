from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.utils.wikipedia_utils import WikipediaUtils
from epochai.data_collection.base_collector import BaseCollector
from epochai.database_savers.wikipedia_saver import WikipediaSaver


class WikipediaCollector(BaseCollector):
    def __init__(
        self,
    ):
        self.config = ConfigLoader.get_wikipedia_config()

        super().__init__(
            collector_name=self.config.get("api").get("collector_name"),
            config=self.config,
            utils_class=WikipediaUtils(self.config),
            saver_class=WikipediaSaver(),
        )

    def _collect_each_page_metadata(
        self,
        collection_name: str,
        language_code: str,
        collection_target_id: int,
    ) -> Optional[Dict[str, Any]]:
        """Gets metadata for only one page and saves it (if incrementally saving)"""
        self.logger.info(f"Collecting ({language_code}): {collection_name}")

        self.current_collection_name = collection_name
        self.current_language_code = language_code

        metadata: Dict[str, Any] = self.utils.get_wikipedia_metadata(
            self.current_collection_name,
            self.current_language_code,
        )

        if metadata:
            self.logger.debug(
                f"Successfully collected ({self.current_language_code}): {self.current_collection_name}",
            )

            if self.save_to_database:
                self._add_to_batch(metadata, collection_target_id)
        else:
            self.logger.warning(
                f"Nothing collected for ({self.current_language_code}): {self.current_collection_name}",
            )

        return metadata

    def _collect_and_save(
        self,
        items_by_language: Dict[str, Dict[str, int]],
        collection_type: str,
    ) -> List[Dict[str, Any]]:
        if not items_by_language:
            self.logger.warning(f"No items provided for {collection_type}")
            return []

        self.logger.info(
            f"Collecting {collection_type} for {len(items_by_language)} items across {len(items_by_language.keys())} languages",  # noqa
        )

        self.current_collection_type = collection_type

        results_by_language = self.utils.process_items_by_language(
            items_by_language,
            self._collect_each_page_metadata,
        )

        all_collected_data = []
        for _language_code, results in results_by_language.items():
            if results:
                all_collected_data.extend(results)

        if self.save_to_database:
            self._unconditionally_save_current_batch("Saving in between topic change")

        return all_collected_data
