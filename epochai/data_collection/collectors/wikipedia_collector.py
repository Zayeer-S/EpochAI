from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.utils.wikipedia_utils import WikipediaUtils
from epochai.data_collection.collectors.base_collector import BaseCollector
from epochai.data_collection.savers.wikipedia_saver import WikipediaSaver


class WikipediaCollector(BaseCollector):
    def __init__(
        self,
    ):
        self.yaml_config = ConfigLoader.get_collector_yaml_config("wikipedia")

        self._collector_name = self.yaml_config.get("collector_name")
        self._collector_version = self.yaml_config.get("current_schema_version")

        super().__init__(
            collector_name=self._collector_name,
            yaml_config=self.yaml_config,
            utils_class=WikipediaUtils(self.yaml_config),
            saver_class=WikipediaSaver(
                collector_name=self._collector_name,
                collector_version=self._collector_version,
            ),
        )

    def collect_each_page_metadata(
        self,
        collection_name: str,
        language_code: str,
        collection_target_id: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Gets metadata for only one page and saves it (if incrementally saving)

        Args:
            collection_name: Collection name / title of Wikipedia page
            language_code: Language code of the collection_name
            collection_target_id: PK of collection_target

        Returns:
            Optional[Dict[str, Any]]: Page metadata or None
        """
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
                self._add_to_batch(metadata, collection_target_id, language_code)
        else:
            self.logger.warning(
                f"Nothing collected for ({self.current_language_code}): {self.current_collection_name}",
            )

        return metadata

    def _collect_and_save(
        self,
        items_by_language_code: Dict[str, Dict[str, int]],
        collection_type: str,
    ) -> List[Dict[str, Any]]:
        """
        Collects data and saves (if database saving) via
        process_items_by_language and collect_each_page_metadata

        Args:
            items_by_language:
                {"language_code":
                    {"first_collection_name": id_of_first_collection_name},
                    {"second_collection_name": id_of_second_collection_name}
                }
            collection_type: The type of collection e.g. "political_topics"

        Returns:
            Dict mapping language codes to a nullable list
            e.g.'language_code': [item1, item2, etc]
                'language_code': [null]
        """
        if not items_by_language_code:
            self.logger.warning(f"No items provided for {collection_type}")
            return []

        self.logger.info(
            f"Collecting {collection_type} for {len(items_by_language_code)} items across {len(items_by_language_code.keys())} languages",  # noqa
        )

        self.current_collection_type = collection_type

        for _lanugage_code, _items_dict in items_by_language_code.items():
            results_by_language = self.utils.process_items_by_language(
                items_by_language_code,
                self.collect_each_page_metadata,
            )

            if self.save_to_database:
                self._unconditionally_save_current_batch("Saving in between language change")

        all_collected_data = []
        for _language_code, results in results_by_language.items():
            if results:
                all_collected_data.extend(results)

        if self.save_to_database:
            self._unconditionally_save_current_batch("Saving in between topic change")

        return all_collected_data
