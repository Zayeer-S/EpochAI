from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors
from epochai.common.utils.fivethirtyeight_utils import FiveThirtyEightUtils
from epochai.data_collection.collectors.base_collector import BaseCollector
from epochai.data_collection.savers.fivethirtyeight_saver import FiveThirtyEightSaver


class FiveThirtyEightCollector(BaseCollector):
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self, collection_type: str):
        self._yaml_config = ConfigLoader.get_collector_yaml_config("fivethirtyeight")
        self._collector_name = self._yaml_config["collector_name"]
        self._collector_version = self._yaml_config["current_schema_version"]
        self._collection_type = collection_type

        super().__init__(
            collector_name=self._collector_name,
            yaml_config=self._yaml_config,
            utils_class=FiveThirtyEightUtils(self._yaml_config, collection_type),
            saver_class=FiveThirtyEightSaver(
                collector_name=self._collector_name,
                collector_version=self._collector_version,
            ),
        )

    @handle_generic_errors_gracefully("", None)
    def collect_each_record(
        self,
        collection_name: str,
        language_code: str,
        collection_target_id: int,
    ) -> Optional[Dict[str, Any]]:
        """
        Gets polling data for a single collection target and saves it (if incrementally saving)

        Args:
            collection_name: Collection name in format "{cycle}_{state}_{candidate}_{row_id}"
            language_code: Language code (should be "en" for FiveThirtyEight data)
            collection_target_id: PK of collection_target
        Returns:
            Optional[Dict[str, Any]]: Polling record metadata or None
        """

        self.logger.info(f"Collecting ({collection_target_id}): {collection_name}")

        self.current_collection_name = collection_name
        self.current_language_code = language_code

        try:
            parts = collection_name.split("_")
            if len(parts) < 4:
                self.logger.error(f"Inavlid collection name format: {collection_name}")
                return None

            row_id = parts[-1]

        except (IndexError, ValueError) as error:
            self.logger.error(f"Error parsing collection name: {error}")
            return None

        metadata: Dict[str, Any] = self.utils.get_target(
            row_id=row_id,
        )

        if metadata:
            self.logger.debug(
                f"Successfully collected {collection_target_id}: {self.current_collection_name}",
            )

            if self.save_to_database:
                self._add_to_batch(metadata, collection_target_id, language_code)
        else:
            self.logger.warning(
                f"Nothing collected for {self.current_language_code}: {self.current_collection_name}",
            )

        return metadata

    def _collect_and_save(
        self,
        items_by_language: Dict[str, Dict[str, int]],
        collection_type: str,
    ) -> List[Dict[str, Any]]:
        """
        Collects polling data and saves it (if database saving) via helper methods

        Args:
            items_by_language:
                {
                    "language_code":
                        {"first_collection_name"}: id_of_first_collection_name,
                        {"second_collection_name"}: id_of_second_collection_name,
                }
            collection_type: The type of collection

        Returns:
            List of collected polling records
        """

        if not items_by_language:
            self.logger.warning(f"No items provided for {collection_type}")
            return []

        self.logger.info(
            f"Collecting {collection_type} for {sum(len(items) for items in items_by_language.values())} items across {len(items_by_language.keys())} languages",  # noqa
        )

        self.current_collection_type = collection_type

        all_collected_data = []

        for language_code, items_dict in items_by_language.items():
            self.logger.info(f"Processing {len(items_dict)}")

            results_by_language = self.utils.process_items_by_language(
                {language_code: items_dict},
                self.collect_each_record,
            )

            if self.save_to_database:
                self._unconditionally_save_current_batch("Saving in between language change")

            for _language_code, results in results_by_language.items():
                if results:
                    all_collected_data.extend(results)

        if self.save_to_database:
            self._unconditionally_save_current_batch("Saving in between topic change")

        return all_collected_data
