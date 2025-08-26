from abc import ABC, abstractmethod
from typing import Any, Dict, List

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.collection_targets_manager import CollectionTargetManager
from epochai.common.logging_config import get_logger


class BaseCollector(ABC):
    def __init__(
        self,
        collector_name: str,
        config: Dict[str, Any],
        utils_class: Any = None,
        saver_class: Any = None,
        collection_targets_class: Any = None,
    ):
        try:
            # GET LOGGER
            self.logger = get_logger(__name__)
            self.logger.debug(f"Initalizing {__name__} collector")

            # PARAMETERS
            self.collector_name = collector_name
            self.config = config
            self.utils = utils_class
            self.saver = saver_class
            self.coll_targets = CollectionTargetManager()  # Pass it here because its common

            # MISC
            self.current_language_code: str
            self.current_collection_type: str
            self.current_collection_name: str

            # DATABASE
            self.data_config = ConfigLoader.get_data_config()
            self.save_to_database: bool = (
                self.data_config.get("data_output").get("database").get("save_to_database")
            )

            if self.save_to_database:
                self.batch_size = self.data_config.get("data_output").get("database").get("batch_size")
                self.current_batch: List[Any] = []
                self.total_saved_to_db = 0
                self.logger.info(f"Initialized {self.collector_name}")
            else:
                self.collected_data: List[Any] = []
                self.logger.info(f"Initialized {self.collector_name} in Test Mode")

        except ImportError as import_error:
            raise ImportError(f"Error importing modules: {import_error}") from import_error

        except Exception as general_error:
            raise Exception(f"Error during initalization: {general_error}") from general_error

    def _get_clean_capitalised_name(self, name_to_clean: str) -> str:
        """Cleans name by removing '_' if present and returning the first word capitalized, otherwise just capitalises"""  # noqa: E501
        if "_" in self.collector_name:
            temp_name = name_to_clean.split("_")
            neat_name = temp_name[0]
        else:
            neat_name = self.collector_name
        return neat_name.capitalize()

    def _add_to_batch(
        self,
        metadata: Dict[str, Any],
        collection_target_id: int,
    ) -> None:
        """Adds a single item with its collection_config_id to the batch"""
        if not self.save_to_database:
            self.collected_data.append(metadata)  # Still append to attempt local save
            return

        if collection_target_id:
            self.current_batch.append((metadata, collection_target_id))
        else:
            self.logger.error(f"Error getting collction_target_id '{collection_target_id}'")

        if len(self.current_batch) >= self.batch_size:
            self._save_current_batch()

    def _save_current_batch(self) -> None:
        """Saves current batch if needed and resets current_batch var"""
        if not self.current_batch:
            self.logger.error("No current_batch var")
            return

        items_by_config_id: Dict[int, List[Dict[str, Any]]] = {}
        for item_data, collection_config_id in self.current_batch:
            if collection_config_id not in items_by_config_id:
                items_by_config_id[collection_config_id] = []
            items_by_config_id[collection_config_id].append(item_data)

        total_saved_in_batch = 0
        for collection_config_id, items in items_by_config_id.items():
            success_count = self.saver.save_incrementally_to_database(
                collected_data=items,
                collection_target_id=collection_config_id,
                language_code=self.current_language_code,
            )

            if success_count >= 0:
                total_saved_in_batch += len(items)
                self.logger.debug(f"Saved item with collection_config_id: {collection_config_id}")

        self.total_saved_to_db += total_saved_in_batch
        self.logger.info(f"Saved batch of {total_saved_in_batch} items")

        self.current_batch = []

    def _unconditionally_save_current_batch(self, log_msg: str) -> None:
        """Saves current batch regardless of the batches condition (use at end of collection, topic change, etc)"""  # noqa: E501
        if not self.save_to_database or not self.current_batch:
            self.logger.debug(
                f"{self._unconditionally_save_current_batch.__name__} was called but not executed",
            )
            return

        self.logger.info(log_msg)
        self._save_current_batch()

    def _prep_for_collection(
        self,
        collection_type: str,
    ) -> List[Dict[str, Any]]:
        config_section = self.config.get(collection_type)
        if not config_section:
            self.logger.warning(f"No valid configs found for '{collection_type}', skipping...")
            return []

        has_items = any(items for items in config_section.values() if isinstance(items, list) and items)
        if not has_items:
            self.logger.warning(f"No items found in config for '{collection_type}', skipping...")
            return []

        neat_name = self._get_clean_capitalised_name(collection_type)

        self.logger.info(
            "=" * 30,
            f"Starting Collection for {neat_name}",
            "=" * 30,
        )

        return self._collect_and_save(
            self.config[f"{collection_type}"],
            collection_type,
        )

    def collect_data(self) -> List[Dict[str, Any]]:
        """Orchestrates data collection and returns the collection via helper methods"""
        neat_name = self._get_clean_capitalised_name(self.collector_name)

        self.logger.info(f"=== Starting {neat_name} Data Collection ===")

        target_types = self.coll_targets.get_list_of_uncollected_types_by_collector_name(
            self.collector_name,
            unique_types_only=True,
        )

        all_data = []
        for type in target_types:
            this_loops_collection = self._prep_for_collection(type)
            all_data.extend(this_loops_collection)

        self.logger.info("=== Collection Complete :) ===")

        return all_data

    @abstractmethod
    def _collect_and_save(
        self,
        items_by_language: Dict[str, Dict[str, int]],
        collection_type: str,
    ) -> List[Dict[str, Any]]:
        """Actually collects the data and saves it to the database (if not saving locally)"""
        raise NotImplementedError(
            f"Error subclasses must implement {self._collect_and_save.__name__} function",
        )
