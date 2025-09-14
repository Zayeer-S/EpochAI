from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.logging_config import get_logger
from epochai.common.services.collection_reports_service import CollectionReportsService
from epochai.common.services.collection_targets_query_service import CollectionTargetsQueryService
from epochai.data_collection.checker import Checker


class BaseCollector(ABC):
    def __init__(
        self,
        collector_name: str,
        yaml_config: Dict[str, Any],
        utils_class: Any = None,
        saver_class: Any = None,
        service_class: Any = None,
    ):
        try:
            # GET LOGGER
            self.logger = get_logger(__name__)
            self.logger.debug(f"Initalizing {__name__} collector")

            # PARAMETERS
            self.collector_name = collector_name
            self.config = yaml_config
            self.utils = utils_class
            self.saver = saver_class
            if service_class is None:
                self.logger.debug("Using default value for self.coll_targets")
                self.service = CollectionTargetsQueryService()
            else:
                self.service = service_class()

            # HARDCODED SERVICES
            self.reporter = CollectionReportsService()

            # MISC
            self.current_language_code: str
            self.current_collection_type: str
            self.current_collection_name: str

            # DATABASE
            self.data_config = ConfigLoader.get_data_config()
            self.save_to_database: bool = self.data_config.get("data_output").get("database").get("save_to_database")

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

    def _get_available_collection_types(
        self,
        collector_name: str,
        collection_status: str,
    ) -> List[str]:
        """Gets a list of collection types that have uncollected data in the passed-in collector_name"""
        try:
            return self.reporter.get_collection_type_list(
                collector_name=collector_name,
                unique_types_only=True,
                collection_status_name=collection_status,
            )
        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected collection types for {collector_name}: {general_error}",
            )
            return []

    def _get_clean_capitalised_name(self, name_to_clean: str) -> str:
        """Cleans name by removing '_' if present and returning the first word capitalized, otherwise just capitalises"""
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
        language_code: str,
    ) -> None:
        """Appends metadata, collection_target_id and language_code of the just collected collection to current_batch"""
        if not self.save_to_database:
            self.collected_data.append(metadata)  # Still append to attempt local save
            return

        if collection_target_id:
            self.current_batch.append((metadata, collection_target_id, language_code))
        else:
            self.logger.error(f"Error getting collction_target_id '{collection_target_id}'")

        if len(self.current_batch) >= self.batch_size:
            self._save_current_batch()

    def _save_current_batch(self) -> None:
        """Saves current batch whenever this function is called and resets current_batch var"""
        if not self.current_batch:
            self.logger.error("No current_batch var")
            return

        items_by_id_and_language: Dict[Tuple[int, str], List[Dict[str, Any]]] = {}
        for item_data, collection_config_id, language_code in self.current_batch:
            key = (collection_config_id, language_code)
            if key not in items_by_id_and_language:
                items_by_id_and_language[key] = []
            items_by_id_and_language[key].append(item_data)

        total_saved_in_batch = 0
        for (collection_config_id, language_code), items in items_by_id_and_language.items():
            success_count = self.saver.save_incrementally_to_database(
                collected_data=items,
                collection_target_id=collection_config_id,
                language_code=language_code,
            )

            if success_count >= 0:
                total_saved_in_batch += len(items)
                self.logger.debug(f"Saved item with collection_config_id: {collection_config_id}")

        self.total_saved_to_db += total_saved_in_batch
        self.logger.info(f"Saved batch of {total_saved_in_batch} items")

        self.current_batch = []

    def _unconditionally_save_current_batch(self, log_msg: str) -> None:
        """Saves current batch regardless of the batches condition (use at end of collection, topic change, etc)"""
        if not self.save_to_database or not self.current_batch:
            self.logger.debug(
                f"{self._unconditionally_save_current_batch.__name__} was called but not executed",
            )
            return

        self.logger.debug(log_msg)
        self._save_current_batch()

    def _prep_for_collection(
        self,
        collection_type: Optional[str] = None,
        language_code: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Carries out checks on config of collection_type parameter and
        passes whole config of collection_type to a helper function

        Returns:
            Dict mapping language codes to a nullable list
            e.g.'language_code': [item1, item2, etc]
                'language_code': [null]
        """
        config_section = self.config.get(collection_type)
        if not config_section:
            self.logger.warning(f"No config found for '{collection_type}', skipping...")
            return []

        has_items = any(items for items in config_section.values() if isinstance(items, list) and items)
        if not has_items:
            self.logger.warning(f"No data found in config for '{collection_type}', skipping...")
            return []

        self.logger.info(
            "=" * 30,
            f"Starting Collection for {self._get_clean_capitalised_name(collection_type)}",
            "=" * 30,
        )

        if collection_type and language_code:
            try:
                if language_code not in self.config[collection_type]:
                    self.logger.warning(f"No '{language_code}' data available for '{collection_type}'")
                    return []
                return self._collect_and_save(
                    self.config[str(collection_type)][str(language_code)],
                    collection_type,
                )
            except KeyError:
                self.logger.warning(
                    f"Config missing for '{collection_type}' or '{language_code}', skipping...",
                )
                return []

        elif collection_type:
            return self._collect_and_save(
                self.config[collection_type],
                collection_type,
            )

        else:
            self.logger.error("Neither collection type nor language code were passed in")
            return []

    def collect_data(
        self,
        collection_status: str,
        collection_types: Optional[List[str]] = None,
        target_ids: Optional[List[int]] = None,
        language_codes: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Orchestrates data collection and returns the collection via helper methods"""
        neat_name, result = self._get_clean_capitalised_name(self.collector_name), []

        target_config = ConfigLoader.get_wikipedia_targets_config(
            collector_name=self.collector_name,
            collection_status=collection_status,
            collection_types=collection_types,
            target_ids=target_ids,
            language_codes=language_codes,
        )

        self.logger.info(f"=== Starting {neat_name} Data Collection ===")

        for collection_type, language_data in target_config.items():
            if collection_type == "_database_info":  # Skip metadata
                continue

            this_loops_collection = self._collect_and_save(
                language_data,  # This is {"language_code": {"collection_name": target_id}}
                collection_type,
            )
            result.extend(this_loops_collection)

        self.logger.info("=== Collection Complete :) ===")
        return result

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

    def check_targets(
        self,
        collection_status: str,
        collection_types: Optional[List[str]] = None,
        target_ids: Optional[List[int]] = None,
        language_codes: Optional[List[str]] = None,
        recheck: Optional[bool] = None,
    ) -> Any:
        """Checks collection_targets using"""
        target_config = ConfigLoader.get_wikipedia_targets_config(
            collector_name=self.collector_name,
            collection_status=collection_status,
            collection_types=collection_types,
            target_ids=target_ids,
            language_codes=language_codes,
        )

        checker = Checker(
            target_config=target_config,
            utils_instance=self.utils,
            saver_instance=self.saver,
            yaml_config=self.config,
        )

        return checker.check_targets(
            collector_name=self.collector_name,
            collection_status=collection_status,
            collection_types=collection_types,
            target_ids=target_ids,
            language_codes=language_codes,
            recheck=recheck,
        )
