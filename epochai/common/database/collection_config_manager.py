from typing import Any, Dict, List, Optional

from epochai.common.database.dao.collection_configs_dao import CollectionConfigsDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.database.database import get_database
from epochai.common.logging_config import get_logger


class CollectionConfigManager:
    _db_connection = None
    _collection_configs_dao = None
    _collector_names_dao = None
    _collection_types_dao = None
    _logger = None
    _collector_name = None  # Set by config loader

    @classmethod
    def _lazy_database_init(cls):
        if cls._db_connection is None:
            cls._logger = get_logger(__name__)
            try:
                cls._db_connection = get_database()
                cls._collection_configs_dao = CollectionConfigsDAO()
                cls._collector_names_dao = CollectorNamesDAO()
                cls._collection_types_dao = CollectionTypesDAO()
                cls._logger.info("Database components initialized for CollectionConfigManager")
            except Exception as general_error:
                cls._logger.error(f"Failed to initialize the database components: {general_error}")
                raise

    @classmethod
    def set_collector_name(
        cls,
        collector_name: str,
    ):
        cls._collector_name = collector_name

    @classmethod
    def get_collection_configs_from_database(
        cls,
        collector_name: Optional[str] = None,
        collection_type: Optional[str] = None,
        language_code: Optional[str] = None,
        is_collected: Optional[bool] = None,
    ) -> Dict[str, Any]:
        """Gets collection configurations from database"""

        cls._lazy_database_init()

        if collector_name is None:
            collector_name = cls._collector_name

        if collector_name is None:
            cls._logger.error("Error, cls._collector_name is null")
            raise

        try:
            # Directly get uncollected if we have collection_type and language_code
            if collection_type and language_code:
                collection_configs = cls._collection_configs_dao.get_uncollected_by_type_and_language(
                    collection_type,
                    language_code,
                )

            elif collection_type:
                if is_collected is not None:
                    all_configs = cls._collection_configs_dao.get_by_collector_and_type(
                        collector_name,
                        collection_type,
                    )
                    collection_configs = [c for c in all_configs if c.is_collected == is_collected]
                else:
                    collection_configs = cls._collection_configs_dao.get_uncollected_by_type(collection_type)

            # Get all configs and filter so that we only have uncollected
            else:
                all_configs = cls._collection_configs_dao.get_all()
                if all_configs is None:
                    collection_configs = []
                else:
                    collection_configs = all_configs

                    if is_collected is not None:
                        collection_configs = [c for c in collection_configs if c.is_collected == is_collected]
                    else:
                        collection_configs = [c for c in collection_configs if not c.is_collected]

            grouped_configs: Dict[str, Any] = {}

            for each_config in collection_configs:
                collection_type_obj = cls._collection_types_dao.get_by_id(each_config.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                if type_name not in grouped_configs:
                    grouped_configs[type_name] = {}

                if each_config.language_code not in grouped_configs[type_name]:
                    grouped_configs[type_name][each_config.language_code] = []

                grouped_configs[type_name][each_config.language_code].append(each_config.collection_name)

            cls._logger.info(f"Retrieved {len(collection_configs)} collection configs from database")
            return grouped_configs

        except Exception as general_error:
            cls._logger.error(f"Error retrieving collection configs from database: {general_error}")
            return {}

    @classmethod
    def get_uncollected_configs_by_type(
        cls,
        collection_type: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Gets uncollected configs of a specific type  for a specific collector, grouped by language"""

        cls._lazy_database_init()

        try:
            collection_configs = cls._collection_configs_dao.get_uncollected_grouped_by_language(
                collection_type,
            )

            result = {}
            for language_code, config_list in collection_configs.items():
                result[language_code] = [
                    {
                        "id": each_config.id,
                        "name": each_config.collection_name,
                        "is_collected": each_config.is_collected,
                    }
                    for each_config in config_list
                ]

            cls._logger.info(f"Retrieved uncollected {collection_type} configs for {len(result)} languages")
            return result

        except Exception as general_error:
            cls._logger.error(f"Error retrieving uncollected {collection_type} configs: {general_error}")
            return {}

    @classmethod
    def get_collection_status_summary(cls) -> Dict[str, Any]:
        """Gets summary of collection status across all types and languages"""

        cls._lazy_database_init()

        try:
            status = cls._collection_configs_dao.get_collection_status()
            cls._logger.info("Retrieved collection status summary from database")
            return status

        except Exception as general_error:
            cls._logger.error(f"Error retrieving collection status summary: {general_error}")
            return {"by_type_and_language": [], "summary": {}}

    @classmethod
    def mark_config_as_collected(
        cls,
        config_id: int,
    ) -> bool:
        """Marks a collection config as collected"""

        cls._lazy_database_init()

        try:
            success = cls._collection_configs_dao.mark_as_collected(config_id)
            if success:
                cls._logger.info(f"Marked config {config_id} as collected")
            return success

        except Exception as general_error:
            cls._logger.error(f"Error marking config {config_id} as collected: {general_error}")
            return False

    @classmethod
    def mark_config_as_uncollected(
        cls,
        config_id: int,
    ) -> bool:
        """Marks a collection config as uncollected"""

        cls._lazy_database_init()

        try:
            success = cls._collection_configs_dao.mark_as_uncollected(config_id)
            if success:
                cls._logger.info(f"Marked config {config_id} as uncollected")
            return success

        except Exception as general_error:
            cls._logger.error(f"Error marking config {config_id} as uncollected: {general_error}")
            return False

    @classmethod
    def search_collection_configs(
        cls,
        search_term: str,
    ) -> List[Dict[str, Any]]:
        """Search collection configs by their names"""

        cls._lazy_database_init()

        try:
            collection_configs = cls._collection_configs_dao.search_by_name(search_term)

            result = []
            for each_config in collection_configs:
                collection_type_obj = cls._collection_types_dao.get_by_id(each_config.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                result.append(
                    {
                        "id": each_config.id,
                        "name": each_config.collection_name,
                        "type": type_name,
                        "language_code": each_config.language_code,
                        "is_collected": each_config.is_collected,
                        "created_at": each_config.created_at,
                    },
                )

            cls._logger.info(f"Found {len(result)} configs matching search term '{search_term}'")
            return result

        except Exception as general_error:
            cls._logger.error(f"Error searching collection configs for '{search_term}': {general_error}")
            return []

    @classmethod
    def get_combined_wikipedia_config(
        cls,
        collector_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Gets combined wikipedia config from database and config.yml

        Note:
            Falls back to YAML only config if combining fails
        """

        try:
            if collector_name is None:
                collector_name = cls._collector_name
            elif collector_name is not None:
                cls.set_collector_name(collector_name)

            from epochai.common.config.config_loader import ConfigLoader

            yaml_config = ConfigLoader.get_wikipedia_yaml_config()

            db_configs = cls.get_collection_configs_from_database(collector_name=collector_name)

            combined_config = yaml_config.copy()

            if "politicians" in db_configs:
                combined_config["politicians"] = db_configs["politicians"]

            if "political_topics" in db_configs:
                combined_config["political_topics"] = db_configs["political_topics"]

            if "important_persons" in db_configs:
                combined_config["important_persons"] = db_configs["important_persons"]

            combined_config["_database_info"] = {
                "total_types": len(db_configs),
                "last_updated": "from_database",
            }

            if cls._logger:
                cls._logger.info("Successfully combined YAML and Database configurations")
            return combined_config

        except Exception as general_error:
            if cls._logger:
                cls._logger.error(
                    f"Error getting combined Wikipedia config: {general_error} - falling back to YAML only",
                )
            return ConfigLoader.get_wikipedia_yaml_config()

    @classmethod
    def test_database_connection(cls) -> bool:
        """Tests if database connection is working"""

        try:
            cls._lazy_database_init()
            return cls._db_connection.test_connection()
        except Exception as general_error:
            if cls._logger:
                cls._logger.error(f"Database connection test failed: {general_error}")
            return False
