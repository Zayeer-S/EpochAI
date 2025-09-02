from typing import Any, Dict, List, Optional

from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.database_utils import DatabaseUtils


class CollectionTargetsQueryService:
    def __init__(self):
        try:
            self._logger = get_logger(__name__)
            self._collection_targets_dao = CollectionTargetsDAO()
            self._collector_names_dao = CollectorNamesDAO()
            self._collection_statuses_dao = CollectionStatusesDAO()
            self._collection_types_dao = CollectionTypesDAO()
            self._database_utils = DatabaseUtils()
            self._logger.debug("Database components initialized for CollectionTargetManager")
        except Exception as general_error:
            raise Exception("Failed to initialize the database components") from general_error

    def _unused_search_collection_targets(
        self,
        search_term: str,
    ) -> List[Dict[str, Any]]:
        """
        Search collection targets by their names

        Note: left unused on purpose, will be used in future version
        """

        try:
            collection_targets = self._collection_targets_dao.search_by_name(search_term)

            result = []
            for each_target in collection_targets:
                collection_type_obj = self._collection_types_dao.get_by_id(each_target.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                collection_status_obj = self._collection_statuses_dao.get_by_id(
                    each_target.collection_status_id,
                )
                status_name = collection_status_obj.collection_status_name if collection_status_obj else "unknown"

                result.append(
                    {
                        "id": each_target.id,
                        "name": each_target.collection_name,
                        "type": type_name,
                        "language_code": each_target.language_code,
                        "collection_status": status_name,
                        "created_at": each_target.created_at,
                    },
                )

            self._logger.info(f"Found {len(result)} targets matching search term '{search_term}'")
            return result

        except Exception as general_error:
            self._logger.error(f"Error searching collection targets for '{search_term}': {general_error}")
            return []

    def get_wikipedia_targets_config(
        self,
        collector_name: str,
        collection_status: str,
        collection_types: Optional[List[str]] = None,
        language_codes: Optional[List[str]] = None,
        target_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """
        Gets Wikipedia collection targets from database based on specified filters

        Valid parameter combinations:
            1. collector_name only
            2. collector_name + collection_types + language_codes + collection_status
            3. collector_name + collection_types + collection_status
            4. collector_name + language_codes + collection_status

        Returns:
            Dict in format: {"collection_type": {"language_code": {"collection_name": target_id}}}
        """

        try:
            collector_name_id, _, collection_status_id = self._database_utils.get_name_type_status_ids(
                collector_name=collector_name,
                collection_status_name=collection_status,
            )

            # collector_name only -> get all uncollected
            if not collection_types and not language_codes and not target_ids:
                collection_targets = self._collection_targets_dao.get_by_collector_name_id(
                    collector_name_id,
                    collection_status_id,
                )

            # collector_name + collection_types + language_codes + collection_status
            elif collection_types and language_codes:
                collection_targets = []
                for collection_type in collection_types:
                    _, collection_type_id, _ = self._database_utils.get_name_type_status_ids(
                        collection_type=collection_type,
                    )

                    for language_code in language_codes:
                        type_lang_targets = self._collection_targets_dao.get_by_type_and_language(
                            collection_type_id,
                            language_code,
                            collection_status_id,
                        )
                        collection_targets.extend(type_lang_targets)

            # collector_name + collection_types + collection_status
            elif collection_types and not language_codes:
                collection_targets = []
                for collection_type in collection_types:
                    _, collection_type_id, _ = self._database_utils.get_name_type_status_ids(
                        collection_type=collection_type,
                    )

                    type_targets = self._collection_targets_dao.get_by_collection_type_id(
                        collection_type_id,
                        collection_status_id,
                    )
                    collection_targets.extend(type_targets)

            # collector_name + language_codes + collection_status
            elif language_codes and not collection_types:
                all_targets = self._collection_targets_dao.get_by_collector_name_id(
                    collector_name_id,
                    collection_status_id,
                )
                collection_targets = [target for target in all_targets if target.language_code in language_codes]

            elif target_ids and language_code:
                pass

            elif target_ids:
                self._logger.info("IDs not yet implemented")

            else:
                self._logger.warning("Invalid parameter combination provided")
                return {}

            # Group targets by type and language
            grouped_targets: Dict[str, Any] = {}

            for target in collection_targets:
                collection_type_obj = self._collection_types_dao.get_by_id(target.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                if type_name not in grouped_targets:
                    grouped_targets[type_name] = {}

                if target.language_code not in grouped_targets[type_name]:
                    grouped_targets[type_name][target.language_code] = {}

                grouped_targets[type_name][target.language_code][target.collection_name] = target.id

            self._logger.info(f"Retrieved {len(collection_targets)} collection targets from database")
            return grouped_targets

        except Exception as general_error:
            self._logger.error(f"Error retrieving Wikipedia targets from database: {general_error}")
            return {}
