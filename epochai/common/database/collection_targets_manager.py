from typing import Any, Dict, List, Optional, Tuple

from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionTargets
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger


class CollectionTargetManager:
    def __init__(self):
        self._db_connection = None
        self._collection_targets_dao = None
        self._collector_names_dao = None
        self._collection_types_dao = None
        self._collection_statuses_dao = None
        self._logger = None

    def _lazy_database_init(self):
        if self._db_connection is None:
            self._logger = get_logger(__name__)
            try:
                self._db_connection = get_database()
                self._collection_targets_dao = CollectionTargetsDAO()
                self._collector_names_dao = CollectorNamesDAO()
                self._collection_statuses_dao = CollectionStatusesDAO()
                self._collection_types_dao = CollectionTypesDAO()
                self._logger.info("Database components initialized for CollectionTargetManager")
            except Exception as general_error:
                self._logger.error(f"Failed to initialize the database components: {general_error}")
                raise

    def _get_name_type_status_ids(
        self,
        collector_name: Optional[str] = None,
        collection_type: Optional[str] = None,
        collection_status: Optional[str] = None,
    ) -> Tuple[Optional[int], Optional[int], Optional[int]]:
        """Gets optional collector_name_id, optional collection_type_id and mandatory collection_status_id"""
        collector_name_id = collection_type_id = collection_status_id = None

        if collector_name:
            collector_obj = self._collector_names_dao.get_by_name(collector_name)
            if not (collector_obj and isinstance(collector_obj.id, int)):
                raise ValueError(f"Collector '{collector_name}' not found")
            collector_name_id = collector_obj.id

        if collection_type:
            collection_type_obj = self._collection_types_dao.get_by_name(collection_type)
            if not (collection_type_obj and isinstance(collection_type_obj.id, int)):
                raise ValueError(f"Collection type '{collection_type}' not found")
            collection_type_id = collection_type_obj.id

        if collection_status:
            collection_status_id = self._collection_statuses_dao.get_id_by_name(collection_status)
            if not isinstance(collection_status_id, int):
                raise ValueError(f"Collection status '{collection_status}' not found")

        return collector_name_id, collection_type_id, collection_status_id

    TargetsDict = Dict[str, Dict[str, Dict[str, int]]]

    def _get_collection_targets_from_database(
        self,
        collector_name: Optional[str] = None,
        collection_type: Optional[str] = None,
        language_code: Optional[str] = None,
        target_id: Optional[int] = None,
        collection_status: Optional[str] = None,
    ) -> TargetsDict:
        """
        Gets collection targets from database

        Returns:
            {"collection_type":
                {"language_code":
                    {"collection_name": collection_id}
                }
            }
        """

        self._lazy_database_init()

        try:
            name_type_status_ids = self._get_name_type_status_ids(
                collector_name,
                collection_type,
                collection_status,
            )
            collector_name_id, collection_type_id, collection_status_id = name_type_status_ids

            if collection_type_id and language_code:
                collection_targets = self._collection_targets_dao.get_by_type_and_language(
                    collection_type_id,
                    language_code,
                    collection_status_id,
                )
            elif collection_type_id:
                collection_targets = self._collection_targets_dao.get_by_collection_type_id(
                    collection_type_id,
                    collection_status_id,
                )
            elif (target_id and language_code) or target_id or language_code:
                pass
            else:
                collection_targets = self._collection_targets_dao.get_by_collector_name_id(
                    collector_name_id,
                    collection_status_id,
                )

            grouped_targets: Dict[str, Any] = {}

            for target in collection_targets:
                collection_type_obj = self._collection_types_dao.get_by_id(target.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                if type_name not in grouped_targets:
                    grouped_targets[type_name] = {}

                if target.language_code not in grouped_targets[type_name]:
                    grouped_targets[type_name][target.language_code] = {}

                grouped_targets[type_name][target.language_code][target.collection_name] = target.id

            self._logger.debug(f"Retrieved {len(collection_targets)} collection targets from database")
            return grouped_targets

        except Exception as general_error:
            self._logger.error(f"Error retrieving collection targets from database: {general_error}")
            return {}

    def get_uncollected_targets_by_type(
        self,
        collection_type: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Gets uncollected targets of a specific type for a specific collector, grouped by language"""

        self._lazy_database_init()

        try:
            _collector_name_id, collection_type_id, collection_status_id = self._get_name_type_status_ids(
                collection_type=collection_type,
                collection_status=CollectionStatusNames.NOT_COLLECTED.value,
            )

            collection_targets = self._collection_targets_dao.get_grouped_by_language(
                collection_type_id,
                collection_status_id,
            )

            result = {}
            for language_code, target_list in collection_targets.items():
                result[language_code] = [
                    {
                        "id": each_target.id,
                        "name": each_target.collection_name,
                        "collection_status_id": each_target.collection_status_id,
                    }
                    for each_target in target_list
                ]

            self._logger.info(f"Retrieved uncollected {collection_type} targets for {len(result)} languages")
            return result

        except Exception as general_error:
            self._logger.error(f"Error retrieving uncollected {collection_type} targets: {general_error}")
            return {}

    def get_collection_status_summary(self) -> Dict[str, Any]:
        """Gets summary of collection status across all types and languages"""

        self._lazy_database_init()

        try:
            all_targets = self._collection_targets_dao.get_all()
            all_statuses = self._collection_statuses_dao.get_all()
            all_types = self._collection_types_dao.get_all()

            status_map = {status.id: status.collection_status_name for status in all_statuses}
            type_map = {type_obj.id: type_obj.collection_type for type_obj in all_types}

            by_type_language_status = []
            status_counts = {}

            # Group targets by type, language, and status
            for target in all_targets:
                type_name = type_map.get(target.collection_type_id, "unknown")
                status_name = status_map.get(target.collection_status_id, "unknown")

                key = (type_name, target.language_code, status_name)
                if key not in status_counts:
                    status_counts[key] = 0
                status_counts[key] += 1

            # Convert to expected format
            for (type_name, language_code, status_name), count in status_counts.items():
                by_type_language_status.append(
                    {
                        "collection_type": type_name,
                        "language_code": language_code,
                        "collection_status_name": status_name,
                        "count": count,
                    },
                )

            # Calculate stats
            total_targets = len(all_targets)
            collected_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.COLLECTED.value
            )
            not_collected_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.NOT_COLLECTED.value
            )
            in_progress_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.IN_PROGRESS.value
            )
            failed_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.FAILED.value
            )
            needs_retry_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.NEEDS_RETRY.value
            )
            skipped_count = sum(
                1
                for t in all_targets
                if status_map.get(t.collection_status_id) == CollectionStatusNames.SKIPPED.value
            )

            summary = {
                "total_targets": total_targets,
                "collected": collected_count,
                "not_collected": not_collected_count,
                "in_progress": in_progress_count,
                "failed": failed_count,
                "needs_retry": needs_retry_count,
                "skipped": skipped_count,
                "collection_percentage": round((collected_count / total_targets * 100), 2)
                if total_targets > 0
                else 0,
            }

            status = {
                "by_type_language_status": by_type_language_status,
                "summary": summary,
            }

            self._logger.info("Retrieved collection status summary from database")
            return status

        except Exception as general_error:
            self._logger.error(f"Error retrieving collection status summary: {general_error}")
            return {"by_type_language_status": [], "summary": {}}

    def mark_target_as_collected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a collection target as collected"""

        self._lazy_database_init()

        try:
            collected_status_id = self._collection_statuses_dao.get_id_by_name(
                CollectionStatusNames.COLLECTED.value,
            )
            if collected_status_id is None:
                self._logger.error("'collected' status not found")
                return False

            success: bool = self._collection_targets_dao.update_collection_status_id(
                target_id,
                collected_status_id,
            )
            if success:
                self._logger.info(f"Marked target {target_id} as collected")
            return success

        except Exception as general_error:
            self._logger.error(f"Error marking target {target_id} as collected: {general_error}")
            return False

    def mark_target_as_uncollected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a collection target as uncollected"""

        self._lazy_database_init()

        try:
            not_collected_status_id = self._collection_statuses_dao.get_id_by_name(
                CollectionStatusNames.NOT_COLLECTED.value,
            )
            if not_collected_status_id is None:
                self._logger.error("'not_collected' status not found")
                return False

            success: bool = self._collection_targets_dao.update_collection_status_id(
                target_id,
                not_collected_status_id,
            )
            if success:
                self._logger.info(f"Marked target {target_id} as uncollected")
            return success

        except Exception as general_error:
            self._logger.error(f"Error marking target {target_id} as uncollected: {general_error}")
            return False

    def mark_target_as_in_progress(
        self,
        target_id: int,
    ) -> bool:
        """Marks a collection target as in progress"""

        self._lazy_database_init()

        try:
            in_progress_status_id = self._collection_statuses_dao.get_id_by_name(
                CollectionStatusNames.IN_PROGRESS.value,
            )
            if in_progress_status_id is None:
                self._logger.error("'in_progress' status not found")
                return False

            success: bool = self._collection_targets_dao.update_collection_status_id(
                target_id,
                in_progress_status_id,
            )
            if success:
                self._logger.info(f"Marked target {target_id} as in progress")
            return success

        except Exception as general_error:
            self._logger.error(f"Error marking target {target_id} as in progress: {general_error}")
            return False

    def mark_target_as_failed(
        self,
        target_id: int,
    ) -> bool:
        """Marks a collection target as failed"""

        self._lazy_database_init()

        try:
            failed_status_id = self._collection_statuses_dao.get_id_by_name(
                CollectionStatusNames.FAILED.value,
            )
            if failed_status_id is None:
                self._logger.error("'failed' status not found")
                return False

            success: bool = self._collection_targets_dao.update_collection_status_id(
                target_id,
                failed_status_id,
            )
            if success:
                self._logger.info(f"Marked target {target_id} as failed")
            return success

        except Exception as general_error:
            self._logger.error(f"Error marking target {target_id} as failed: {general_error}")
            return False

    def search_collection_targets(
        self,
        search_term: str,
    ) -> List[Dict[str, Any]]:
        """Search collection targets by their names"""

        self._lazy_database_init()

        try:
            collection_targets = self._collection_targets_dao.search_by_name(search_term)

            result = []
            for each_target in collection_targets:
                collection_type_obj = self._collection_types_dao.get_by_id(each_target.collection_type_id)
                type_name = collection_type_obj.collection_type if collection_type_obj else "unknown"

                collection_status_obj = self._collection_statuses_dao.get_by_id(
                    each_target.collection_status_id,
                )
                status_name = (
                    collection_status_obj.collection_status_name if collection_status_obj else "unknown"
                )

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

    """def get_combined_wikipedia_target_config(
        self,
        yaml_config: Dict[str, Any],
        collector_name: str,
        collection_type: Optional[List[str]] = None,
        language_code: Optional[List[str]] = None,
        target_id: Optional[List[int]] = None,
        collection_status: Optional[str] = None,
    ) -> Dict[str, Any]:
        Gets combined wikipedia targets and config from database and config.yml

        Note:
            Falls back to YAML only config if combining fails

        try:
            db_targets = self._get_collection_targets_from_database(
                collector_name=collector_name,
                collection_type=collection_type,
                language_code=language_code,
                target_id=target_id,
                collection_status=collection_status,
            )

            combined_config = yaml_config.copy()

            for key in db_targets:
                combined_config[key] = db_targets[key]

            combined_config["_database_info"] = {
                "total_types": len(db_targets),
                "last_updated": "from_database",
            }

            if self._logger:
                self._logger.info("Successfully combined YAML and Database configurations")
            return combined_config

        except Exception as general_error:
            if self._logger:
                self._logger.error(
                    f"Error getting combined Wikipedia target: {general_error} - falling back to YAML only",
                )
            return yaml_config"""

    def get_wikipedia_target_config(
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

        self._lazy_database_init()

        try:
            collector_name_id, _, collection_status_id = self._get_name_type_status_ids(
                collector_name=collector_name,
                collection_status=collection_status,
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
                    _, collection_type_id, _ = self._get_name_type_status_ids(
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
                    _, collection_type_id, _ = self._get_name_type_status_ids(
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
                collection_targets = [
                    target for target in all_targets if target.language_code in language_codes
                ]

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

    def test_database_connection(self) -> bool:
        """Tests if database connection is working"""

        try:
            self._lazy_database_init()
            return bool(self._db_connection.test_connection())
        except Exception as general_error:
            if self._logger:
                self._logger.error(f"Database connection test failed: {general_error}")
            return False

    def get_list_of_uncollected_types_by_collector_name(
        self,
        collector_name: str,
        collection_status: str,
        unique_types_only: bool,
    ) -> List[str]:
        """
        Gets list of either all or unique, uncollected collection types by the collector's name

        Returns:
            List: ["collection_type", ...]
        """
        try:
            result = []
            self._lazy_database_init()

            collector_name_id, _, status_id = self._get_name_type_status_ids(
                collector_name=collector_name,
                collection_status=collection_status,
            )

            uncollected_targets = self._collection_targets_dao.get_by_collector_name_id(
                collector_name_id=collector_name_id,
                collection_status_id=status_id,
                unique_languages_only=unique_types_only,
            )

            for uncollected in uncollected_targets:
                collection_type_obj = self._collection_types_dao.get_by_id(uncollected.collection_type_id)
                name_type = collection_type_obj.collection_type if collection_type_obj else "unknown"
                result.append(name_type)

            return result

        except Exception as general_error:
            self._logger.error(f"Error searching collection targets for '{collector_name}': {general_error}")
            return []

    def get_list_of_uncollected_language_codes_by_collector_name(
        self,
        collector_name: str,
        unique_types_only: bool,
        collection_status: str,
    ) -> List[str]:
        """
        Gets list of either all or unique, uncollected language codes by the collector's name

        Returns:
            List: ["language_code", ...]
        """
        try:
            result = []
            self._lazy_database_init()

            collector_name_id, _, not_collected_status_id = self._get_name_type_status_ids(
                collector_name=collector_name,
                collection_status=collection_status,
            )

            uncollected_targets = self._collection_targets_dao.get_by_collector_name_id(
                collector_name_id,
                not_collected_status_id,
                unique_types_only,
            )

            for target in uncollected_targets:
                result.append(target.language_code)

            return result

        except Exception as general_error:
            self._logger.error(f"Error searching collection targets for '{collector_name}': {general_error}")
            return []

    def get_collection_targets_by_multiple_ids(
        self,
        id_list: List[int],
    ) -> Optional[List[CollectionTargets]]:
        """Gets list of collection target objects by ID"""
        try:
            self._lazy_database_init()

            collection_target_objects: List[CollectionTargets] = self._collection_targets_dao.get_by_id(id)
            return collection_target_objects if collection_target_objects else None

        except Exception as general_error:
            self._logger.error(f"Error searching collection targets table for '{id_list}': {general_error}")
            return None

    def get_collection_targets_by_single_id(
        self,
        id: int,
    ) -> Optional[CollectionTargets]:
        """Gets collection target by ID"""
        try:
            self._lazy_database_init()

            collection_target_obj = self._collection_targets_dao.get_by_id(id)
            result: CollectionTargets = collection_target_obj[0]
            return result if result else None

        except Exception as general_error:
            self._logger.error(f"Error searching collection targets table for '{id}': {general_error}")
            return None
