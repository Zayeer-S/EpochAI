from typing import Any, Dict, List

from epochai.common.database.dao.collection_statuses_dao import CollectionStatusesDAO
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.database.dao.collection_types_dao import CollectionTypesDAO
from epochai.common.database.dao.collector_names_dao import CollectorNamesDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.database_utils import DatabaseUtils
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class CollectionReportsService:
    @handle_initialization_errors(f"{__name__} initialization")
    def __init__(self):
        try:
            self._logger = get_logger(__name__)
            self._collection_targets_dao = CollectionTargetsDAO()
            self._collection_types_dao = CollectionTypesDAO()
            self._collector_names_dao = CollectorNamesDAO()
            self._collection_statuses_dao = CollectionStatusesDAO()
            self._database_utils = DatabaseUtils()
            self._logger.debug(f"Database components initialized for {CollectionReportsService.__name__}")
        except Exception as general_error:
            raise RuntimeError("Failed to initialize the database components") from general_error

    @handle_generic_errors_gracefully("retrieval of collection targets", {})
    def get_targets_by_type_and_status(
        self,
        collection_type: str,
        collection_status_name: str,
    ) -> Dict[str, List[Dict[str, Any]]]:
        """Gets uncollected targets of a specific type for a specific collector and collection status, grouped by language"""

        _, collection_type_id, collection_status_id = self._database_utils.get_name_type_status_ids(
            collection_type=collection_type,
            collection_status_name=collection_status_name,
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

    @handle_generic_errors_gracefully("retrieval of collection types", [])
    def get_collection_type_list(
        self,
        collector_name: str,
        collection_status_name: str,
        unique_types_only: bool,
    ) -> List[str]:
        """
        Gets list of either all or unique, collection types by the collector's name and their collection status

        Returns:
            List: ["collection_type", ...]
        """
        result = []

        collector_name_id, _, collection_status_id = self._database_utils.get_name_type_status_ids(
            collector_name=collector_name,
            collection_status_name=collection_status_name,
        )

        uncollected_targets = self._collection_targets_dao.get_by_collector_name_id(
            collector_name_id=collector_name_id,
            collection_status_id=collection_status_id,
            unique_languages_only=unique_types_only,
        )

        for uncollected in uncollected_targets:
            collection_type_obj = self._collection_types_dao.get_by_id(uncollected.collection_type_id)
            name_type = collection_type_obj.collection_type if collection_type_obj else "unknown"
            result.append(name_type)

        return result

    @handle_generic_errors_gracefully("retrieval of language code list", [])
    def get_language_code_list(
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
        result = []

        collector_name_id, _, collection_status_id = self._database_utils.get_name_type_status_ids(
            collector_name=collector_name,
            collection_status_name=collection_status,
        )

        uncollected_targets = self._collection_targets_dao.get_by_collector_name_id(
            collector_name_id,
            collection_status_id,
            unique_types_only,
        )

        for target in uncollected_targets:
            result.append(target.language_code)

        return result

    @handle_generic_errors_gracefully("retrieving collection status summary", {"by_type_language_status": [], "summary": {}})
    def get_collection_status_summary(self) -> Dict[str, Any]:
        """Gets summary of collection status across all types and languages"""

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
        from epochai.common.enums import CollectionStatusNames

        total_targets = len(all_targets)
        collected_count = sum(
            1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.COLLECTED.value
        )
        not_collected_count = sum(
            1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.NOT_COLLECTED.value
        )
        in_progress_count = sum(
            1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.IN_PROGRESS.value
        )
        failed_count = sum(1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.FAILED.value)
        needs_retry_count = sum(
            1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.NEEDS_RETRY.value
        )
        skipped_count = sum(
            1 for t in all_targets if status_map.get(t.collection_status_id) == CollectionStatusNames.SKIPPED.value
        )

        summary = {
            "total_targets": total_targets,
            "collected": collected_count,
            "not_collected": not_collected_count,
            "in_progress": in_progress_count,
            "failed": failed_count,
            "needs_retry": needs_retry_count,
            "skipped": skipped_count,
            "collection_percentage": round((collected_count / total_targets * 100), 2) if total_targets > 0 else 0,
        }

        status = {
            "by_type_language_status": by_type_language_status,
            "summary": summary,
        }

        self._logger.info("Retrieved collection status summary from database")
        return status
