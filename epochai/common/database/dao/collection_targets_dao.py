# ruff: noqa: E501
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from epochai.common.database.database import get_database
from epochai.common.database.models import CollectionTargets
from epochai.common.logging_config import get_logger


class CollectionTargetsDAO:
    """DAO for collection_targets table"""

    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_collection_target(
        self,
        collector_name_id: int,
        collection_type_id: int,
        language_code: str,
        collection_name: str,
        collection_status_id: int,
    ) -> Optional[int]:
        """
        Creates a new collection target entry

        Returns:
            ID of created config or None if failure
        """

        query = """
            INSERT INTO collection_targets
            (collector_name_id, collection_type_id, language_code, collection_name, collection_status_id, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            params = (
                collector_name_id,
                collection_type_id,
                language_code,
                collection_name,
                collection_status_id,
                current_timestamp,
                current_timestamp,
            )
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(f"Created collection target: '{collection_name}' ({language_code})")
                return result
            self.logger.error(
                f"Failed to create collection target: '{collection_name}' ({language_code})",
            )
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error creating collection target '{collection_name}' ({language_code}): {general_error}",
            )
            return None

    def get_by_id(
        self,
        target_id: int,
    ) -> Optional[CollectionTargets]:
        """Gets collection target by id"""

        query = """
            SELECT * FROM collection_targets WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (target_id,))
            if results:
                return CollectionTargets.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collection target by id {target_id}: {general_error}")
            return None

    def get_all(self) -> List[CollectionTargets]:
        """Gets all collection targets"""
        query = """
            Select * FROM collection_targets ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query)

            if results is None:
                self.logger.warning(
                    "Database query return None in get_all() - table might be empty/not exist",
                )
                return []

            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all collection targets: {general_error}")
            return []

    def get_by_collection_status(
        self,
        collection_status_name: str,
    ) -> List[CollectionTargets]:
        """Gets targets by collection status name"""

        query = """
            SELECT ct.* FROM collection_targets ct
            JOIN collection_statuses cs ON ct.collection_status_id = cs.id
            WHERE cs.collection_status_name = %s
            ORDER BY ct.created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_name,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets by collection status '{collection_status_name}': {general_error}",
            )
            return []

    def get_by_collection_status_id(
        self,
        collection_status_id: int,
    ) -> List[CollectionTargets]:
        """Gets targets by collection status ID"""

        query = """
            SELECT * FROM collection_targets WHERE collection_status_id = %s ORDER BY created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_status_id,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets by collection status ID '{collection_status_id}': {general_error}",
            )
            return []

    def get_uncollected_by_type_and_language(
        self,
        collection_types: str,
        language_code: str,
    ) -> List[CollectionTargets]:
        """
        Gets uncollected targets by their collection type and language
        """

        query = """
            SELECT ct.*
            FROM collection_targets ct
            JOIN collection_types cty ON ct.collection_type_id = cty.id
            JOIN collection_statuses cs ON ct.collection_status_id = cs.id
            WHERE cty.collection_type = %s
            AND ct.language_code = %s
            AND cs.collection_status_name = 'not_collected'
            ORDER BY ct.created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_types, language_code))
            targets = [CollectionTargets.from_dict(row) for row in results]

            return targets

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected {collection_types} targets for language code '{language_code}': {general_error}",
            )
            return []

    def get_uncollected_language_codes_by_collector_name(
        self,
        collector_name: str,
        unique_languages_only: bool,
    ) -> List[CollectionTargets]:
        """Gets uncollected targets by their language code"""

        if unique_languages_only:
            query = """
                SELECT DISTINCT ON (ct.language_code) ct.*
                FROM collection_targets ct
                JOIN collector_names cn ON ct.collector_name_id = cn.id
                JOIN collection_statuses cs ON ct.collection_status_id = cs.id
                WHERE cn.collector_name = %s
                AND cs.collection_status_name = 'not_collected'
                ORDER BY ct.language_code, ct.created_at ASC
            """
        else:
            query = """
                SELECT ct.*
                FROM collection_targets ct
                JOIN collector_names cn ON ct.collector_name_id = cn.id
                JOIN collection_statuses cs ON ct.collection_status_id = cs.id
                WHERE cn.collector_name = %s
                AND cs.collection_status_name = 'not_collected'
                ORDER BY ct.language_code, ct.created_at ASC
            """

        try:
            results = self.db.execute_select_query(query, (collector_name,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected targets for collector '{collector_name}': {general_error}",
            )
            return []

    def get_uncollected_by_type(
        self,
        collection_type: str,
    ) -> List[CollectionTargets]:
        """Gets all uncollected targets by type across all languages"""

        query = """
            SELECT ct.*
            FROM collection_targets ct
            JOIN collection_types cty ON ct.collection_type_id = cty.id
            JOIN collection_statuses cs ON ct.collection_status_id = cs.id
            WHERE cty.collection_type = %s
            AND cs.collection_status_name = 'not_collected'
            ORDER BY ct.language_code, ct.created_at ASC
        """

        try:
            results = self.db.execute_select_query(query, (collection_type,))
            targets = [CollectionTargets.from_dict(row) for row in results]

            self.logger.info(
                f"Found {len(targets)} uncollected {collection_type} targets across all languages",
            )
            return targets

        except Exception as general_error:
            self.logger.error(f"Error getting uncollected {collection_type} targets: {general_error}")
            return []

    def get_uncollected_grouped_by_language(
        self,
        collection_type: str,
    ) -> Dict[str, List[CollectionTargets]]:
        """
        Gets all uncolected targets grouped by language for easier processing
        """
        targets = self.get_uncollected_by_type(collection_type)
        grouped: Dict[str, List[CollectionTargets]] = {}

        for config in targets:
            if config.language_code not in grouped:
                grouped[config.language_code] = []
            grouped[config.language_code].append(config)

        return grouped

    def update_collection_status(
        self,
        target_id: int,
        collection_status_name: str,
    ) -> bool:
        """Updates the collection status of a target by status name"""

        query = """
            UPDATE collection_targets
            SET collection_status_id = (
                SELECT id FROM collection_statuses
                WHERE collection_status_name = %s
            ),
            updated_at = %s
            WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(
                query,
                (collection_status_name, datetime.now(), target_id),
            )

            if affected_rows > 0:
                self.logger.info(f"Updated target {target_id} status to '{collection_status_name}'")
                return True
            self.logger.warning(f"No target found with id '{target_id}' to update status")
            return False

        except Exception as general_error:
            self.logger.error(
                f"Error updating target {target_id} status to '{collection_status_name}': {general_error}",
            )
            return False

    def mark_as_collected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as collected"""
        return self.update_collection_status(target_id, "collected")

    def mark_as_uncollected(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as uncollected"""
        return self.update_collection_status(target_id, "not_collected")

    def mark_as_in_progress(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as in progress"""
        return self.update_collection_status(target_id, "in_progress")

    def mark_as_failed(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as failed"""
        return self.update_collection_status(target_id, "failed")

    def mark_as_needs_retry(
        self,
        target_id: int,
    ) -> bool:
        """Marks a row as needing retry"""
        return self.update_collection_status(target_id, "needs_retry")

    def bulk_create_collection_targets(
        self,
        collection_targets: List[Tuple[int, int, str, str, int]],
    ) -> int:
        """
        Bulk creates multiple targets

        Returns:
            Number of successfully created targets
        """

        if not collection_targets:
            return 0

        query = """
            INSERT INTO collection_targets
            (collector_name_id, collection_type_id, language_code, collection_name, collection_status_id, updated_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """

        try:
            operations = []
            now = datetime.now()

            for config_data in collection_targets:
                (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    collection_status_id,
                ) = config_data
                params = (
                    collector_name_id,
                    collection_type_id,
                    language_code,
                    collection_name,
                    collection_status_id,
                    now,
                    now,
                )
                operations.append((query, params))

            success = self.db.execute_transaction(operations)

            if success:
                self.logger.info(f"Successfully bulk created {len(collection_targets)} collection targets")
                return len(collection_targets)
            self.logger.error("Failed to bulk create collection targets")
            return 0

        except Exception as general_error:
            self.logger.error(f"Error bulk creating target: {general_error}")
            return 0

    def get_collection_status(self) -> Dict[str, Any]:
        """Gets statistics about collection status"""
        query = """
            SELECT
                cty.collection_type,
                ct.language_code,
                cs.collection_status_name,
                COUNT(*) as count
            FROM collection_targets ct
            JOIN collection_types cty ON ct.collection_type_id = cty.id
            JOIN collection_statuses cs ON ct.collection_status_id = cs.id
            GROUP BY cty.collection_type, ct.language_code, cs.collection_status_name
            ORDER BY cty.collection_type, ct.language_code, cs.collection_status_name
        """

        try:
            results = self.db.execute_select_query(query)

            # Calculate summary statistics
            total_targets = sum(row["count"] for row in results)
            collected_count = sum(
                row["count"] for row in results if row["collection_status_name"] == "collected"
            )
            not_collected_count = sum(
                row["count"] for row in results if row["collection_status_name"] == "not_collected"
            )
            in_progress_count = sum(
                row["count"] for row in results if row["collection_status_name"] == "in_progress"
            )
            failed_count = sum(row["count"] for row in results if row["collection_status_name"] == "failed")
            needs_retry_count = sum(
                row["count"] for row in results if row["collection_status_name"] == "needs_retry"
            )
            skipped_count = sum(row["count"] for row in results if row["collection_status_name"] == "skipped")

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

            stats = {
                "by_type_language_status": results,
                "summary": summary,
            }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting collection stats: {general_error}")
            return {"by_type_language_status": [], "summary": {}}

    def delete_config(
        self,
        target_id: int,
    ) -> bool:
        """Deletes a collection target"""

        query = """
            DELETE FROM collection_targets WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (target_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted collection target {target_id}")
                return True
            self.logger.warning(f"No target found with id {target_id} to delete")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting target {target_id}: {general_error}")
            return False

    def search_by_name(
        self,
        search_term: str,
    ) -> List[CollectionTargets]:
        """Search targets by collection name"""

        query = """
            SELECT * FROM collection_targets WHERE collection_name ILIKE %s ORDER BY collection_name
        """

        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching targets by name '{search_term}': {general_error}")
            return []

    def get_by_collector_and_type(
        self,
        collector_name: str,
        collection_type: str,
    ) -> List[CollectionTargets]:
        """Gets targets by collector name and collection type"""

        query = """
            SELECT ct.*
            FROM collection_targets ct
            JOIN collector_names cn ON ct.collector_name_id = cn.id
            JOIN collection_types cty ON ct.collection_type_id = cty.id
            WHERE cn.collector_name = %s AND cty.collection_type = %s
            ORDER BY ct.language_code, ct.collection_name
        """

        try:
            results = self.db.execute_select_query(query, (collector_name, collection_type))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting targets for collector '{collector_name}' and type '{collection_type}': {general_error}",
            )
            return []

    def get_uncollected_by_collector_name(
        self,
        collector_name: str,
        unique_types_only: bool,
    ) -> List[CollectionTargets]:
        """Gets uncollected targets by collector name"""

        if unique_types_only:
            query = """
                SELECT DISTINCT ON (ct.collection_type_id) ct.*
                FROM collection_targets ct
                JOIN collector_names cn ON ct.collector_name_id = cn.id
                JOIN collection_statuses cs ON ct.collection_status_id = cs.id
                WHERE cn.collector_name = %s
                AND cs.collection_status_name = 'not_collected'
                ORDER BY ct.collection_type_id, ct.created_at ASC
            """
        else:
            query = """
                SELECT ct.*
                FROM collection_targets ct
                JOIN collector_names cn ON ct.collector_name_id = cn.id
                JOIN collection_statuses cs ON ct.collection_status_id = cs.id
                WHERE cn.collector_name = %s
                AND cs.collection_status_name = 'not_collected'
                ORDER BY ct.language_code, ct.created_at ASC
            """

        try:
            results = self.db.execute_select_query(query, (collector_name,))
            return [CollectionTargets.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected targets for collector '{collector_name}': {general_error}",
            )
            return []

    def get_collection_status_id_by_name(self, status_name: str) -> Optional[int]:
        """Helper method to get collection status ID by name"""
        query = """
            SELECT id FROM collection_statuses WHERE collection_status_name = %s
        """

        try:
            results = self.db.execute_select_query(query, (status_name,))
            if results:
                return int(results[0]["id"])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting collection status ID for '{status_name}': {general_error}")
            return None
