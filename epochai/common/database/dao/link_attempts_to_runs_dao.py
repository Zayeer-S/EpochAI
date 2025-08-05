from typing import List, Optional

from epochai.common.database.database import get_database
from epochai.common.logging_config import get_logger


class LinkAttemptsToRunsDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_link(
        self,
        collection_attempt_id: int,
        run_collection_metadata_id: int,
    ) -> Optional[int]:
        """Create a link between an attempt and a run"""

        query = """
            INSERT INTO link_attempts_to_runs
            (collection_attempt_id, run_collection_metadata_id)
            VALUES (%s, %s)
            RETURNING id
        """

        try:
            params = (collection_attempt_id, run_collection_metadata_id)
            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Linked attempt {collection_attempt_id} to run {run_collection_metadata_id}",
                )
                return result
            self.logger.error(
                f"Failed to link attempt {collection_attempt_id} to run {run_collection_metadata_id}",
            )
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error linking attempt {collection_attempt_id} to run {run_collection_metadata_id}: {general_error}",  # noqa
            )
            return None

    def bulk_link_attempts_to_run(
        self,
        collection_attempt_ids: List[int],
        run_collection_metadata_id: int,
    ) -> int:
        """Bulk link multiple attempts to one run"""
        if not collection_attempt_ids:
            return 0

        query = """
            INSERT INTO link_attempts_to_runs
            (collection_attempt_id, run_collection_metadata_id)
            VALUES (%s, %s)
        """

        try:
            operations = []
            for attempt_id in collection_attempt_ids:
                params = (attempt_id, run_collection_metadata_id)
                operations.append((query, params))

            success = self.db.execute_transaction(operations)

            if success:
                self.logger.info(
                    f"Bulk linked {len(collection_attempt_ids)} attempts to run {run_collection_metadata_id}",
                )
                return len(collection_attempt_ids)
            self.logger.error(f"Failed to bulk link attempts to run {run_collection_metadata_id}")
            return 0

        except Exception as general_error:
            self.logger.error(
                f"Error bulk linking attempts to run {run_collection_metadata_id}: {general_error}",
            )
            return 0

    def get_attempts_for_run(
        self,
        run_collection_metadata_id: int,
    ) -> List[int]:
        """Gets all attempt IDs linked to a specific run"""

        query = """
            SELECT collection_attempt_id FROM link_attempts_to_runs WHERE run_collection_metadata_id = %s
        """

        try:
            results = self.db.execute_select_query(query, (run_collection_metadata_id,))
            return [row["collection_attempt_id"] for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting attempts for run {run_collection_metadata_id}: {general_error}")
            return []

    def get_runs_for_attempt(
        self,
        collection_attempt_id: int,
    ) -> List[int]:
        """Gets all run IDs that include a specific attempt"""
        query = (
            "SELECT run_collection_metadata_id FROM link_attempts_to_runs WHERE collection_attempt_id = %s"
        )

        try:
            results = self.db.execute_select_query(query, (collection_attempt_id,))
            return [row["run_collection_metadata_id"] for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting runs for attempt {collection_attempt_id}: {general_error}")
            return []

    def delete_links_for_run(
        self,
        run_collection_metadata_id: int,
    ) -> int:
        """Delete all links for a specific run"""
        query = """
            DELETE FROM link_attempts_to_runs WHERE run_collection_metadata_id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (run_collection_metadata_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} links for run {run_collection_metadata_id}")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting links for run {run_collection_metadata_id}: {general_error}")
            return 0
