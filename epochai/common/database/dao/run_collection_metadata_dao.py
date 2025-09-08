from datetime import datetime
import json
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import RunCollectionMetadata
from epochai.common.logging_config import get_logger


class RunCollectionMetadataDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_run_metadata(
        self,
        collection_attempt_id: int,
        run_type_id: int,
        run_status_id: int,
        attempts_successful: int = 0,
        attempts_failed: int = 0,
        config_used: Optional[Dict[str, Any]] = None,
        completed_at: Optional[datetime] = None,
    ) -> Optional[int]:
        """Create a new collection run metadata record"""

        query = """
            INSERT INTO run_collection_metadata
            (collection_attempt_id, run_type_id, run_status_id, attempts_successful,
             attempts_failed, config_used, completed_at, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            config_json = json.dumps(config_used) if config_used else None

            params = (
                collection_attempt_id,
                run_type_id,
                run_status_id,
                attempts_successful,
                attempts_failed,
                config_json,
                completed_at,
                current_timestamp,
            )

            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Created run metadata for attempt {collection_attempt_id}: {attempts_successful} successful, {attempts_failed} failed",  # noqa
                )
                return result
            self.logger.error(f"Failed to create run metadata for attempt {collection_attempt_id}")
            return None

        except Exception as general_error:
            self.logger.error(
                f"Error creating run metadata for attempt {collection_attempt_id}: {general_error}",
            )
            return None

    def update_run_completion(
        self,
        run_id: int,
        run_status_id: int,
        attempts_successful: int,
        attempts_failed: int,
        completed_at: Optional[datetime] = None,
    ) -> bool:
        """Update run completion statistics"""

        query = """
            UPDATE run_collection_metadata
            SET run_status_id = %s, attempts_successful = %s, attempts_failed = %s, completed_at = %s
            WHERE id = %s
        """

        try:
            completion_time = completed_at or datetime.now()
            params = (run_status_id, attempts_successful, attempts_failed, completion_time, run_id)

            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(
                    f"Updated run {run_id} completion: {attempts_successful} successful, {attempts_failed} failed",
                )
                return True
            self.logger.warning(f"No run found with ID {run_id} to update")
            return False

        except Exception as general_error:
            self.logger.error(f"Error updating run completion for run {run_id}: {general_error}")
            return False

    def get_by_run_type(
        self,
        run_type_name: str,
    ) -> List[RunCollectionMetadata]:
        """Gets runs by type"""

        query = """
            SELECT rcm.*
            FROM run_collection_metadata rcm
            JOIN run_types rt ON rcm.run_type_id = rt.id
            WHERE rt.run_type_name = %s
            ORDER BY rcm.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (run_type_name,))
            runs = [RunCollectionMetadata.from_dict(row) for row in results]

            self.logger.info(f"Found {len(runs)} runs of type '{run_type_name}'")
            return runs

        except Exception as general_error:
            self.logger.error(f"Error getting runs by type '{run_type_name}': {general_error}")
            return []

    def get_by_run_status(
        self,
        run_status_name: str,
    ) -> List[RunCollectionMetadata]:
        """Gets runs by status"""

        query = """
            SELECT rcm.*
            FROM run_collection_metadata rcm
            JOIN run_statuses rs ON rcm.run_status_id = rs.id
            WHERE rs.run_status_name = %s
            ORDER BY rcm.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (run_status_name,))
            runs = [RunCollectionMetadata.from_dict(row) for row in results]

            self.logger.info(f"Found {len(runs)} runs with status '{run_status_name}'")
            return runs

        except Exception as general_error:
            self.logger.error(f"Error getting runs by status '{run_status_name}': {general_error}")
            return []

    def get_run_performance_stats(self) -> Dict[str, Any]:
        """Gets performance statistics across all runs"""

        query = """
            SELECT
                rt.run_type_name,
                rs.run_status_name,
                COUNT(*) as run_count,
                AVG(rcm.attempts_successful) as avg_successful,
                AVG(rcm.attempts_failed) as avg_failed,
                AVG(EXTRACT(EPOCH FROM (rcm.completed_at - rcm.created_at))/60) as avg_duration_minutes
            FROM run_collection_metadata rcm
            JOIN run_types rt ON rcm.run_type_id = rt.id
            JOIN run_statuses rs ON rcm.run_status_id = rs.id
            WHERE rcm.completed_at IS NOT NULL
            GROUP BY rt.run_type_name, rs.run_status_name
            ORDER BY rt.run_type_name, rs.run_status_name
        """

        try:
            results = self.db.execute_select_query(query)

            summary: Dict[str, Any] = {
                "total_runs": sum(row["run_count"] for row in results),
                "avg_success_rate": 0,
                "avg_duration_minutes": 0,
            }

            stats = {
                "performance_by_type_and_status": results,
                "summary": summary,
            }

            if results:
                total_successful = sum(row["avg_successful"] * row["run_count"] for row in results)
                total_attempts = sum((row["avg_successful"] + row["avg_failed"]) * row["run_count"] for row in results)

                if total_attempts > 0:
                    summary["avg_success_rate"] = round((total_successful / total_attempts * 100), 2)

                weighted_duration = sum(
                    row["avg_duration_minutes"] * row["run_count"] for row in results if row["avg_duration_minutes"]
                )
                total_runs_with_duration = sum(row["run_count"] for row in results if row["avg_duration_minutes"])

                if total_runs_with_duration > 0:
                    summary["avg_duration_minutes"] = round(
                        weighted_duration / total_runs_with_duration,
                        2,
                    )

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting run performance stats: {general_error}")
            return {"performance_by_type_and_status": [], "summary": {}}
