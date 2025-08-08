from datetime import datetime
import json
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import RawData
from epochai.common.logging_config import get_logger


class RawDataDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_raw_data(
        self,
        collection_attempt_id: int,
        raw_data_metadata_schema_id: int,
        title: str,
        language_code: str,
        url: Optional[str] = None,
        validation_status_id: int = 0,
        validation_error: Optional[Dict[str, Any]] = None,
        filepath_of_save: Optional[str] = None,
    ) -> Optional[int]:
        """
        Creates a new raw data record

        Returns:
            The id of created raw data or None if it fails
        """

        query = """
            INSERT INTO raw_data
            (collection_attempt_id, raw_data_metadata_schema_id , title, language_code,
             url, validation_status_id, validation_error, filepath_of_save, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            validation_error_json = json.dumps(validation_error) if validation_error else None

            params = (
                collection_attempt_id,
                raw_data_metadata_schema_id,
                title,
                language_code,
                url,
                validation_status_id,
                validation_error_json,
                filepath_of_save,
                current_timestamp,
            )

            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Created raw data: '{title}' (attempt_id: {collection_attempt_id})",
                )
                return result
            self.logger.error(f"Failed to create raw data: '{title}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating raw data '{title}': {general_error}")
            return None

    def get_by_id(
        self,
        raw_data_id: int,
    ) -> Optional[RawData]:
        """Gets raw data by id"""

        query = """
            SELECT * FROM raw_data WHERE id = %s
        """

        try:
            results = self.db.execute_select_query(query, (raw_data_id,))
            if results:
                return RawData.from_dict(results[0])
            return None

        except Exception as general_error:
            self.logger.error(f"Error getting raw data by id {raw_data_id}: {general_error}")
            return None

    def get_all(
        self,
        limit: Optional[int] = None,
    ) -> List[RawData]:
        """Gets all raw datas with an optional limit"""

        query = """
            SELECT * FROM raw_data ORDER BY created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query)
            return [RawData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting all raw datas: {general_error}")
            return []

    def get_by_attempt_id(
        self,
        collection_attempt_id: int,
    ) -> List[RawData]:
        """Gets all raw data for a specific collection attempt"""

        query = """
            SELECT * FROM raw_data WHERE collection_attempt_id = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (collection_attempt_id,))
            return [RawData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting raw data for attempt {collection_attempt_id}: {general_error}")
            return []

    def get_by_validation_status(
        self,
        validation_status_name: str,
    ) -> List[RawData]:
        """Gets raw data by validation status"""

        query = """
            SELECT rd.*
            FROM raw_data rd
            JOIN validation_statuses vs ON rd.validation_status_id = vs.id
            WHERE vs.validation_status_name = %s
            ORDER BY rd.created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (validation_status_name,))
            raw_data = [RawData.from_dict(row) for row in results]

            self.logger.info(
                f"Found {len(raw_data)} raw data with validation status '{validation_status_name}'",
            )
            return raw_data

        except Exception as general_error:
            self.logger.error(
                f"Error getting raw data by validation status '{validation_status_name}': {general_error}",
            )
            return []

    def get_invalid_rows(self) -> List[RawData]:
        """Gets all raw data rows that failed validation"""
        return self.get_by_validation_status("invalid")

    def get_valid_rows(self) -> List[RawData]:
        """Gets all raw data rows that passed validation"""
        return self.get_by_validation_status("valid")

    def get_pending_validation(self) -> List[RawData]:
        """Gets all raw data rows pending validation"""
        return self.get_by_validation_status("pending")

    def update_single_validation_status(
        self,
        raw_data_id: int,
        validation_status_id: int,
        validation_error: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Update validation status and error for a row"""

        query = """
            UPDATE raw_data
            SET validation_status_id = %s, validation_error = %s
            WHERE id = %s
        """

        try:
            validation_error_json = json.dumps(validation_error) if validation_error else None
            params = (validation_status_id, validation_error_json, raw_data_id)

            affected_rows = self.db.execute_update_delete_query(query, params)

            if affected_rows > 0:
                self.logger.info(f"Updated validation status for raw data row {raw_data_id}")
                return True
            self.logger.warning(f"No raw data row found with id {raw_data_id} to update")
            return False

        except Exception as general_error:
            self.logger.error(
                f"Error updating validation status for raw data row {raw_data_id}: {general_error}",
            )
            return False

    def search_by_title(
        self,
        search_term: str,
    ) -> List[RawData]:
        """Search raw data by title (partial match)"""

        query = """
            SELECT * FROM raw_data WHERE title ILIKE %s ORDER BY created_at DESC
        """

        try:
            search_pattern = f"%{search_term}%"
            results = self.db.execute_select_query(query, (search_pattern,))
            return [RawData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error searching raw data by title '{search_term}': {general_error}")
            return []

    def get_rows_with_details(
        self,
        limit: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """
        Gets raw data with all relevant details
        """

        query = """
            SELECT
                rd.*,
                vs.validation_status_name,
                ca.search_term_used,
                ca.language_code_used,
                cfg.collection_name,
                ct.collection_type
            FROM raw_data rd
            LEFT JOIN validation_statuses vs ON rd.validation_status_id = vs.id
            LEFT JOIN collection_attempts ca ON rd.collection_attempt_id = ca.id
            LEFT JOIN collection_targets cfg ON ca.collection_target_id = cfg.id
            LEFT JOIN collection_types ct ON cfg.collection_type_id = ct.id
            ORDER BY rd.created_at DESC
        """

        if limit:
            query += f" LIMIT {limit}"

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Retrieved {len(results)} raw data with details")
            return results

        except Exception as general_error:
            self.logger.error(f"Error getting raw data with details: {general_error}")
            return []

    def get_raw_data_statistics(self) -> Dict[str, Any]:
        """Gets comprehensive statistics about the raw data records"""

        basic_stats_query = """
            SELECT
                COUNT(*) as total_records,
                COUNT(DISTINCT collection_attempt_id) as unique_attempts,
                COUNT(DISTINCT language_code) as unique_languages
            FROM raw_data
        """

        validation_stats_query = """
            SELECT
                vs.validation_status_name,
                COUNT(*) as record_count
            FROM raw_data rd
            JOIN validation_statuses vs ON rd.validation_status_id = vs.id
            GROUP BY vs.validation_status_name
            ORDER BY record_count DESC
        """

        try:
            basic_stats = self.db.execute_select_query(basic_stats_query)
            validation_stats = self.db.execute_select_query(validation_stats_query)

            summary: Dict[str, Any] = {}

            stats = {
                "basic_stats": basic_stats[0] if basic_stats else {},
                "by_validation_status": validation_stats,
                "summary": summary,
            }

            if basic_stats and basic_stats[0]["total_records"]:
                total_records = basic_stats[0]["total_records"]

                for status_row in validation_stats:
                    status_name = status_row["validation_status_name"]
                    record_count = status_row["record_count"]
                    percentage = round((record_count / total_records * 100), 2)

                    summary[status_name] = {
                        "count": record_count,
                        "percentage": percentage,
                    }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting raw data statistics: {general_error}")
            return {"basic_stats": {}, "by_validation_status": [], "summary": {}}

    def get_recent_contents(
        self,
        hours: int = 24,
    ) -> List[RawData]:
        """Gets contents collected in the last X hours"""

        query = """
            SELECT * FROM raw_data
            WHERE created_at >= %s
            ORDER BY created_at DESC
        """

        try:
            from datetime import timedelta

            cutoff_time = datetime.now() - timedelta(hours=hours)
            results = self.db.execute_select_query(query, (cutoff_time,))
            return [RawData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting recent contents from last {hours} hours: {general_error}")
            return []

    def get_duplicate_titles(self) -> List[Dict[str, Any]]:
        """Find contents with duplicate titles for cleanup"""

        query = """
            SELECT title, COUNT(*) as duplicate_count,
                   ARRAY_AGG(id) as raw_data_ids,
                   MIN(created_at) as first_created,
                   MAX(created_at) as last_created
            FROM raw_data
            GROUP BY title
            HAVING COUNT(*) > 1
            ORDER BY duplicate_count DESC
        """

        try:
            results = self.db.execute_select_query(query)
            self.logger.info(f"Found {len(results)} titles with duplicates")
            return results

        except Exception as general_error:
            self.logger.error(f"Error finding duplicate titles: {general_error}")
            return []

    def delete_single_row(
        self,
        raw_data_id: int,
    ) -> bool:
        """Delete a raw data record"""

        query = """
            DELETE FROM raw_data WHERE id = %s
        """

        try:
            affected_rows = self.db.execute_update_delete_query(query, (raw_data_id,))

            if affected_rows > 0:
                self.logger.info(f"Deleted raw data {raw_data_id}")
                return True
            self.logger.warning(f"No raw data found with id {raw_data_id} to delete")
            return False

        except Exception as general_error:
            self.logger.error(f"Error deleting raw data {raw_data_id}: {general_error}")
            return False

    def delete_multiple_rows(
        self,
        days_old: int,
    ) -> int:
        """Delete raw data older than specified days"""

        query = """
            DELETE FROM raw_data
            WHERE created_at < %s
        """

        try:
            from datetime import timedelta

            cutoff_date = datetime.now() - timedelta(days=days_old)
            affected_rows = self.db.execute_update_delete_query(query, (cutoff_date,))

            if affected_rows > 0:
                self.logger.info(f"Deleted {affected_rows} raw data older than {days_old} days")
            else:
                self.logger.info(f"No raw data older than {days_old} days found to delete")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error deleting old raw data: {general_error}")
            return 0

    def bulk_update_validation_status(
        self,
        raw_data_ids: List[int],
        validation_status_id: int,
    ) -> int:
        """Bulk update validation status for multiple raw data rows"""

        if not raw_data_ids:
            return 0

        placeholders = ",".join(["%s"] * len(raw_data_ids))

        query = f"""
            UPDATE raw_data
            SET validation_status_id = %s
            WHERE id IN ({placeholders})
        """

        try:
            params = [validation_status_id, *raw_data_ids]
            affected_rows = self.db.execute_update_delete_query(query, tuple(params))

            if affected_rows > 0:
                self.logger.info(f"Bulk updated validation status for {affected_rows} raw data")

            return affected_rows

        except Exception as general_error:
            self.logger.error(f"Error bulk updating validation status: {general_error}")
            return 0

    def get_by_filepath(
        self,
        filepath: str,
    ) -> List[RawData]:
        """Gets raw data by their save filepath (only works if saved locally)"""

        query = """
            SELECT * FROM raw_data WHERE filepath_of_save = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (filepath,))
            return [RawData.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting raw data by filepath '{filepath}': {general_error}")
            return []
