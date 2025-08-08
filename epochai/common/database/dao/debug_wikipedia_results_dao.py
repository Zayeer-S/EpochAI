from datetime import datetime, timedelta
import json
from typing import Any, Dict, List, Optional

from epochai.common.database.database import get_database
from epochai.common.database.models import DebugWikipediaResults
from epochai.common.logging_config import get_logger


class DebugWikipediaResultsDAO:
    def __init__(self):
        self.db = get_database()
        self.logger = get_logger(__name__)

    def create_debug_result(
        self,
        collection_target_id: int,
        search_term_used: str,
        language_code: str,
        test_status: str,
        search_results_found: List[str],
        error_message: str = "",
        test_duration: int = 0,
    ) -> Optional[int]:
        """Creates a new debug test result"""

        query = """
            INSERT INTO debug_wikipedia_results
            (collection_target_id, search_term_used, language_code, test_status,
             search_results_found, error_message, test_duration, created_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id
        """

        try:
            current_timestamp = datetime.now()
            search_results_json = json.dumps(search_results_found)

            params = (
                collection_target_id,
                search_term_used,
                language_code,
                test_status,
                search_results_json,
                error_message,
                test_duration,
                current_timestamp,
            )

            result = self.db.execute_insert_query(query, params)

            if result:
                self.logger.info(
                    f"Created debug result for '{search_term_used}' ({language_code}): {test_status}",
                )
                return result
            self.logger.error(f"Failed to create debug result for '{search_term_used}'")
            return None

        except Exception as general_error:
            self.logger.error(f"Error creating debug result for '{search_term_used}': {general_error}")
            return None

    def get_by_test_status(
        self,
        test_status: str,
    ) -> List[DebugWikipediaResults]:
        """Gets debug results by test status"""

        query = """
            SELECT * FROM debug_wikipedia_results WHERE test_status = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (test_status,))
            debug_results = [DebugWikipediaResults.from_dict(row) for row in results]

            self.logger.info(f"Found {len(debug_results)} debug results with status '{test_status}'")
            return debug_results

        except Exception as general_error:
            self.logger.error(f"Error getting debug results by status '{test_status}': {general_error}")
            return []

    def get_failed_tests(self) -> List[DebugWikipediaResults]:
        """Gets all failed debug tests"""
        return self.get_by_test_status("failed")

    def get_successful_tests(self) -> List[DebugWikipediaResults]:
        """Gets all successful debug tests"""
        return self.get_by_test_status("success")

    def get_by_target_id(
        self,
        collection_target_id: int,
    ) -> List[DebugWikipediaResults]:
        """Gets all debug results for a specific target"""

        query = """
            SELECT * FROM debug_wikipedia_results WHERE collection_target_id = %s ORDER BY created_at DESC
        """

        try:
            results = self.db.execute_select_query(query, (collection_target_id,))
            return [DebugWikipediaResults.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(
                f"Error getting debug results for target {collection_target_id}: {general_error}",
            )
            return []

    def get_debug_statistics(self) -> Dict[str, Any]:
        """Gets comprehensive debug testing statistics"""

        stats_query = """
            SELECT
                test_status,
                COUNT(*) as test_count,
                AVG(test_duration) as avg_duration,
                MIN(test_duration) as min_duration,
                MAX(test_duration) as max_duration
            FROM debug_wikipedia_results
            GROUP BY test_status
            ORDER BY test_count DESC
        """

        language_stats_query = """
            SELECT
                language_code,
                COUNT(*) as test_count,
                COUNT(CASE WHEN test_status = 'success' THEN 1 END) as success_count,
                COUNT(CASE WHEN test_status = 'failed' THEN 1 END) as failed_count
            FROM debug_wikipedia_results
            GROUP BY language_code
            ORDER BY test_count DESC
        """

        try:
            status_stats = self.db.execute_select_query(stats_query)
            language_stats = self.db.execute_select_query(language_stats_query)

            total_tests = sum(row["test_count"] for row in status_stats)

            stats = {
                "total_tests": total_tests,
                "by_status": status_stats,
                "by_language": language_stats,
                "summary": {},
            }

            # Calculate success rate
            for status_row in status_stats:
                status_name = status_row["test_status"]
                count = status_row["test_count"]
                percentage = round((count / total_tests * 100), 2) if total_tests > 0 else 0

                stats["summary"][status_name] = {
                    "count": count,
                    "percentage": percentage,
                    "avg_duration": float(status_row["avg_duration"]) if status_row["avg_duration"] else 0,
                }

            return stats

        except Exception as general_error:
            self.logger.error(f"Error getting debug statistics: {general_error}")
            return {"total_tests": 0, "by_status": [], "by_language": [], "summary": {}}

    def get_recent_tests(
        self,
        hours: int = 24,
    ) -> List[DebugWikipediaResults]:
        """Gets debug tests from the last X hours"""

        query = """
            SELECT * FROM debug_wikipedia_results
            WHERE created_at >= %s
            ORDER BY created_at DESC
        """

        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            results = self.db.execute_select_query(query, (cutoff_time,))
            return [DebugWikipediaResults.from_dict(row) for row in results]

        except Exception as general_error:
            self.logger.error(f"Error getting recent debug tests from last {hours} hours: {general_error}")
            return []
