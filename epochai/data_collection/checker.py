import time
from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.check_collection_targets_dao import CheckCollectionTargetsDAO
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger
from epochai.common.services.target_status_management_service import TargetStatusManagementService
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class Checker:
    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(
        self,
        target_config: Dict[str, Any],
        yaml_config: Dict[str, Any],
        utils_instance: Any,
        saver_instance: Any,
    ):
        self._logger = get_logger(__name__)
        self._targets = target_config

        self._utils = utils_instance
        self._saver = saver_instance
        self._dao = CheckCollectionTargetsDAO()

        self._target_status_service = TargetStatusManagementService()

        self._data_config = ConfigLoader.get_data_config()

        self._rate_limit_delay = yaml_config.get("api").get("rate_limit_delay")

        self._logger.debug(f"{__name__} initialized")

    @handle_generic_errors_gracefully("while checking targets", [])
    def check_targets(
        self,
        collector_name: str,
        collection_status: str,
        collection_types: Optional[List[str]] = None,
        target_ids: Optional[List[int]] = None,
        language_codes: Optional[List[str]] = None,
        recheck: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Checks collection targets to verify they can be successfully retrieved

        Returns:
            List of check results with success/failure information
        """

        self._logger.info(f"Starting target check for {collector_name} (recheck: {recheck})")

        if not self._targets:
            self._logger.warning("No target config provided for checking")
            return []

        check_results = []
        total_targets = 0
        successful_checks = 0
        failed_checks = 0

        for collection_type, language_data in self._targets.items():
            if collection_type == "_database_info":
                continue

            # Filter by collection types if specified
            if collection_types and collection_type not in collection_types:
                continue

            self._logger.info(f"Checking collection type: {collection_type}")

            for language_code, items_dict in language_data.items():
                # Filter by language codes if specified
                if language_codes and language_code not in language_codes:
                    continue

                self._logger.info(f"Checking language: {language_code} for type: {collection_type}")

                for collection_name, collection_target_id in items_dict.items():
                    # Filter by target IDs if specified
                    if target_ids and collection_target_id not in target_ids:
                        continue

                    total_targets += 1

                    if recheck is False and self._already_checked(collection_target_id):
                        self._logger.debug(f"Skipping already checked target: {collection_name}")
                        continue

                    check_result = self._check_single_target(
                        collection_name=collection_name,
                        language_code=language_code,
                        collection_target_id=collection_target_id,
                        collection_type=collection_type,
                    )

                    if check_result:
                        check_results.append(check_result)

                        if check_result.get("test_status") == "success":
                            successful_checks += 1
                        else:
                            failed_checks += 1
                            self._update_failed_target_status(collection_target_id, collection_name)

                    # Rate limiting
                    if self._rate_limit_delay > 0:
                        time.sleep(self._rate_limit_delay)

        # Log summary
        self._logger.info(f"Check completed: {total_targets} targets processed")
        self._logger.info(f"Results: {successful_checks} successful, {failed_checks} failed")

        return check_results

    @handle_generic_errors_gracefully("while checking if target has been checked recently", False)
    def _already_checked(self, collection_target_id: int) -> bool:
        """Check if target has already been checked recently"""
        existing_checks = self._dao.get_by_target_id(collection_target_id)
        return len(existing_checks) > 0

    @handle_generic_errors_gracefully("while checking single target", None)
    def _check_single_target(
        self,
        collection_name: str,
        language_code: str,
        collection_target_id: int,
        collection_type: str,
    ) -> Optional[Dict[str, Any]]:
        """Check a single collection target"""

        self._logger.info(f"Checking ({language_code}): '{collection_name}'")

        start_time = time.time()
        test_status = "failed"
        search_results_found = []
        error_message = ""

        try:
            if hasattr(self._utils, "get_page"):
                # Page-based check
                result = self._utils.get_page(collection_name, language_code)
            else:
                # Fallback - try to call a test method
                self._logger.warning(f"No known check method found for utils class {type(self._utils)}")
                result = None
                error_message = "No suitable check method found for collector type"

            if result:
                test_status = "success"
                # Extract title/name from result if available
                if isinstance(result, dict):
                    found_title = result.get("title", collection_name)
                    search_results_found = [found_title]
                elif hasattr(result, "title"):
                    search_results_found = [result.title]
                else:
                    search_results_found = [str(result)]

                self._logger.info(f"Successfully checked: '{collection_name}'")

            else:
                # Try search fallback if available
                if hasattr(self._utils, "search_using_config"):
                    search_results = self._utils.search_using_config(collection_name, language_code)
                    if search_results:
                        test_status = "failed_with_suggestions"
                        search_results_found = search_results[:5]
                        error_message = f"Target not found, but {len(search_results)} search suggestions available"
                        self._logger.warning(f"Target not found but search suggestions available: '{collection_name}'")
                    else:
                        test_status = "failed"
                        error_message = "Target not found and no search suggestions available"
                        self._logger.warning(f"Target not found: '{collection_name}'")
                else:
                    test_status = "failed"
                    error_message = "Target not accessible"
                    self._logger.warning(f"Target check failed: '{collection_name}'")

        except Exception as e:
            test_status = "failed"
            error_message = f"Error during check: {e!s}"
            self._logger.error(f"Error checking '{collection_name}': {e}")

        end_time = time.time()
        test_duration_ms = int((end_time - start_time) * 1000)

        # Save result to database
        try:
            result_id = self._dao.create_debug_result(
                collection_target_id=collection_target_id,
                search_term_used=collection_name,
                language_code=language_code,
                test_status=test_status,
                search_results_found=search_results_found,
                error_message=error_message,
                test_duration=test_duration_ms,
            )

            if result_id:
                self._logger.debug(f"Saved check result for '{collection_name}' to database")
            else:
                self._logger.warning(f"Failed to save check result for '{collection_name}' to database")

        except Exception as e:
            self._logger.error(f"Error saving check result for '{collection_name}': {e}")

        return {
            "collection_name": collection_name,
            "language_code": language_code,
            "collection_target_id": collection_target_id,
            "collection_type": collection_type,
            "test_status": test_status,
            "search_results_found": search_results_found,
            "error_message": error_message,
            "test_duration_ms": test_duration_ms,
        }

    @handle_generic_errors_gracefully("while updating failed target status", None)
    def _update_failed_target_status(self, collection_target_id: int, collection_name: str) -> None:
        """Update collection status for targets that fail validation"""
        success = self._target_status_service.update_target_collection_status(
            collection_target_id=collection_target_id,
            collection_status_name=CollectionStatusNames.CHECK_FAILED.value,
        )

        if success:
            self._logger.info(f"Updated status to for failed target: '{collection_name}' (ID: {collection_target_id})")
        else:
            self._logger.error(f"Failed to update status for target: '{collection_name}' (ID: {collection_target_id})")

    @handle_generic_errors_gracefully("while getting summary of check results", {})
    def get_check_summary(self) -> Dict[str, Any]:
        """Get summary of recent check results"""
        stats = self._dao.get_debug_statistics()
        return {
            "total_checks": stats.get("total_tests", 0),
            "status_breakdown": stats.get("summary", {}),
            "language_breakdown": stats.get("by_language", []),
            "recent_failures": [
                {
                    "collection_name": test.search_term_used,
                    "language_code": test.language_code,
                    "error_message": test.error_message,
                    "created_at": test.created_at,
                }
                for test in self._dao.get_failed_tests()[:10]
            ],
        }
