# ruff: noqa: RET505

from datetime import datetime
import os
import time
from typing import Any, Callable, Dict, List, Optional

import pandas as pd

from epochai.common.enums import CollectionTypeNames
from epochai.common.logging_config import get_logger
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class FiveThirtyEightUtils:
    """Utility class for FiveThirtyEight polling data operations"""

    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self, yaml_config: Dict[str, Any], collection_type: str):
        self._logger = get_logger(__name__)
        self._yaml_config = yaml_config
        self._collection_type = collection_type

        self._rate_limit_delay = self._yaml_config["api"]["rate_limit_delay"]

        self._csv_data: Optional[pd.DataFrame] = None
        self._csv_loaded = False

        self._logger.debug(f"{__name__} initialized with rate limit delay: {self._rate_limit_delay}s")

    @handle_generic_errors_gracefully("while converting numpty types", None)
    def _convert_numpy_types(self, obj: Any) -> Any:
        """Convert numpy types to JSON-serializable Python types"""
        if pd.isna(obj):
            return None
        elif hasattr(obj, "item"):  # numpty scalar types
            return obj.item()
        elif isinstance(obj, pd.Timestamp):
            return obj.isoformat() if obj is not None else None
        return obj

    @handle_generic_errors_gracefully("while getting project root", None)
    def _get_project_root(self) -> Optional[str]:
        """Gets the project root directory"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
        return project_root

    @handle_generic_errors_gracefully("while loading CSV data", False)
    def _load_csv_data(self) -> bool:
        """Loads and caches the FiveThirtyEight CSV data"""
        if self._csv_loaded:
            return True

        project_root = self._get_project_root()
        if not project_root:
            self._logger.error("Could not determine project root directory")
            return False

        if self._collection_type == CollectionTypeNames.POST_2016.value:
            csv_path = os.path.join(
                project_root,
                "data",
                "raw",
                "fivethirtyeight",
                "polls",
                "2024-averages",
                "presidential_general_averages_2024-09-12_uncorrected.csv",
            )
        elif self._collection_type == CollectionTypeNames.PRE_2016.value:
            csv_path = os.path.join(
                project_root,
                "data",
                "raw",
                "fivethirtyeight",
                "polls",
                "pres_pollaverages_1968-2016.csv",
            )

        if not os.path.exists(csv_path):
            self._logger.error(f"CSV file not found at: {csv_path}")
            return False

        try:
            self._logger.info(f"Loading CSV data from: {csv_path}")
            self._csv_data = pd.read_csv(csv_path)
            self._csv_loaded = True
            self._logger.info(f"Successfully loaded {len(self._csv_data)} rows of polling data")
            return True

        except Exception as e:
            self._logger.error(f"Error loading CSV data: {e}")
            return False

    @handle_generic_errors_gracefully("while getting polling record", {})
    def get_target(
        self,
        row_id: str,
    ) -> Dict[str, Any]:
        """
        Finds and retrieves a polling record from CSV data based on collection_name criteria

        This method searches the CSV data to find a record that matches the criteria
        extracted from the collection_name in collection_targets table.

        Args:
            cycle: Election cycle year from collection_name
            state: State name from collection_name
            candidate: Candidate name from collection_name
            row_id: Row index identifier from collection_name

        Returns:
            Dictionary containing the polling record metadata
        """
        if not self._load_csv_data():
            return {}

        if self._csv_data is None or self._csv_data.empty:
            self._logger.error("No CSV data available")
            return {}

        try:
            collection_row_id = int(row_id)
            pandas_index = collection_row_id - 2

            if pandas_index < 0 or pandas_index >= len(self._csv_data):
                self._logger.error(f"Row index {pandas_index} out of bounds (0-{len(self._csv_data)-1})")
                return {}

            row = self._csv_data.iloc[pandas_index]

            cycle = str(row.get("cycle", "Unknown"))
            state = str(row.get("state", "Unknown")).replace(" ", "_")
            candidate_raw = None
            for col_name in ["candidate_name", "candidate"]:
                if col_name in row.index and pd.notna(row.get(col_name)):
                    candidate_raw = row.get(col_name)
                    break
            candidate = str(candidate_raw or "Unknown").replace(" ", "_")

            metadata = {}
            for column in self._csv_data.columns:
                value = row.get(column)
                metadata[column] = self._convert_numpy_types(value)

            if "candidate" in metadata and "candidate_name" not in metadata:
                metadata["candidate_name"] = metadata["candidate"]

            if "date" in metadata:
                metadata["modeldate"] = metadata["date"]
                if not metadata.get("election_date") and metadata.get("cycle"):
                    cycle = metadata["cycle"]
                    if cycle == "2020":
                        metadata["election_date"] = "11/3/2020"
                    elif cycle == "2024":
                        metadata["election_date"] = "11/5/2024"

            metadata.update(
                {
                    "language": "en",
                    "collected_at": datetime.now().isoformat(),
                    "collection_source": "fivethirtyeight_csv",
                    "title": f"{candidate} - {state} - {cycle}",
                    "content": f"Polling data for {candidate} in {state} ({cycle}): {row.get('pct_estimate', 'N/A')}% estimate",
                    "original_row_index": pandas_index,
                },
            )

            self._logger.debug(
                f"Successfully retrieved polling record: {metadata.get('candidate_name')} "
                f"in {metadata.get('state')} ({metadata.get('cycle')})",
            )

            return metadata

        except (ValueError, IndexError, KeyError) as e:
            self._logger.error(f"Error retrieving polling record: {e}")
            return {}

    @handle_generic_errors_gracefully("while processing items by language", {})
    def process_items_by_language(
        self,
        items_by_language: Dict[str, Dict[str, int]],
        callback_function: Callable[[str, str, int], Optional[Dict[str, Any]]],
    ) -> Dict[str, List[Optional[Dict[str, Any]]]]:
        """
        Processes collection items by language using the provided callback function

        Args:
            items_by_language: Dictionary mapping language codes to collection items
            callback_function: Function to call for each item (collection_name, language_code, target_id)

        Returns:
            Dictionary mapping language codes to lists of results
        """
        results_by_language = {}

        for language_code, items_dict in items_by_language.items():
            self._logger.info(f"Processing {len(items_dict)} items for language '{language_code}'")
            results = []

            for collection_name, collection_target_id in items_dict.items():
                try:
                    if self._rate_limit_delay > 0:
                        time.sleep(self._rate_limit_delay)

                    result = callback_function(collection_name, language_code, collection_target_id)
                    results.append(result)

                except Exception as e:
                    self._logger.error(f"Error processing {collection_name}: {e}")
                    results.append(None)

            results_by_language[language_code] = results
            self._logger.info(
                f"Completed processing for language '{language_code}': "
                f"{sum(1 for r in results if r is not None)}/{len(results)} successful",
            )

        return results_by_language
