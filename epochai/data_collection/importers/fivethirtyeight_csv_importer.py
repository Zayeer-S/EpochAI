import os
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.collection_targets_dao import CollectionTargetsDAO
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger
from epochai.common.utils.database_utils import DatabaseUtils
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class FiveThirtyEightCSVImporter:
    """Imports FiveThirtyEight CSV data into collection_targets table"""

    collection_target_type = List[Tuple[int, int, str, str, int]]

    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self):
        self._logger = get_logger(__name__)
        self._yaml_config = ConfigLoader.get_collector_yaml_config("fivethirtyeight")

        # DAOs
        self.collection_targets_dao = CollectionTargetsDAO()

        # UTILs
        self._database_utils = DatabaseUtils()

        # CONSTANTS
        self.COLLECTOR_NAME = self._yaml_config.get("collector_name")
        self.COLLECTION_TYPE = "presidential_polling_averages"
        self.LANGUAGE_CODE = self._yaml_config.get("api").get("language")
        self.BATCH_SIZE = self._yaml_config.get("importer").get("batch_size")

        self._logger.info(f"{__name__} initialized")

    @handle_generic_errors_gracefully("while getting project root path", None)
    def _get_project_root(self) -> Optional[str]:
        """Gets project root directory"""
        current_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(current_dir, "..", "..", ".."))
        return project_root

    @handle_generic_errors_gracefully("while reading CSV file", None)
    def _read_csv_file(self, filepath: str) -> Optional[pd.DataFrame]:
        """Reads the CSV file"""
        if not os.path.exists(filepath):
            self._logger.error(f"CSV file not found at: {filepath}")
            return None

        df = pd.read_csv(filepath)

        self._logger.info(f"Successfully read CSV with {len(df)} rows")
        return df

    @handle_generic_errors_gracefully("while creating collection name", "unkown_target_error")
    def _create_collection_name(
        self,
        row: pd.Series,
        row_index: int,
    ) -> str:
        """Dynamically creates unique collection name for each CSV row"""
        cycle = row.get("cycle", "unknown")
        state = row.get("state", "unknown").replace(" ", "_")
        candidate_name = row.get("candidate_name", "unknown").replace(" ", "_")
        modeldate = row.get("modeldate", "unknown")

        if isinstance(modeldate, str) and "T" in modeldate:
            modeldate = modeldate.split("T")[0]
        elif pd.isna(modeldate):
            modeldate = "unknown"

        return f"{cycle}_{state}_{candidate_name}_{modeldate}_{row_index}"

    @handle_generic_errors_gracefully("while inserting batch", 0)
    def _insert_batch(
        self,
        batch_data: collection_target_type,
        batch_num: int,
    ) -> int:
        """
        Inserts a batch of data into the database

        Args:
            batch_data: List of tuples containing the data to insert
            batch_num: Batch number for logging purposes

        Returns:
            Number of records successfully inserted
        """
        if not batch_data:
            return 0

        self._logger.info(f"Inserting batch {batch_num} with {len(batch_data)} records")
        created_count = self.collection_targets_dao.bulk_create_collection_targets(batch_data)

        if created_count > 0:
            self._logger.info(f"Successfully inserted batch {batch_num}: {created_count} records")
        else:
            self._logger.error(f"Failed to insert batch {batch_num}")

        return created_count

    @handle_generic_errors_gracefully("while preparing bulk data", [])
    def _process_data_in_batches(
        self,
        df: pd.DataFrame,
        collector_name_id: int,
        collection_type_id: int,
        collection_status_id: int,
        dry_run: bool,
    ) -> int:
        """
        Processes DataFrame in batches and inserts into database

        Args:
            df: DataFrame containing the CSV data
            collector_name_id: ID of the collector name
            collection_type_id: ID of the collection type
            collection_status_id: ID of the collection status
            dry_run: If True, only logs what would be inserted without actual insertion

        Returns:
            Total number of records successfully inserted
        """

        current_batch = []
        total_inserted = 0
        batch_num = 1

        for index, row in df.iterrows():
            collection_name = self._create_collection_name(row, index)

            current_batch.append(
                (
                    collector_name_id,
                    collection_type_id,
                    self.LANGUAGE_CODE,
                    collection_name,
                    collection_status_id,
                ),
            )

            if len(current_batch) >= self.BATCH_SIZE:
                if dry_run:
                    self._logger.info(f"DRY RUN - Would insert batch {batch_num} with {len(current_batch)} records")
                    total_inserted += len(current_batch)
                else:
                    inserted_count = self._insert_batch(current_batch, batch_num)
                    total_inserted += inserted_count

                # Reset and increment
                current_batch = []
                batch_num += 1

            if (index + 1) % 1000 == 0:
                self._logger.info(f"Processed {index + 1} records")

        if current_batch:
            if dry_run:
                self._logger.info(f"DRY RUN - Would insert final batch {batch_num} with {len(current_batch)} records")
                total_inserted += len(current_batch)
            else:
                inserted_count = self._insert_batch(current_batch, batch_num)
                total_inserted += inserted_count

        return total_inserted

    @handle_generic_errors_gracefully("while importing CSV data", False)
    def import_csv_to_targets(
        self,
        csv_filepath: Optional[str] = None,
        dry_run: bool = False,
    ) -> bool:
        """Main method to import FiveThirtyEight CSV data into collection_targets

        Args:
            csv_filepath: Optional filepath to CSV file
            dry_run: Validates without insertion when True

        Returns:
            True if successful and vice versa
        """

        if not csv_filepath:
            project_root = self._get_project_root()
            if not project_root:
                self._logger.error(f"Couldn't determine project root directory: {project_root}")
                return False

            csv_filepath = os.path.join(
                project_root,
                "data",
                "raw",
                "fivethirtyeight_raw",
                "polls",
                "pres_pollaverages_1968-2016.csv",
            )

        self._logger.info(f"Starting CSV import from {csv_filepath}")

        df = self._read_csv_file(csv_filepath)
        if df is None or df.empty:
            return False

        collector_name_id, collection_type_id, collection_status_id = self._database_utils.get_name_type_status_ids(
            collector_name=self.COLLECTOR_NAME,
            collection_type=self.COLLECTION_TYPE,
            collection_status_name=CollectionStatusNames.NOT_COLLECTED.value,
        )

        self._logger.info(f"Processing {len(df)} records in batches of {self.BATCH_SIZE}")

        if dry_run:
            self._logger.info("DRY RUN - Would insert the following:")
            self._logger.info(f"\tCollector: {self.COLLECTOR_NAME} (ID: {collector_name_id})")
            self._logger.info(f"\tCollection Type: {self.COLLECTION_TYPE} (ID: {collection_type_id})")
            self._logger.info(f"\tLanguage: {self.LANGUAGE_CODE}")
            self._logger.info(f"\tTotal records: {len(df)}")
            self._logger.info(f"\tBatch size: {self.BATCH_SIZE}")

        total_inserted = self._process_data_in_batches(
            df=df,
            collector_name_id=collector_name_id,
            collection_type_id=collection_type_id,
            collection_status_id=collection_status_id,
            dry_run=dry_run,
        )

        if dry_run:
            self._logger.info(f"DRY RUN completed - Would have inserted {total_inserted} records")
            return True

        if total_inserted > 0:
            self._logger.info(f"Successfully imported {total_inserted} collection targets in batches")
            return True
        self._logger.error("Failed to import collection targets")
        return False

    @handle_generic_errors_gracefully("while getting import statistics", {})
    def get_import_statistics(self) -> Dict[str, Any]:
        """Gets statistics about imported FiveThirtyEight data"""
        collector_name_id, collection_type_id, _ = self._database_utils.get_name_type_status_ids(
            collector_name=self.COLLECTOR_NAME,
            collection_type=self.COLLECTION_TYPE,
        )

        if not collector_name_id or not collection_type_id:
            self._logger.warning("Collector name ID or collection type ID not found for statistics")
            return {}

        targets = self.collection_targets_dao.get_by_collector_and_type_ids(
            collector_name_id,
            collection_type_id,
        )

        status_counts: Dict[int, Any] = {}
        for target in targets:
            status_id = target.collection_status_id
            status_counts[status_id] = status_counts.get(status_id, 0) + 1

        return {
            "total_targets": len(targets),
            "by_status_id": status_counts,
            "collector_name": self.COLLECTOR_NAME,
            "collection_type": self.COLLECTION_TYPE,
        }
