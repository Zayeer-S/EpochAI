import argparse
import importlib
import inspect
import json
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.validation_statuses_dao import ValidationStatusesDAO
from epochai.common.logging_config import get_logger, setup_logging


class CleanerNotFoundError(Exception):
    pass


class Cleaner:
    """Cleaning orchestrator and CLI"""

    def __init__(self):
        log_config = ConfigLoader.get_logging_config()
        setup_logging(
            log_level=log_config["level"],
            log_to_file=log_config["log_to_file"],
            log_dir=log_config["log_directory"],
        )

        self.logger = get_logger(__name__)

        self.config = ConfigLoader.get_data_config()
        self.available_cleaners = self._get_available_cleaners(self.config)

        self.validation_statuses_dao = ValidationStatusesDAO()
        self.validation_status_names = self._get_all_validation_statuses()

        # CONVENIENCE VAR FOR IDE SUPPORT - DO NOT USE IN PROD
        # force_log = self.logger.info("Using convenience var for IDE support")
        # if force_log:
        #    from epochai.data_processing.cleaning.wikipedia_cleaner import WikipediaCleaner
        #    self.convenience_available_cleaners= {
        #        "wikipedia": WikipediaCleaner,
        #    }

        self.logger.info("Cleaning Orchestrator and CLI Initialized")

    def _get_all_validation_statuses(self) -> List[str]:
        """Convenience method to get all validation statuses directly from DAO"""
        validation_status_names = self.validation_statuses_dao.get_all()

        if validation_status_names:
            return [status.validation_status_name for status in validation_status_names]
        self.logger.error("Error getting validation status names")
        return []

    def _get_available_cleaners(self, config) -> Dict[str, Any]:
        """Gets cleaner names without suffix mapped to Class"""
        available_cleaners = {}

        current_dir = Path(__file__).parent
        cleaners_dir = current_dir / "cleaning"

        if not cleaners_dir.exists():
            self.logger.warning(f"Cleaners directory not found: {cleaners_dir}")
            return {}

        for file_path in cleaners_dir.glob("*_cleaner.py"):
            module_name = file_path.stem

            try:
                full_module_name = f"epochai.data_processing.cleaning.{module_name}"
                module = importlib.import_module(full_module_name)

                for class_name, class_obj in inspect.getmembers(module, inspect.isclass):
                    if (
                        class_name.endswith("Cleaner")
                        and class_obj.__module__ == full_module_name
                        and class_name != "BaseCleaner"
                    ):
                        suffixless_name = module_name.replace("_cleaner", "")
                        available_cleaners[suffixless_name] = class_obj

                        self.logger.debug(f"Discovered cleaner: '{suffixless_name}': {class_name}")
                        break

            except Exception as general_error:
                self.logger.warning(f"Error loading cleaner from {module_name}: {general_error}")

        return available_cleaners

    def _get_id_range(
        self,
        raw_data_id_range: str,
    ) -> List[int]:
        """
        Gets inputed id ranges

        Example:
            User inputs 10-30, this returns 10, 11, 12... 29, 30
        """
        ranges = [r.strip() for r in raw_data_id_range.split(",")]

        id_list: List[int] = []

        for range_str in ranges:
            parts = range_str.split("-")

            try:
                lower_bound = int(parts[0])
                upper_bound = int(parts[1])

                if upper_bound >= lower_bound:
                    id_list.extend(range(lower_bound, upper_bound + 1))
                else:
                    self.logger.error(
                        f"Second number in range ({lower_bound}-{upper_bound}) is greater than first. Needs to be other way around.",
                    )
            except ValueError as value_error:
                self.logger.error(f"Invalid numbers in range: '{range_str}' - {value_error}")
                return []
            except Exception as general_error:
                self.logger.error(
                    f"Unknown error occurred. range_str: {range_str}, lower_bound: {lower_bound}, upper_bound: {upper_bound} - {general_error}",
                )

        return id_list

    def _get_cleaner(
        self,
        cleaner_type: str,
    ):
        """Gets a cleaner instance by its name"""
        if cleaner_type not in self.available_cleaners:
            raise CleanerNotFoundError(
                f"Unkown cleaner type: {cleaner_type}"
                f"Available cleaners: {', '.join(self.available_cleaners.keys())}",
            )
        try:
            return self.available_cleaners[cleaner_type]()
        except Exception as e:
            self.logger.error(f"Failed to initialize {cleaner_type} cleaner: {e}")
            return None

    def clean_single(
        self,
        cleaner_type: str,
        raw_data_id: int,
    ) -> bool:
        """Cleans a single raw data record"""
        self.logger.info(f"Cleaning single record {raw_data_id} with {cleaner_type} cleaner")

        try:
            cleaner = self._get_cleaner(cleaner_type)
        except CleanerNotFoundError as bruh:
            self.logger.error(bruh)
            return False

        try:
            cleaned_data_id = cleaner.clean_single_record(raw_data_id)
            if cleaned_data_id:
                self.logger.info(
                    f"Successfully cleaned record raw: {raw_data_id} -> cleaned: {cleaned_data_id}",
                )
                return True
            self.logger.error(f"Failed to clean record: {raw_data_id}")
            return False
        except Exception as general_error:
            self.logger.error(f"Error cleaning record {raw_data_id}: {general_error}")
            return False

    def clean_multiple(
        self,
        cleaner_type: str,
        raw_data_id_range: str,
    ) -> Dict[str, Any]:
        """Cleans multiple raw data records"""

        id_list = self._get_id_range(raw_data_id_range)
        if not id_list:
            return {"success": False, "error": "No valid ids found"}

        self.logger.info(f"Cleaning {len(id_list)} records with {cleaner_type} cleaner")

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            results: Dict[str, Any] = cleaner.clean_multiple_records(id_list)
            self.logger.info(
                f"Batch cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
            )
            return results
        except Exception as general_error:
            self.logger.error(f"Error in batch cleaning: {general_error}")
            return {"success": False, "error": f"{general_error!s}"}

    def clean_by_status(
        self,
        cleaner_type: str,
        validation_status: str,
    ) -> Dict[str, Any]:
        """Cleans all records with a specific validation status"""

        self.logger.info(
            f"Cleaning records with validation status '{validation_status}' using {cleaner_type} cleaner",
        )

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            results: Dict[str, Any] = cleaner.clean_by_validation_status(validation_status)
            self.logger.info(
                f"Status based cleaning completed: {results['success_count']} successful, {results['error_count']}",
            )
            return results
        except Exception as general_error:
            self.logger.error(f"Error in status-based cleaning: {general_error}")
            return {"success": False, "error": str(general_error)}

    def clean_recent(
        self,
        cleaner_type: str,
        hours: int,
    ) -> Dict[str, Any]:
        """Cleans records in the last X hours"""

        self.logger.info(f"Cleaning records from the last {hours} hours using {cleaner_type} cleaner")

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            results: Dict[str, Any] = cleaner.clean_recent_data(hours)
            self.logger.info(
                f"Recent data cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
            )
            return results
        except Exception as general_error:
            self.logger.error(f"Error cleaning recent data: {general_error}")
            return {"success": False, "error": str(general_error)}

    def clean_wikipedia_batch(
        self,
        limit: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Cleans wikipedia data with 'valid' status with optional limit"""
        self.logger.info(f"Running wikipedia batch cleaning (limited to {limit})")

        cleaner_type = next(iter(self.available_cleaners.keys()))
        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            results: Dict[str, Any] = cleaner.clean_wikipedia_batch(limit)
            self.logger.info(
                f"Wikipedia batch cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
            )
            return results
        except Exception as general_error:
            self.logger.error(f"Error cleaning recent data: {general_error}")
            return {"success": False, "error": str(general_error)}

    def get_statistics(
        self,
        cleaner_type: str,
    ) -> Dict[str, Any]:
        """Gets cleaning stats for a cleaner"""
        self.logger.info(f"Getting statistics for {cleaner_type} cleaner")

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            stats = cleaner.get_cleaning_statistics()
            return {"success": True, "statistics": stats}
        except Exception as general_error:
            self.logger.error(f"Error getting statistics: {general_error}")
            return {"success": False, "error": str(general_error)}

    def get_schema_info(
        self,
        cleaner_type: str,
    ) -> Dict[str, Any]:
        """Gets schema info for a cleaner"""

        self.logger.info(f"Getting schema info for {cleaner_type} cleaner")

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return {"success": False, "error": "Failed to initialize cleaner"}

        try:
            schema_info = cleaner.get_schema_info()
            return {"success": True, "schema_info": schema_info}
        except Exception as general_error:
            self.logger.error(f"Error getting schema info: {general_error}")
            return {"success": False, "error": str(general_error)}

    def reload_schema(
        self,
        cleaner_type: str,
    ) -> bool:
        """Reloads schema from database for a cleaner"""

        self.logger.info(f"Reloading schema for {cleaner_type} cleaner")

        cleaner = self._get_cleaner(cleaner_type)
        if not cleaner:
            return False

        try:
            success: bool = cleaner.reload_schema_from_database()
            if success:
                self.logger.info("Schema reloaded successfully")
            else:
                self.logger.warning("Schema reload completed but no changes detected")
            return success
        except Exception as general_error:
            self.logger.error(f"Error reloading schema: {general_error}")
            return False

    def list_all_cleaners(self) -> None:
        """Lists all available cleaners"""
        print("Available cleaners:")
        for cleaner_name in self.available_cleaners:
            print(f"'{cleaner_name}'")


def setup_args(
    available_cleaners_keys: List[str],
    validation_status_names: List[str],
) -> argparse.ArgumentParser:
    """Sets up CLI args"""
    parser = argparse.ArgumentParser(
        description="EpochAI Data Cleaning CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        FYI - wikipedia used as example below, replace wikipedia with relevant cleaner name
        # Clean single
        python cleaner.py clean-single wikipedia 123

        # Clean multiple
        python cleaner.py clean-multiple wikipedia 1-10, 15-20

        # Clean all valid records
        python cleaner.py clean-by-status wikipedia valid

        # Clean recent data (last X hours)
        python cleaner.py clean-recent wikipedia 24

        # Wikipedia batch clean
        python cleaner.py wikipedia-batch --limit 100

        # Get cleaning stats
        python cleaner.py stats wikipedia

        # Get schema information
        python cleaner.py schema-info wikipedia

        # Reload schema from database
        python cleaner.py reload-schema wikipedia

        # List all available cleaners
        python cleaner.py list-cleaners
        """,
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        default="INFO",
        help="Set logging level (default: INFO)",
    )

    parser.add_argument(
        "--no-log-file",
        action="store_true",
        help="Disable logging to file",
    )

    parser.add_argument(
        "--json-output",
        action="store_true",
        help="Output results in JSON format",
    )

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    clean_single_parser = subparsers.add_parser('clean-single', help="Cleans a single specified ID")  # fmt: skip
    clean_single_parser.add_argument('cleaner_type', choices=available_cleaners_keys, help="Type of cleaner to use")  # fmt: skip
    clean_single_parser.add_argument('raw_data_id', type=int, help="ID of raw data record to clean")  # fmt: skip

    clean_multiple_parser = subparsers.add_parser("clean-multiple", help="Cleans multiple IDs")  # fmt: skip
    clean_multiple_parser.add_argument('cleaner_type', choices=available_cleaners_keys, help="Type of cleaner to use")  # fmt: skip
    clean_multiple_parser.add_argument('raw_data_id', type=str, help="ID range of raw data records to clean (e.g. insert '1-3, 5-6')")  # fmt: skip

    clean_by_status_parser = subparsers.add_parser('clean-by-status', help="Clean all IDs with specific validation status")  # fmt: skip
    clean_by_status_parser.add_argument("cleaner_type", choices=available_cleaners_keys, help="Type of cleaner to use")  # fmt: skip
    clean_by_status_parser.add_argument("validation_status", choices=validation_status_names, help="Validation status to filter by")  # fmt: skip

    clean_by_hours_parser = subparsers.add_parser("clean-by-hours", help="Cleans all ID's collected in past X hours")  # fmt: skip
    clean_by_hours_parser.add_argument("cleaner_type", choices=available_cleaners_keys, help="Type of cleaner to use")  # fmt: skip
    clean_by_hours_parser.add_argument("hours", type=int, help="Number of hours to look back")

    wiki_batch_parser = subparsers.add_parser("wikipedia-batch", help="Runs Wikipedia batch cleaning")  # fmt: skip
    wiki_batch_parser.add_argument('--limit', type=int, help='Limit number of records to process')  # fmt: skip

    stats_parser = subparsers.add_parser('stats', help='Gets cleaning statistics for a cleaner')  # fmt: skip
    stats_parser.add_argument('cleaner_type', choices=available_cleaners_keys, help='Type of cleaner to get stats for')  # fmt: skip

    schema_info_parser = subparsers.add_parser('schema-info', help='Gets schema information for a cleaner')  # fmt: skip
    schema_info_parser.add_argument('cleaner_type', choices=available_cleaners_keys, help='Type of cleaner to get schema info for')  # fmt: skip

    reload_schema_parser = subparsers.add_parser('reload-schema', help='Reloads schema from database')  # fmt: skip
    reload_schema_parser.add_argument('cleaner_type', choices=available_cleaners_keys, help='Type of cleaner to reload schema for')  # fmt: skip

    subparsers.add_parser("list-cleaners", help="List all available cleaners")

    return parser


def main():
    cli = Cleaner()

    available_cleaner_keys = list(cli.available_cleaners.keys())

    parser = setup_args(available_cleaner_keys, cli.validation_status_names)

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    log_config = ConfigLoader.get_logging_config()
    setup_logging(
        log_level=args.log_level,
        log_to_file=not args.no_log_file,
        log_dir=log_config.get("log_directory", "logs"),
    )

    result = None
    success = True

    try:
        if args.command == "clean-single":
            success = cli.clean_single(args.cleaner_type, args.raw_data_id)
            result = {"success": success, "cleaned_id": args.raw_data_id if success else None}

        elif args.command == "clean-multiple":
            result = cli.clean_multiple(args.cleaner_type, args.raw_data_id)
            success = result.get("success_count", 0) > 0

        elif args.command == "clean-by-status":
            result = cli.clean_by_status(args.cleaner_type, args.validation_status)
            success = result.get("success_count", 0) > 0

        elif args.command == "clean-by-hours":
            result = cli.clean_recent(args.cleaner_type, args.hours)
            success = result.get("success_count", 0) > 0

        elif args.command == "wikipedia-batch":
            result = cli.clean_wikipedia_batch(args.limit)
            success = result.get("success_count", 0) > 0

        elif args.command == "stats":
            result = cli.get_statistics(args.cleaner_type)
            success = result.get("success", False)

        elif args.command == "schema-info":
            result = cli.get_schema_info(args.cleaner_type)
            success = result.get("success", False)

        elif args.command == "reload-schema":
            success = cli.reload_schema(args.cleaner_type)
            result = {
                "success": success,
                "message": "Schema reloaded" if success else "No schema changes detected",
            }

        elif args.command == "list-cleaners":
            cli.list_all_cleaners()
            success = True

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)

    except Exception as general_error:
        logger = get_logger(__name__)
        logger.error(f"Unexpected error: {general_error}")
        result = {"success": False, "error": str(general_error)}
        success = False

    if result and args.json_output:
        print(json.dumps(result, indent=2, default=str))
    elif result and args.command != "list-cleaners":
        if success:
            print("\tOperation completed successfully")
            if "statistics" in result:
                stats = result["statistics"]
                print(f"\t\tCleaner: {stats.get('cleaner_name')} v{stats.get('cleaner_version')}")
                print(f"\t\tTotal cleaned: {stats.get('total_cleaned', 0)}")
                print(f"\t\tValid: {stats.get('valid_count', 0)}")
                print(f"\t\tInvalid: {stats.get('invalid_count', 0)}")
                print(f"\t\tSuccess rate: {stats.get('success_rate', 0):.1f}%")
            elif "schema_info" in result:
                schema = result["schema_info"]
                print(f"\t\tSchema cached: {schema.get('schema_cached')}")
                print(f"\t\tSchema ID: {schema.get('schema_id')}")
                print(f"\t\tValidator available: {schema.get('validator_available')}")
                print(f"\t\tRecords processed: {schema.get('records_processed')}")
            elif "success_count" in result:
                print(f"\t\tSuccessful: {result.get('success_count', 0)}")
                print(f"\t\tFailed: {result.get('error_count', 0)}")
                if "total_time_seconds" in result:
                    print(f"\t\tTotal time: {result.get('total_time_seconds', 0):.2f}s")
                    print(f"\t\tAverage time per record: {result.get('average_time_per_record', 0):.2f}s")
        else:
            print("\tOperation failed")
            if "error" in result:
                print(f"Error: {result['error']}")

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
