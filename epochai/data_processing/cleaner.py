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
from epochai.common.utils.decorators import handle_generic_errors_gracefully, handle_initialization_errors


class CleanerNotFoundError(Exception):
    pass


class CleanerCLI:
    """Cleaning orchestrator and CLI"""

    @handle_initialization_errors(f"{__name__} Initialization")
    def __init__(self):
        self.logger = get_logger(__name__)

        self.available_cleaners = self._get_available_cleaners()
        self.validation_statuses_dao = ValidationStatusesDAO()
        self.validation_status_names = self._get_all_validation_statuses()

        self.cleaning_actions_list = {
            "clean": self.clean,
            "clean-by-status": self.clean_by_status,
            "clean-recent": self.clean_recent,
            "stats": self.get_statistics,
            "schema-info": self.get_schema_info,
            "reload-schema": self.reload_schema,
        }

        self.logger.info("Cleaning Orchestrator and CLI Initialized")

    @handle_generic_errors_gracefully("while getting validation status names", [])
    def _get_all_validation_statuses(self) -> List[str]:
        """Convenience method to get all validation statuses directly from DAO"""
        validation_status_names = self.validation_statuses_dao.get_all()

        if validation_status_names:
            return [status.validation_status_name for status in validation_status_names]
        self.logger.error("Error getting validation status names")
        return []

    @handle_generic_errors_gracefully("while getting available cleaners", {})
    def _get_available_cleaners(self) -> Dict[str, Any]:
        """Gets cleaner names without suffix mapped to Class"""
        available_cleaners = {}

        current_dir = Path(__file__).parent
        cleaners_dir = current_dir / "cleaners"

        if not cleaners_dir.exists():
            self.logger.warning(f"Cleaners directory not found: {cleaners_dir}")
            return {}

        for file_path in cleaners_dir.glob("*_cleaner.py"):
            module_name = file_path.stem

            try:
                full_module_name = f"epochai.data_processing.cleaners.{module_name}"
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

    @handle_generic_errors_gracefully("while getting ID range", [])
    def get_id_range(self, raw_data_id_input: str) -> List[int]:
        """
        Gets inputed id ranges

        Example:
            User inputs "10-30", this returns "10, 11, 12... 29, 30"
            User input "11", this returns "11"
        """
        raw_data_id_input = raw_data_id_input.strip()

        if "," not in raw_data_id_input and "-" not in raw_data_id_input:
            try:
                return [int(raw_data_id_input)]
            except ValueError:
                self.logger.error(f"Invalid single ID: {raw_data_id_input}")
                return []

        id_list: List[int] = []
        ranges = [r.strip() for r in raw_data_id_input.split(",")]

        for range_str in ranges:
            if "-" in range_str:  # it must be a range
                parts = range_str.split("-")
                if len(parts) != 2:
                    self.logger.error(f"Invalid range format: '{range_str}'")
                    continue

                try:
                    lower_bound = int(parts[0])
                    upper_bound = int(parts[1])

                    if upper_bound >= lower_bound:
                        id_list.extend(range(lower_bound, upper_bound + 1))
                    else:
                        self.logger.error(
                            f"Second number in range ({lower_bound}-{upper_bound}) must be >= first number",
                        )
                except ValueError as value_error:
                    self.logger.error(f"Invalid numbers in range: '{range_str}' - {value_error}")
                    return []
            else:  # its a single number after a range i.e. 5 in input: "1-3, 5"
                try:
                    id_list.append(int(range_str))
                except ValueError:
                    self.logger.error(f"Invalid ID: '{range_str}'")
                    return []

        return id_list

    @handle_generic_errors_gracefully("while getting cleaner instance", None)
    def _get_cleaner_instance(self, cleaner_name: str):
        """Gets a cleaner instance by its name"""
        if cleaner_name not in self.available_cleaners:
            raise CleanerNotFoundError(
                f"Unknown cleaner type: {cleaner_name}. " f"Available cleaners: {', '.join(self.available_cleaners.keys())}",
            )
        return self.available_cleaners[cleaner_name]()

    @handle_generic_errors_gracefully("while cleaning data", {"success": False, "error": "Cleaning failed"})
    def clean(
        self,
        cleaner: Any,
        cleaner_name: str,
        raw_data_ids: Optional[List[int]] = None,
    ) -> Dict[str, Any]:
        """Clean specific raw data IDs"""
        if not raw_data_ids:
            return {"success": False, "error": "No raw data IDs provided"}

        self.logger.info(f"Cleaning {len(raw_data_ids)} records with {cleaner_name} cleaner")

        results = cleaner.clean_multiple_records(raw_data_ids)
        self.logger.info(
            f"Batch cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
        )
        return results

    @handle_generic_errors_gracefully("while cleaning by status", {"success": False, "error": "Status cleaning failed"})
    def clean_by_status(
        self,
        cleaner: Any,
        cleaner_name: str,
        validation_status: str,
    ) -> Dict[str, Any]:
        """Clean all records with a specific validation status"""
        self.logger.info(
            f"Cleaning records with validation status '{validation_status}' using {cleaner_name} cleaner",
        )

        results = cleaner.clean_by_validation_status(validation_status)
        self.logger.info(
            f"Status based cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
        )
        return results

    @handle_generic_errors_gracefully("while cleaning recent data", {"success": False, "error": "Recent cleaning failed"})
    def clean_recent(
        self,
        cleaner: Any,
        cleaner_name: str,
        hours: int,
    ) -> Dict[str, Any]:
        """Clean records in the last X hours"""
        self.logger.info(f"Cleaning records from the last {hours} hours using {cleaner_name} cleaner")

        results = cleaner.clean_recent_data(hours)
        self.logger.info(
            f"Recent data cleaning completed: {results['success_count']} successful, {results['error_count']} failed",
        )
        return results

    @handle_generic_errors_gracefully("while getting statistics", {"success": False, "error": "Stats failed"})
    def get_statistics(
        self,
        cleaner: Any,
        cleaner_name: str,
    ) -> Dict[str, Any]:
        """Get cleaning stats for a cleaner"""
        self.logger.info(f"Getting statistics for {cleaner_name} cleaner")

        try:
            stats = cleaner.get_cleaning_statistics()
            return {"success": True, "statistics": stats}
        except Exception as general_error:
            self.logger.error(f"Error getting statistics: {general_error}")
            return {"success": False, "error": str(general_error)}

    @handle_generic_errors_gracefully("while getting schema info", {"success": False, "error": "Schema info failed"})
    def get_schema_info(
        self,
        cleaner: Any,
        cleaner_name: str,
    ) -> Dict[str, Any]:
        """Get schema info for a cleaner"""
        self.logger.info(f"Getting schema info for {cleaner_name} cleaner")

        try:
            schema_info = cleaner.get_schema_info()
            return {"success": True, "schema_info": schema_info}
        except Exception as general_error:
            self.logger.error(f"Error getting schema info: {general_error}")
            return {"success": False, "error": str(general_error)}

    @handle_generic_errors_gracefully("while reloading schema", {"success": False, "message": "Schema reload failed"})
    def reload_schema(
        self,
        cleaner: Any,
        cleaner_name: str,
    ) -> Dict[str, Any]:
        """Reload schema from database for a cleaner"""
        self.logger.info(f"Reloading schema for {cleaner_name} cleaner")

        try:
            success = cleaner.reload_schema_from_database()
            message = "Schema reloaded successfully" if success else "No schema changes detected"
            self.logger.info(message)
            return {"success": success, "message": message}
        except Exception as general_error:
            self.logger.error(f"Error reloading schema: {general_error}")
            return {"success": False, "message": str(general_error)}

    @handle_generic_errors_gracefully("while executing cleaning operation", {"success": False, "error": "Execution failed"})
    def execute_cleaning(
        self,
        action: str,
        cleaner_name: str,
        raw_data_ids: Optional[List[int]] = None,
        validation_status: Optional[str] = None,
        hours: Optional[int] = None,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        """Execute a cleaning operation"""
        if action not in self.cleaning_actions_list:
            raise ValueError(f"Unknown action: {action}. Available actions: {', '.join(self.cleaning_actions_list.keys())}")

        if dry_run:
            return {
                "success": True,
                "message": f"DRY RUN: Would execute {action} with {cleaner_name} cleaner",
                "available_cleaners": list(self.available_cleaners.keys()),
                "available_actions": list(self.cleaning_actions_list.keys()),
            }

        cleaner = self._get_cleaner_instance(cleaner_name)
        if not cleaner:
            return {"success": False, "error": f"Failed to initialize {cleaner_name} cleaner"}

        # Prepare arguments based on action type
        action_kwargs = {
            "cleaner": cleaner,
            "cleaner_name": cleaner_name,
        }

        if action == "clean" and raw_data_ids:
            action_kwargs["raw_data_ids"] = raw_data_ids
        elif action == "clean-by-status" and validation_status:
            action_kwargs["validation_status"] = validation_status
        elif action == "clean-recent" and hours:
            action_kwargs["hours"] = hours

        return self.cleaning_actions_list[action](**action_kwargs)

    def list_all_cleaners(self) -> None:
        """List all available cleaners"""
        print("Available cleaners:")
        for cleaner_name in self.available_cleaners:
            print(f"  {cleaner_name}")


def setup_args(
    available_cleaners_keys: List[str],
    validation_status_names: List[str],
    cleaning_actions_list: List[str],
) -> argparse.ArgumentParser:
    """Sets up CLI args"""
    parser = argparse.ArgumentParser(
        description="EpochAI Data Cleaning CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=f"""
        Examples:
            cleaning_actions: {', '.join(cleaning_actions_list)}
            available_cleaners: {', '.join(available_cleaners_keys)}

            # Clean specific IDs
            python cleaner.py clean wikipedia --ids "1,2,3"

            # Clean by validation status
            python cleaner.py clean-by-status wikipedia --status valid

            # Clean recent data
            python cleaner.py clean-recent wikipedia --hours 24

            # Get statistics
            python cleaner.py stats wikipedia

            # Get schema info
            python cleaner.py schema-info wikipedia

            # Dry run
            python cleaner.py clean wikipedia --dry-run

            # List available cleaners
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

    for action in cleaning_actions_list:
        if action in ["stats", "schema-info", "reload-schema"]:
            action_parser = subparsers.add_parser(action, help=f"Get {action.replace('-', ' ')} for a cleaner")
            action_parser.add_argument("cleaner_name", choices=available_cleaners_keys, help="Type of cleaner to use")
            action_parser.add_argument("--dry-run", action="store_true", help="Preview what would be done")
        else:
            action_parser = subparsers.add_parser(action, help=f"{action.replace('-', ' ').title()} data")
            action_parser.add_argument("cleaner_name", choices=available_cleaners_keys, help="Type of cleaner to use")
            action_parser.add_argument("--dry-run", action="store_true", help=f"Preview what would be {action}ed")

            if action == "clean":
                action_parser.add_argument("--ids", dest="raw_data_ids", help="Comma-separated list of raw data IDs or ranges")
            elif action == "clean-by-status":
                action_parser.add_argument(
                    "--status",
                    dest="validation_status",
                    choices=validation_status_names,
                    help="Validation status to filter by",
                )
            elif action == "clean-recent":
                action_parser.add_argument("--hours", dest="hours", type=int, help="Number of hours to look back")

    subparsers.add_parser("list-cleaners", help="List all available cleaners")

    return parser


def main():
    cli = CleanerCLI()
    available_cleaner_keys = list(cli.available_cleaners.keys())
    cleaning_actions_list = list(cli.cleaning_actions_list.keys())

    parser = setup_args(available_cleaner_keys, cli.validation_status_names, cleaning_actions_list)

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()

    log_config = ConfigLoader.get_logging_config()
    setup_logging(
        log_level=args.log_level,
        log_to_file=not args.no_log_file,
        log_dir=log_config.get("log_directory", "logs"),
    )

    result = None
    success = True

    try:
        command = args.command

        if command in cli.cleaning_actions_list:
            raw_data_ids = None
            if hasattr(args, "raw_data_ids") and args.raw_data_ids:
                raw_data_ids = cli.get_id_range(args.raw_data_ids)

            validation_status = getattr(args, "validation_status", None)
            hours = getattr(args, "hours", None)

            result = cli.execute_cleaning(
                action=command,
                cleaner_name=args.cleaner_name,
                raw_data_ids=raw_data_ids,
                validation_status=validation_status,
                hours=hours,
                dry_run=getattr(args, "dry_run", False),
            )

        elif command == "list-cleaners":
            cli.list_all_cleaners()
            success = True

        else:
            parser.print_help()
            return

        # Handle output
        if result and args.json_output:
            print(json.dumps(result, indent=2, default=str))
        elif result and command != "list-cleaners":
            if result.get("success"):
                print("Operation completed successfully!")

                if "statistics" in result:
                    stats = result["statistics"]
                    print(f"\tCleaner: {stats.get('cleaner_name')} v{stats.get('cleaner_version')}")
                    print(f"\tTotal cleaned: {stats.get('total_cleaned', 0)}")
                    print(f"\tValid: {stats.get('valid_count', 0)}")
                    print(f"\tInvalid: {stats.get('invalid_count', 0)}")
                    print(f"\tSuccess rate: {stats.get('success_rate', 0):.1f}%")
                elif "schema_info" in result:
                    schema = result["schema_info"]
                    print(f"\tSchema cached: {schema.get('schema_cached')}")
                    print(f"\tSchema ID: {schema.get('schema_id')}")
                    print(f"\tValidator available: {schema.get('validator_available')}")
                elif "success_count" in result:
                    print(f"\tSuccessful: {result.get('success_count', 0)}")
                    print(f"\tFailed: {result.get('error_count', 0)}")
                    if "total_time_seconds" in result:
                        print(f"\tTotal time: {result.get('total_time_seconds', 0):.2f}s")
                        print(f"\tAverage time per record: {result.get('average_time_per_record', 0):.2f}s")
                elif "message" in result:
                    print(f"\t{result['message']}")
            else:
                print("Operation failed!")
                if "error" in result:
                    print(f"Error: {result['error']}")
                success = False

    except KeyboardInterrupt:
        print("Operation cancelled by user")
        sys.exit(130)

    except Exception as general_error:
        print(f"Unexpected error: {general_error}")
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
