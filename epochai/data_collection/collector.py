# ruff: noqa: E501
import argparse
import importlib
import inspect
from pathlib import Path
import sys
from typing import Any, Dict, List, Optional, Union

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.collection_targets_manager import CollectionTargetManager
from epochai.common.enums import CollectionStatusNames
from epochai.common.logging_config import get_logger, setup_logging
from epochai.data_collection.collectors.base_collector import BaseCollector


class CollectorNotFoundError(Exception):
    pass


class CollectorCLI:
    """Collectors orchestrator and CLI"""

    def __init__(self):
        self.logger = get_logger(__name__)

        self.coll_targets = CollectionTargetManager()

        self.available_collectors = self._get_available_collectors()

    def _get_available_collectors(self) -> Dict[str, Any]:
        """Gets collector names without suffix mapped to collector class"""
        available_collectors = {}

        current_dir = Path(__file__).parent
        collectors_dir = current_dir / "collectors"

        if not collectors_dir.exists():
            self.logger.warning(f"Collectors directory not found: {collectors_dir}")
            return {}

        for filepath in collectors_dir.glob("*_collector.py"):
            module_name = filepath.stem

            try:
                full_module_name = f"epochai.data_collection.collectors.{module_name}"
                module = importlib.import_module(full_module_name)

                for class_name, class_obj in inspect.getmembers(module, inspect.isclass):
                    if (
                        class_name.lower().endswith("collector")
                        and class_obj.__module__ == full_module_name
                        and class_name != "BaseCollector"
                    ):
                        suffixless_name = module_name.replace("_collector", "")
                        available_collectors[suffixless_name] = class_obj

                        self.logger.debug(f"Discovered collector: '{suffixless_name}': {class_name}")
                        break

            except Exception as general_error:
                self.logger.warning(f"Error loading collector from {module_name}: {general_error}")

        return available_collectors

    def _get_available_collection_types(
        self,
        collector_name: str,
    ) -> List[str]:
        """Gets a list of collection types that have uncollected data in the passed-in collector_name"""
        try:
            return self.coll_targets.get_list_of_uncollected_types_by_collector_name(
                collector_name=collector_name,
                unique_types_only=True,
                collection_status=CollectionStatusNames.NOT_COLLECTED.value,
            )
        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected collection types for {collector_name}: {general_error}",
            )
            return []

    def _get_available_language_codes(self, collector_name: str) -> List[str]:
        """Gets a list of language codes that have uncollected data in the passed-in collector_name"""
        try:
            return self.coll_targets.get_list_of_uncollected_language_codes_by_collector_name(
                collector_name=collector_name,
                unique_types_only=True,
                collection_status=CollectionStatusNames.NOT_COLLECTED.value,
            )
        except Exception as general_error:
            self.logger.error(
                f"Error getting uncollected language codes for {collector_name}: {general_error}",
            )
            return []

    def _get_id_range(self, collection_targets_id_range: str) -> List[int]:
        """
        Gets inputed id ranges

        Example:
            User inputs "10-30", this returns "10, 11, 12... 29, 30"
            User input "11", this returns "11"
            User inputs "13-15, 19", this returns "13, 14, 15, 19"
        """
        if "," not in collection_targets_id_range and "-" not in collection_targets_id_range:
            try:
                return [int(collection_targets_id_range)]
            except ValueError:
                self.logger.error(f"Invalid single ID: {collection_targets_id_range}")
                return []

        id_list: List[int] = []
        ranges = [r.strip() for r in collection_targets_id_range.split(",")]

        for range_str in ranges:
            if "-" in range_str:
                individual_parts = range_str.split("-")
                if len(individual_parts) != 2:
                    self.logger.error(f"Invalid range format: '{range_str}'")
                    continue

                try:
                    start_point = int(individual_parts[0])
                    end_point = int(individual_parts[1])

                    if end_point >= start_point:
                        id_list.extend(range(start_point, end_point + 1))
                    else:
                        self.logger.error(
                            f"Second number in inputted range {start_point}-{end_point} must be >= first number",
                        )
                except ValueError as value_error:
                    raise ValueError(
                        f"Invalid numbers in range: '{range_str}' - {value_error}",
                    ) from value_error
            else:  # its a single number after a range i.e. 5 in input: "1-3, 5"
                try:
                    id_list.append(int(range_str))
                except ValueError as value_error:
                    raise ValueError(f"Invalid ID: {range_str}") from value_error
        return id_list

    def _get_collector_instance(
        self,
        collector_name: str,
    ) -> Optional[BaseCollector]:
        if collector_name not in self.available_collectors:
            raise CollectorNotFoundError(
                f"Unknown collector type: {collector_name}"
                f"Available collectors: {', '.join(self.available_collectors.keys())}",
            )
        try:
            result: BaseCollector = self.available_collectors[collector_name]()
            return result
        except Exception as general_error:
            self.logger.error(f"Failed to initalize {collector_name} collector: {general_error}")
            return None

    def get_status(
        self,
        collector_name: str,
    ) -> Dict[str, Any]:
        """Gets collection status for a collector"""
        try:
            if collector_name not in self.available_collectors:
                return {"success": False, "error": f"Unknown collector: {collector_name}"}

            status = self.coll_targets.get_collection_status_summary()
            all_available_types = self._get_available_collection_types(collector_name)
            available_languages = {}

            for type in all_available_types:
                each_language = self._get_available_language_codes(type)
                available_languages[type] = each_language

            type_details = {}
            for each_type in all_available_types:
                targets = self.coll_targets.get_uncollected_targets_by_type(each_type)
                total_uncollected = sum(len(lang_targets) for lang_targets in targets.values())
                type_details[each_type] = {
                    "uncollected_count": total_uncollected,
                    "languages": list(targets.keys()),
                    "targets_by_language": targets,
                }

            return {
                "success": True,
                "collector_name": collector_name,
                "available_types_and_their_languages": (", ".join(available_languages)),
                "type_details": type_details,
                "overall_status": status,
            }

        except Exception as general_error:
            self.logger.error(f"Error getting status for {collector_name}: {general_error}")
            return {"success": False, "error": str(general_error)}

    def _validate_user_input(
        self,
        collector: Any,
        collector_name: Optional[str],
        collection_type: Optional[List[str]] = None,
        target_ids: Optional[List[int]] = None,
        language_code: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        errors = []
        if not collector:
            errors.append("Failed to initialize collector")

        if collection_type:
            available_collection_types = self._get_available_collection_types(collector_name)
            invalid_types = [ct for ct in collection_type if ct not in available_collection_types]
            if invalid_types:
                errors.append(f"Invalid collection types were input: {invalid_types}")

        if language_code:
            available_language_codes = self._get_available_language_codes(collector_name)
            invalid_languages = [lc for lc in language_code if lc not in available_language_codes]
            if invalid_languages:
                errors.append(f"Invalid language codes were input: {invalid_languages}")

        if target_ids is not None and not target_ids:
            errors.append("Failed to get list of IDs to collect")

        return {"success": False, "error": errors} if errors else {"success": True}

    def collect(
        self,
        collector_name: Optional[str],
        collection_type: Optional[List[str]] = None,
        id_list: Optional[str] = None,
        language_code: Optional[List[str]] = None,
        collection_status: str = CollectionStatusNames.NOT_COLLECTED.value,
        dry_run: bool = False,
    ) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Collects all collection_target_ids that were inputted by the user"""
        if collection_type and id_list:  # Notifying user about wrong inputs is more important than dry run
            raise TypeError(
                f"Specified collection type and specified IDs are mutually exclusive ({collection_type}, {id_list})",
            )

        if dry_run:
            return self.get_status(collector_name)

        target_ids: Optional[List[int]] = None
        if id_list:
            target_ids = self._get_id_range(id_list)
            return []  # TODO

        collector = self._get_collector_instance(collector_name)

        check_for_errors = self._validate_user_input(
            collector=collector,
            collector_name=collector_name,
            collection_type=collection_type,
            target_ids=target_ids,
            language_code=language_code,
        )
        if not check_for_errors["success"]:
            self.logger.error(f"Validation failed: {check_for_errors['error']}")
            return []

        self.logger.info(f"Cleaning targets with {collector_name} collector")
        try:
            self.logger.info(
                f"Starting Uncollected Data Collection for {collector_name.capitalize()} Collector",
            )
            return collector.collect_data(
                collection_types=collection_type,
                target_ids=target_ids,
                language_codes=language_code,
                collection_status=collection_status,
            )

        except Exception as general_error:
            self.logger.error(f"Error in collection: {general_error}")
            return []

    def retry(
        self,
    ) -> Dict[str, Any]:
        """Retries all failed collections"""
        # TODO
        return {"success": False, "error": "Retry functionality not yet implemented"}


def setup_args(available_collector_keys: List[str]) -> argparse.ArgumentParser:
    """Sets up CLI args"""

    parser = argparse.ArgumentParser(
        description="EpochAI Data Collector CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
        Examples:
            # Collect all uncollected data from a collector_name
                python collector.py collect wikipedia

            # Collect all uncollected data from a collection_type in a collector_name
                python collector.py collect wikipedia --type political_events

            # Collect with language filter
                python collector.py collect wikipedia --language en

            # Collect specific IDs (NOT YET IMPLEMENTED)
                python collector.py collect wikipedia --ids "1-4, 5"

            # Get status
                python collector.py status wikipedia

            # Dry run
                python collector.py collect wikipedia --dry-run
        """,
    )

    parser.add_argument("--log-level", dest="log_level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], default="INFO", help="Set logging level")  # fmt: skip
    parser.add_argument("--no-log-file", dest="log_to_file", action="store_true", help="Disables logging to file")  # fmt: skip

    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    collect_parser = subparsers.add_parser("collect", help="Collects all currently uncollected data")  # fmt: skip
    collect_parser.add_argument("collector_name", choices=available_collector_keys, help="Type of collector to use")  # fmt: skip
    collect_parser.add_argument(
        "--type",
        dest="collection_type",
        type=lambda x: [s.strip() for s in x.split(',')],
        help="Specific collection type to collect",
    )  # fmt: skip
    collect_parser.add_argument("--ids", dest="id_list", help="List of IDs or ranges (e.g. '1, 2' or '8-10' or '1, 2, 8-10')")  # fmt: skip
    collect_parser.add_argument(
        "--language",
        dest="language_code",
        type=lambda x: [s.strip() for s in x.split(',')],
        help="Language filter (e.g., en, es)",
    )  # fmt: skip
    collect_parser.add_argument("--dry-run", dest="dry_run", action="store_true", help="Preview what would be collected without actually collecting")  # fmt: skip

    status_parser = subparsers.add_parser("status", help="Shows what uncollected data is prensent in a collector")  # fmt: skip
    status_parser.add_argument("collector_name", choices=available_collector_keys)

    retry_parser = subparsers.add_parser("retry", help="Retry failed collections")
    retry_parser.add_argument(
        "collector_name",
        choices=available_collector_keys,
        help="Collector type to retry",
    )
    retry_parser.add_argument("--failed", action="store_true", help="Only retry failed items")

    return parser


def main():
    cli = CollectorCLI()
    available_collector_keys = list(cli.available_collectors.keys())
    parser = setup_args(available_collector_keys)

    if len(sys.argv) == 1:
        parser.print_help()
        return

    args = parser.parse_args()

    log_config = ConfigLoader.get_logging_config()
    setup_logging(
        log_level=args.log_level,
        log_to_file=not args.log_to_file,
        log_dir=log_config.get("log_directory", "logs"),
    )

    result = None
    success = True

    try:
        if args.command == "collect":
            options_count = sum([bool(args.collection_type), bool(args.id_list)])
            if options_count > 1:
                print("Error --type and --ids are mutually exclusive")
                sys.exit(1)

            if args.id_list:
                result = cli.collect(
                    collector_name=args.collector_name,
                    id_list=args.id_list,
                    language_code=args.language_code,
                    dry_run=args.dry_run,
                )
            elif args.collection_type:
                result = cli.collect(
                    collector_name=args.collector_name,
                    collection_type=args.collection_type,
                    language_code=args.language_code,
                    dry_run=args.dry_run,
                )
            else:
                result = cli.collect(
                    collector_name=args.collector_name,
                    language_code=args.language_code,
                    dry_run=args.dry_run,
                )

        elif args.command == "status":
            result = cli.get_status(args.collector_name)

        elif args.command == "retry":
            result = cli.retry()  # TODO

        else:
            parser.print_help()
            return

        if result:
            if args.command == "status":
                if isinstance(result, dict) and result.get("success"):
                    collector_name = result["collector_name"].title()
                    print(f"\n{collector_name} Collector Status:")
                    print()

                    overall_status = result.get("overall_status", {})
                    summary = overall_status.get("summary", {}) if isinstance(overall_status, dict) else {}
                    by_type_lang = (
                        overall_status.get("by_type_language_status", [])
                        if isinstance(overall_status, dict)
                        else []
                    )

                    status_groups: Dict[str, Dict[str, Dict[str, int]]] = {}
                    for item in by_type_lang:
                        status = item.get("collection_status_name", "unknown")
                        if status not in status_groups:
                            status_groups[status] = {}

                        col_type = item.get("collection_type", "unknown")
                        if col_type not in status_groups[status]:
                            status_groups[status][col_type] = {}

                        lang = item.get("language_code", "unknown")
                        count = item.get("count", 0)
                        status_groups[status][col_type][lang] = count

                    status_display_names = {
                        "not_collected": "Uncollected targets",
                        "failed": "Failed collection",
                        "failed_check": "Failed check",
                        "in_progress": "In progress",
                        "collected": "Successfully collected",
                        "needs_retry": "Needs retry",
                        "skipped": "Skipped",
                    }

                    for status, display_name in status_display_names.items():
                        if status in status_groups and status_groups[status]:
                            print(f"\t{display_name}:")

                            for col_type, languages in status_groups[status].items():
                                print(f"\t\ttype: '{col_type}'")
                                print("\t\tlanguages: ", end="")

                                lang_items = list(languages.items())
                                if len(lang_items) == 1:
                                    lang, count = lang_items[0]
                                    print(f"'{lang}' -> {count}")
                                else:
                                    print(f"'{lang_items[0][0]}' -> {lang_items[0][1]}")
                                    for lang, count in lang_items[1:]:
                                        print(f"\t\t\t   '{lang}' -> {count}")

                                total = sum(languages.values())
                                print(f"\t\ttotal: {total}")
                                print()
                        elif status == "not_collected":
                            print(f"\t{display_name}:")
                            print("\t\tNone")
                            print()

                    if summary:
                        print("\tOverall Summary:")
                        print(f"\t\tTotal targets: {summary.get('total_targets', 0)}")
                        print(
                            f"\t\tCollection progress: {summary.get('collected', 0)}/{summary.get('total_targets', 0)} ({summary.get('collection_percentage', 0):.1f}%)",
                        )
                        if summary.get("failed", 0) > 0:
                            print(f"\t\tFailed: {summary.get('failed', 0)}")
                        if summary.get("in_progress", 0) > 0:
                            print(f"\t\tIn progress: {summary.get('in_progress', 0)}")

                elif isinstance(result, dict):
                    print(f"Error getting status: {result.get('error', 'Unknown error')}")
                    success = False
                else:
                    print("Error: Unexpected result type for status command")
                    success = False

            elif args.command == "collect":
                if isinstance(result, list):
                    if result:
                        print("\nCollection completed successfully!")
                        print(f"Processed {len(result)} items")

                        successful = sum(1 for item in result if item.get("success", True))
                        failed = len(result) - successful

                        if successful > 0:
                            print(f"\tSuccessfully collected: {successful}")
                        if failed > 0:
                            print(f"\t Failed to collect: {failed}")
                            success = False
                    else:
                        print("Collection completed - no items to process or validation failed")
                        success = False

                elif isinstance(result, dict):
                    if result.get("success"):
                        print("Dry run completed successfully - showing what would be collected:")
                        if result.get("type_details"):
                            for type_name, details in result["type_details"].items():
                                print(f"  {type_name}: {details['uncollected_count']} items")
                    else:
                        print(f"Collection failed: {result.get('error', 'Unknown error')}")
                        success = False

            elif args.command == "retry":
                # For retry command, result should be a dict
                if isinstance(result, dict):
                    if result.get("success"):
                        print("Retry completed successfully")
                    else:
                        print(f"Retry failed: {result.get('error', 'Unknown error')}")
                        success = False
                else:
                    print("Error: Unexpected result type for retry command")
                    success = False
        else:
            print("Operation completed with no results")
            success = False

    except KeyboardInterrupt:
        print("Operation cancelled by user")
        sys.exit(130)

    except Exception:
        print("Unexpected error")
        success = False

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
