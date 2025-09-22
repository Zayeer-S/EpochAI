import argparse
import sys

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.enums import CollectionTypeNames
from epochai.common.logging_config import get_logger, setup_logging
from epochai.data_collection.importers.fivethirtyeight_csv_importer import FiveThirtyEightCSVImporter


def main():
    parser = argparse.ArgumentParser(
        description="Import FiveThirtyEight CSV data into collection_targets",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "action",
        choices=["import", "stats"],
        help="Action to perform",
    )

    parser.add_argument(
        "--csv-file",
        type=str,
        help="Path to CSV file (optional - uses default path if not provided)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview what would be imported without actually importing",
    )

    parser.add_argument(
        "--type",
        choices=[CollectionTypeNames.POST_2016.value, CollectionTypeNames.PRE_2016.value],
        help="Date range of data collected",
    )

    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
        help="Set logging level",
    )

    args = parser.parse_args()

    log_config = ConfigLoader.get_logging_config()
    setup_logging(
        log_level=args.log_level,
        log_to_file=log_config.get("log_to_file", True),
        log_dir=log_config.get("log_directory", "logs"),
    )

    logger = get_logger(__name__)

    try:
        importer = FiveThirtyEightCSVImporter()

        if args.action == "import":
            logger.info(f"Starting import (dry_run={args.dry_run})")

            success = importer.import_csv_to_targets(
                csv_filepath=args.csv_file,
                dry_run=args.dry_run,
                collection_type=args.type,
            )

            if success:
                if args.dry_run:
                    print("Dry run completed successfully - no data was imported")
                else:
                    print("Import completed successfully")
                sys.exit(0)
            else:
                print("Import failed - check logs for details")
                sys.exit(1)

        elif args.action == "stats":
            logger.info("Getting import statistics")

            stats = importer.get_import_statistics()

            if stats:
                print("FiveThirtyEight Import Statistics:")
                print(f"\tTotal targets: {stats.get('total_targets', 0)}")
                print(f"\tCollector: {stats.get('collector_name', 'Unknown')}")
                print(f"\tCollection type: {stats.get('collection_type', 'Unknown')}")

                by_status = stats.get("by_status_id", {})
                if by_status:
                    print("\tBy status ID:")
                    for status_id, count in by_status.items():
                        print(f"\t\tStatus ID {status_id}: {count} targets")
                sys.exit(0)
            else:
                print("No statistics available - may not have imported data yet")
                sys.exit(1)

    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        sys.exit(130)

    except Exception as general_error:
        logger.error(f"Unexpected error: {general_error}")
        print(f"Error: {general_error}")
        sys.exit(1)


if __name__ == "__main__":
    main()
