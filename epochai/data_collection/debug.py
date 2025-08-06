from datetime import datetime
import time

from epochai.common.config.config_loader import ConfigLoader
from epochai.common.database.dao.collection_configs_dao import CollectionConfigsDAO
from epochai.common.database.dao.debug_wikipedia_results_dao import DebugWikipediaResultsDAO
from epochai.common.logging_config import get_logger
from epochai.common.utils.wikipedia_utils import WikipediaUtils
from epochai.database_savers.wikipedia_saver import WikipediaSaver

try:
    all_collector_configs = ConfigLoader.get_all_collector_configs()

    print("=" * 30)
    print("LOADED CONFIGURATION FROM config.yml")
    print("=" * 30)

except ImportError:
    print("ERROR: Could not import config_loader.py. Aborting script.")
    exit(1)

wiki_config = all_collector_configs.get("wikipedia")
if not wiki_config:
    print("ERROR: NO WIKIPEDIA CONFIGURATION FOUND.")
    exit(1)

wiki_utils = WikipediaUtils(wiki_config)
if not wiki_utils:
    print("ERROR: NO WIKIPEDIA UTILS FOUND.")
    exit(1)

logger = get_logger(__name__)
debug_wikipedia_results_dao = DebugWikipediaResultsDAO()
collection_config_dao = CollectionConfigsDAO()
wikipedia_saver = WikipediaSaver()

test_pages = []

# Build test pages list with proper structure (title, language, type)
for _language_code, politician_list in wiki_config["politicians"].items():
    for politician in politician_list:
        test_pages.append((politician, _language_code, "politician"))

for _language_code, topic_list in wiki_config["political_topics"].items():
    for topic in topic_list:
        test_pages.append((topic, _language_code, "topic"))

for _language_code, template_list in wiki_config["political_events_template"].items():
    for year in wiki_config["collection_years"]:
        for template in template_list:
            formatted_event = template.format(year=year)
            test_pages.append((formatted_event, _language_code, "event"))

print("=== TESTING WIKIPEDIA PAGE ACCESS ===")
print(f"Testing {len(test_pages)} pages from configuration")

success_count = 0
failure_count = 0

for page_title, _language_code, page_type in test_pages:
    print(f"\nTesting ({_language_code}) ({page_type}): '{page_title}'")

    start_time = time.time()

    collection_config_id = wikipedia_saver.get_collection_config_id(page_type, _language_code, page_title)

    page = wiki_utils.get_wikipedia_page(page_title, _language_code)

    end_time = time.time()
    test_duration_ms = int((end_time - start_time) * 1000)

    if page:
        print(f"\tSuccess: Found '{page.title}'")
        print(f"\tURL: {page.url}")
        print(f"\tSummary: {page.summary[:100]}...")

        if collection_config_id:
            try:
                debug_wikipedia_results_dao.create_debug_result(
                    collection_config_id=collection_config_id,
                    search_term_used=page_title,
                    language_code_used=_language_code,
                    test_status="success",
                    search_results_found=[page.title],
                    error_message="",
                    test_duration=test_duration_ms,
                )
                logger.info(f"Saved test result for '{page_title}' to database")

            except Exception as general_error:
                logger.error(
                    f"Failed to save successful test result to database for '{page_title}': {general_error}",
                )

        else:
            logger.warning(
                f"No collection_config_id found for '{page_title}', not logging test to database for this",
            )

        success_count += 1
    else:
        print(f"\tFailed to find '{page_title}'. Attempting search...")

        search_results = wiki_utils.search_using_config(page_title, _language_code)

        if search_results:
            print(f"\tSearch Suggestions: {search_results[:3]}")
            print(f"\tRecommendation: Use '{search_results[0]}' instead of '{page_title}'")

            if collection_config_id:
                try:
                    debug_wikipedia_results_dao.create_debug_result(
                        collection_config_id=collection_config_id,
                        search_term_used=page_title,
                        language_code_used=_language_code,
                        test_status="failed_with_suggestions",
                        search_results_found=search_results[:5],
                        error_message=f"Page not found, instead {len(search_results)} suggestions found",
                        test_duration=test_duration_ms,
                    )
                    logger.info(f"Saved failed test result with suggestions for '{page_title}' to database")

                except Exception as general_error:
                    logger.error(
                        f"Failed to save failed test result with suggestion to database for '{page_title}': {general_error}",  # noqa
                    )
        else:
            print(f"\tNo search results found - '{page_title}' may not exist on Wikipedia")

            if collection_config_id:
                try:
                    debug_wikipedia_results_dao.create_debug_result(
                        collection_config_id=collection_config_id,
                        search_term_used=page_title,
                        language_code_used=_language_code,
                        test_status="failed",
                        search_results_found=[],
                        error_message="No search results found, page might not exist",
                        test_duration=test_duration_ms,
                    )
                    logger.info(f"Saved completely failed test result for '{page_title}' to database")

                except Exception as general_error:
                    logger.error(
                        f"Failed to save completely failed test result to database for '{page_title}': {general_error}",  # noqa
                    )
        failure_count += 1

print("\n" + "=" * 50)
print("TEST RESULTS SUMMARY")
print("=" * 50)
print(f"Total pages tested: {len(test_pages)}")
print(f"Successful retrievals: {success_count}")
print(f"Failed retrievals: {failure_count}")
print(f"Success rate: {(success_count / len(test_pages) * 100):.1f}%")


print("\n" + "=" * 30)
print("DATABASE RESULTS SUMMARY")
print("=" * 30)
try:
    db_stats = debug_wikipedia_results_dao.get_debug_statistics()
    print(f"Total tests in database: {db_stats['total_tests']}")

    for status, stats in db_stats["summary"].items():
        print(
            f"{status.upper()}: {stats['count']} tests ({stats['percentage']:.1f%}) - Avg duration: {stats['avg_duration']:.0f}ms",  # noqa
        )

    print("\nBy language")
    for lang_stat in db_stats["by_language"]:
        lang = lang_stat["language_code_used"]
        total = lang_stat["test_count"]
        success = lang_stat["success_count"]
        failed = lang_stat["failed_count"]
        success_rate = (success / total * 100) if total > 0 else 0
        print(
            f"  {lang}: {total} tests ({success} success, {failed} failed) - {success_rate:.1f}% success rate",  # noqa
        )

except Exception as e:
    logger.error(f"Failed to retrieve database statistics: {e}")
    print("Could not retrieve database statistics")


print("\n" + "=" * 30)
print("CONFIGURATION SUMMARY")
print("=" * 30)

for collector_name, config in all_collector_configs.items():
    if config:
        print(f"{collector_name.upper()} COLLECTOR:")
        if collector_name == "wikipedia":
            print(f"\tLanguages: {wiki_config['api']['language']}")
            print(f"\tPoliticians: {wiki_config['politicians']}")
            print(f"\tTopics: {wiki_config['political_topics']}")

            formatted_events = []
            for _language_code, template_list in config["political_events_template"].items():
                for year in config["collection_years"]:
                    for template in template_list:
                        formatted_events.append(template.format(year=year))
            print(f"\tEvents: {formatted_events}")
    else:
        print(f"{collector_name.upper()} COLLECTOR: Not configured")


print("\n" + "=" * 30)
print("RECENTLY FAILED TESTS IN PAST 24 HOURS")
print("=" * 30)

try:
    recent_failed = debug_wikipedia_results_dao.get_failed_tests()
    recent_failed_24h = [
        test
        for test in recent_failed
        if test.created_at and (datetime.now() - test.created_at).total_seconds() < 86400
    ]

    if recent_failed_24h:
        for test in recent_failed_24h[:10]:
            print(f"  '{test.search_term_used}' ({test.language_code_used}) - {test.error_message}")
    else:
        print("  No failed tests in the last 24 hours!")

except Exception as e:
    logger.error(f"Failed to retrieve recent failed tests: {e}")
    print("  Could not retrieve recent failed tests")
