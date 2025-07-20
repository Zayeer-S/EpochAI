import wikipedia

from epochai.common.config_loader import ConfigLoader
from epochai.common.wikipedia_utils import WikipediaUtils

try:
    all_collector_configs = ConfigLoader.get_all_collector_configs()
     
    print("=" * 30)
    print("LOADED CONFIGURATION FROM config.yml")
    print("=" * 30)
    
except ImportError:
    print("ERROR: Could not import config_loader.py. Aborting script.")
    exit(1)
    
wiki_config = all_collector_configs.get('wikipedia')
if not wiki_config:
    print("ERROR: NO WIKIPEDIA CONFIGURATION FOUND.")
    exit(1)
    
wiki_utils = WikipediaUtils(wiki_config)
if not wiki_utils:
    print("ERROR: NO WIKIPEDIA UTILS FOUND.")
    exit(1)
    
test_pages = []

# Build test pages list with proper structure (title, language, type)
for language_code, politician_list in wiki_config['politicians'].items():
    for politician in politician_list:
        test_pages.append((politician, language_code, 'politician'))
    
for language_code, topic_list in wiki_config['political_topics'].items():
    for topic in topic_list:
        test_pages.append((topic, language_code, 'topic'))
    
for language_code, template_list in wiki_config['political_events_template'].items():
    for year in wiki_config['collection_years']:
        for template in template_list:
            formatted_event = template.format(year=year)
            test_pages.append((formatted_event, language_code, 'event'))

print(f"=== TESTING WIKIPEDIA PAGE ACCESS ===")
print(f"Testing {len(test_pages)} pages from configuration")

success_count = 0
failure_count = 0

for page_title, language_code, page_type in test_pages:
    print(f"\nTesting ({language_code}) ({page_type}): '{page_title}'")
    
    page = wiki_utils.get_wikipedia_page(page_title, language_code)
    
    if page:
        print(f"\tSuccess: Found '{page.title}'")
        print(f"\tURL: {page.url}")
        print(f"\tSummary: {page.summary[:100]}...")
        success_count += 1
    else:
        print(f"\tFailed to find '{page_title}'. Attempting search...")
        
        search_results = wiki_utils.search_using_config(page_title, language_code)
        
        if search_results:
            print(f"\tSearch Suggestions: {search_results[:3]}")
            print(f"\tRecommendation: Use '{search_results[0]}' instead of '{page_title}'")
        else:
            print(f"\tNo search results found - '{page_title}' may not exist on Wikipedia")
            
        failure_count += 1

print(f"\n" + "=" * 50)
print(f"TEST RESULTS SUMMARY")
print(f"=" * 50)
print(f"Total pages tested: {len(test_pages)}")
print(f"Successful retrievals: {success_count}")
print(f"Failed retrievals: {failure_count}")
print(f"Success rate: {(success_count/len(test_pages)*100):.1f}%")

print(f"\n" + "=" * 30)
print("CONFIGURATION SUMMARY")
print(f"=" * 30)

for collector_name, config in all_collector_configs.items():
    if config:
        print(f"{collector_name.upper()} COLLECTOR:")
        if collector_name == 'wikipedia':
            print(f"\tLanguages: {wiki_config['api']['language']}")
            print(f"\tPoliticians: {wiki_config['politicians']}")
            print(f"\tTopics: {wiki_config['political_topics']}")
            
            formatted_events = []
            for language_code, template_list in config['political_events_template'].items():
                for year in config['collection_years']:
                    for template in template_list:
                        formatted_events.append(template.format(year=year))
            print(f"\tEvents: {formatted_events}")
    else:
        print(f"{collector_name.upper()} COLLECTOR: Not configured")