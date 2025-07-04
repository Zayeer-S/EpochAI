import wikipedia

try:
    from predictai.common.config_loader import ConfigLoader
    
    all_collector_configs = ConfigLoader.get_all_collector_configs()
     
    print("=" * 30)
    print("LOADED CONFIGURATION FROM config.yml")
    print("=" * 30)
    
except ImportError:
    print("ERROR: Could not import config_loader.py. Aborting script.")
    exit(1)

try:
    wiki_config = all_collector_configs.get('wikipedia')
except Exception as e:
    print("ERROR: NO WIKIPEDIA CONFIGURATION FOUND.")
    exit(1)
    
test_pages = []

for language, politician_list in wiki_config['politicians'].items():
    test_pages.extend(politician_list)
    
for language, topic_list in wiki_config['political_topics'].items():
    test_pages.extend(topic_list)
    
for language, template_list in wiki_config['political_events_template'].items(0):
    for year in wiki_config['collection_years']:
        for template in template_list:
            test_pages.append(template.format(year=year))

print(f"=== TESTING WIKIPEDIA PAGE ACCESS ===")
print(f"Testing {len(test_pages)} pages from configuration")

for page_title in test_pages:
    print(f"\nTesting: '{page_title}'")
    try:
        page = wikipedia.page(page_title)
        print(f"\tSuccess: Found '{page.title}'")
        print(f"\tURL: {page.url}")
        print(f"\tSummary: {page.summary[:100]}...")
        
    except wikipedia.exceptions.DisambiguationError as e:
        print(f"Disambiguation error, multiple options found")
        print(f"\tOptions: {e.options[:5]}")
        
        try:
            print(f"\tTrying the first option: '{e.options[0]}'")
            page = wikipedia.page(e.options[0])
            print(f"\t\tSuccess with first option: '{page.title}'")
                  
        except Exception as e2:
            print(f"\t\tFirst option also failed: {e2}")
            
    except wikipedia.exceptions.PageError:
        print(f"\tSearching for similar pages...")
        try:
            search_results = wikipedia.search(page_title, results = 5)
            print(f"\t\tSearch suggestions: {search_results}")
        except:
            print(f"\t\tSearch failed")
    
    except Exception as e:
        print(f"\t\tError: {e}")
        
print("\n" + "=" * 30)

# Print Config Summary    
if wiki_config:
    print("CONFIGURATION SUMMARY")
    print(f"\tLanguages: {wiki_config['api']['language']}")
    print(f"\tPoliticians: {wiki_config['politicians']}")
    print(f"\tTopics: {wiki_config['political_topics']}")
    print(f"\tEvents: {wiki_config['political_events']}")
else:
    print("Create config.yml with wikipedia section to manage all page titles!")