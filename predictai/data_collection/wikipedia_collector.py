import os
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable

import pandas as pd
import wikipedia

from predictai.common.config_loader import ConfigLoader
from predictai.common.logging_config import setup_logging, get_logger

class WikipediaPoliticalCollector:
    def __init__(self):
        log_config = ConfigLoader.get_logging_config()
        
        setup_logging(
            log_level=log_config['level'],
            log_to_file=log_config['log_to_file'],
            log_dir=log_config['log_directory']
        )
        
        self.logger = get_logger(__name__)
        self.logger.info("Initialising WikipediaPoliticalCollector")
        
        self.config = ConfigLoader.get_wikipedia_collector_config()
        self.languages = self.config['api']['language']
        self.collected_data = []
        
    # TODO Decide if theres a better name for this or not, because it doesn't collect rather changes the language and calls a collector
    def _collect_from_wikipedia_all_languages(
        self, 
        items_by_language: Dict[str, List[str]], 
        collection_type: str,
        extra_data_func: Optional[Callable[[str], Dict[str, Any]]] = None
        ) -> list[dict[str, Any]]:
        """Changes the language of the data that is being collected from Wikipedia.

        Note:
            Calls _collect_from_wikipedia_current_language to actually collect the data.
        """
        
        self.logger.info(f"Collecting {collection_type} for {len(items_by_language)} items across {len(self.languages)} languages")
        
        all_collected_data = []
        
        for language_code, items in items_by_language.items():
            if not items:
                self.logger.info(f"No items to collect for '{language_code}', skipping this language...")
                continue
            
            try:
                wikipedia.set_lang(language_code)
                                
                self.logger.info(f"Collecting {collection_type} in language '{language_code}' for {len(items)} items")
                
                lang_data = self._collect_from_wikipedia_current_language(
                    items,
                    collection_type,
                    language_code,
                    extra_data_func
                )
                
                all_collected_data.extend(lang_data)
                
            except Exception as e:
                self.logger.error(f"Error setting language: {e}")
                continue

        return all_collected_data
        
    def _collect_from_wikipedia_current_language(
        self, 
        items: List[str], 
        collection_type: str, 
        language: str, 
        extra_data_func: Optional[Callable[[str], Dict[str, Any]]]=None
        ) -> list[dict[str, Any]]:
        """Collects data for a single language.
        
        Note:
            This single language is determined by _collect_from_wikipedia_all_languages.
        """
        
        self.logger.info(f"Collecting {collection_type} for {len(items)} items")
        
        rate_limit_delay = self.config['api']['rate_limit_delay']
        
        collected_data = []
        
        for item in items:
            try:
                self.logger.info(f"Collecting ({language}): {item}")
                page_data = self._get_wikipedia_page_meta_data(item, language)
                
                if page_data:
                    if extra_data_func:
                        extra_data = extra_data_func(item)
                        page_data.update(extra_data)
                    
                    collected_data.append(page_data)
                    self.logger.debug(f"Successfully collected ({language}): {item}")
                else:
                    self.logger.warning(f"Nothing collected for ({language}): {item}")
                    
                time.sleep(rate_limit_delay)
                
            except Exception as e:
                self.logger.error(f"Error collecting from language: ({language}) in item: {item} with error: {e}")
                continue
            
        self.logger.info(f"Collected {len(collected_data)} items in {language}")
        return collected_data
        
    def collect_political_events_for_years(self, years=None):
        """Collect yearly political event summaries from Wikipedia (e.g. "2023 in Politics" Page)."""
        if years is None:
            years = self.config['collection_years']
            
        events_to_collect = {}
        
        templates = self.config["political_events_template"]
        
        for language, template_list in templates.items():
            events_to_collect[language] = []
            for year in years:
                for template in template_list:
                    events_to_collect[language].append(template.format(year=year))    
                
        self.logger.info(f"Collecting political events for years: {years}")
        
        def add_year_data(event_title):
            """
            Extract year from the political events' title (e.g. get 2023 from "2023 in Politics") and attachess it as metadata.
            """
            for year_val in years:
                if str(year_val) in event_title:
                    return{'event_year': year_val}
            return {'event_year': 'unknown'}
        
        return self._collect_from_wikipedia_all_languages(
            events_to_collect,
            "political_events",
            extra_data_func=add_year_data
        )
    
    def collect_politician_pages(self):
        """Collects specific politician pages from wikipedia."""
        def add_politician_data(politician_name):
            return{'politician_name': politician_name}
        
        return self._collect_from_wikipedia_all_languages(
            self.config['politicians'],
            "politicians_data",
            extra_data_func=add_politician_data
        )
        
    def collect_political_topics(self):
        """Collect wiki pages for specific political topics"""
        def add_topic_data(topic_name):
            return {'topic_name': topic_name}
        
        return self._collect_from_wikipedia_all_languages(
            self.config['political_topics'],
            "political topics",
            extra_data_func=add_topic_data
        )
        
    def _get_wikipedia_page_meta_data(self, page_title, language):
        """Gets Wikipedia page content and meta data"""
        
        max_retries = self.config['api']['max_retries']
        rate_limit_delay = self.config['api']['rate_limit_delay']
        
        for attempt in range(max_retries):
            try:
                page = wikipedia.page(page_title)
                
                page_data = {
                    'title': page.title,
                    'summary': page.summary,
                    'content': page.content,
                    'url': page.url,
                    'categories': list(page.categories) if hasattr(page, 'categories') else [],
                    'links': list(page.links) if hasattr(page, 'links') else [],
                    'collected_at': datetime.now().isoformat(),
                    'source': f'wikipedia_{language}',
                    'language': language,
                    'page_id': getattr(page, 'pageid', None)
                }
                
                return page_data

            except wikipedia.exceptions.DisambiguationError as e:
                self.logger.warning(f"Disambiguation found for '{page_title}', trying the first option...")
                try:
                    first_option = e.options[0]
                    return self._get_wikipedia_page_meta_data(first_option, language)
                except:
                    self.logger.warning(f"First option '{first_option}' did not work.")
                    return None
                
            except wikipedia.exceptions.PageError:
                self.logger.warning(f"Page not found: {page_title}")
                return None
            
            except Exception as e:
                if attempt < max_retries - 1:
                    self.logger.debug(f"Attempt {attempt + 1} failed: {e}")
                    self.logger.debug("Retrying...")
                    time.sleep(rate_limit_delay)
                else:
                    self.logger.debug(f"Final attempt failed: {e}")
                    return None
                
        return None

    def search_political_topics(self, query, language, max_results=None):
        """Search wikipedia for political trends in a specific language
        
        Args:
            query (str): Search term to look for
            language (str): Language code to search in (e.g. 'en')
            max_results (int): Maximum number of results to return
            
        Returns:
            A list of collected page data from the specified language
        """
        self.logger.info(f"Searching wikipedia for: '{query}' in language: '{language}'")
        
        if max_results is None:
            max_results = self.config['api']['search_max_results']
        
        try:
            wikipedia.set_lang(language)
            
            search_results = wikipedia.search(query, results=max_results)
            self.logger.info(f"Found {len(search_results)} search results in '{language}'")
            
            if not search_results:
                self.logger.warning(f"No results found for '{query}' in '{language}'")
                return []
            
            def add_search_data(result):
                return {'search_query': query}
            
            collected_pages = self._collect_from_wikipedia_current_language(
                search_results[:max_results],
                f"search results for '{query}'",
                language,
                extra_data_func=add_search_data
            )
            
            return collected_pages
    
        except Exception as e:
            self.logger.error(f"Search error in '{language}': {e}")
            return []
        
    def save_data(
        self, 
        data: List[Dict[str, Any]], 
        filename: Optional[str] = None,
        data_type: str ="political_data"
        ) -> Optional[str]:
        """Saves the collected data
        
        Returns:
            "filepath" if successful, "None" if there is no data to save
        """
        if not data:
            self.logger.warning("No data collected to save")
            return None
        
        df = pd.DataFrame(data)
        
        if filename is None:
            current_timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"wikipedia_{data_type}_{current_timestamp}.csv"
            
        output_dir = self.config['data_output']['directory']
        os.makedirs(output_dir, exist_ok=True)
        
        filepath = os.path.join(output_dir, filename)
        df.to_csv(filepath, index=False)
        
        self.logger.info(f"Data saved to following file path: {filepath}")
        self.logger.info(f"Total records saved: {len(df)}")
        
        if 'language' in df.columns:
            language_counts = df['language'].value_counts()
            self.logger.info(f"Records by language:")
            for language, count in language_counts.items():
                self.logger.info(f"{language}: {count} records")
        
        return filepath
    
    def wikipedia_political_data_orchestrator(self):
        """Collect political data from multiple sources."""
        
        all_political_data =  []
        
        self.logger.info("=== Starting comprehensive political data collection ===")
        
        self.logger.info(f"1. Collecting yearly political event summaries...")
        previous_year_wiki_data = self.collect_political_events_for_years()
        
        self.logger.info("2. Collecting politician data...")

        politician_wiki_data = self.collect_politician_pages()
        
        self.logger.info("3. Collecting political topic data...")
        topic_wiki_data = self.collect_political_topics()
        
        all_political_data.extend(previous_year_wiki_data)
        all_political_data.extend(politician_wiki_data)
        all_political_data.extend(topic_wiki_data)
        
        self.logger.info(f"==== Collection Complete ===")
        self.logger.info(f"Total data points collected: {len(all_political_data)}")
        
        if all_political_data:
            df_temp = pd.DataFrame(all_political_data)
            if 'language' in df_temp.columns:
                language_counts = df_temp['language'].value_counts()
                self.logger.info(f"Data points by language:")
                for language, count in language_counts.items():
                    self.logger.info(f"{language}: {count} records")
        
        return all_political_data
    
def main():
    collector = WikipediaPoliticalCollector()
    
    collector.logger.info("Wikipedia Political Data Collector")
    collector.logger.info("=" * 30)
        
    all_political_data = collector.wikipedia_political_data_orchestrator()
    
    if all_political_data:
        config_data_type = collector.config['data_output']['default_type']
        filepath = collector.save_data(all_political_data, data_type=config_data_type)
        
        df = pd.DataFrame(all_political_data)
        collector.logger.info("=" * 30)
        collector.logger.info(f"DATA SUMMARY STATISTICS")
        collector.logger.info(f"Total articles collected: {len(df)}")
        collector.logger.info(f"Average content length: {df['content'].str.len().mean():.0f} characters")
        collector.logger.info(f"Data saved to: {filepath}")
        
        
    else:
        collector.logger.info("No data collected. Unknown reason.")
        
if __name__ == "__main__":
    main()