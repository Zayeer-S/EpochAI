import os
from datetime import datetime
from typing import Optional, List, Dict, Any, Callable

import pandas as pd

from epochai.common.config_loader import ConfigLoader
from epochai.common.logging_config import setup_logging, get_logger
from epochai.common.wikipedia_utils import WikipediaUtils
from epochai.database_savers.wikipedia_saver import WikipediaSaver

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
        
        self.config = ConfigLoader.get_wikipedia_config()
        self.languages = self.config['api']['language']
        
        self.wiki_utils = WikipediaUtils(self.config)
        
        self.data_config = ConfigLoader.get_data_config()
        self.wikipedia_saver = WikipediaSaver()
        
        self.save_to_database = self.data_config.get('data_output').get('database').get('save_to_database')
        
        if self.save_to_database:
            self.batch_size = self.data_config.get('data_output').get('database').get('batch_size')
            self.current_batch = []
            self.total_saved_to_db = 0
        else:
            self.collected_data = []
            
        self.current_language_code = ''
        
    def _handle_all_wikipedia_collection(
        self,
        items_by_language: Dict[str, List[str]],
        collection_type: str,
        extra_data_func: Optional[Callable[[str], Dict[str, Any]]]
        ) -> List[Dict[str, Any]]:
        """"""
        
        if not items_by_language:
            self.logger.warning(f"No items provided for {collection_type}")
            return []
        
        self.logger.info(f"Collecting {collection_type} for {len(items_by_language)} items across {len(self.languages)} languages")
        
        def collect_single_page_data(
            item: str,
            language_code: str
        ) -> Optional[Dict[str, Any]]:
            """"""
            self.logger.info(f"Collecting ({language_code}): {item}")
            
            if extra_data_func:
                extra_data = extra_data_func(item)
            else:
                extra_data = None
            
            page = self.wiki_utils.get_wikipedia_metadata(item, language_code, extra_data)
            
            if page:
                self.logger.debug(f"Successfully collected ({language_code}): {item}")
                
                if self.save_to_database:
                    self._add_to_batch([page], language_code)
            else:
                self.logger.warning(f"Nothing collected for ({language_code}): {item}")
                
            return page
        
        results_by_language = self.wiki_utils.process_items_by_language(
            items_by_language,
            collect_single_page_data
        )
        
        all_collected_data = []
        for language_code, results in results_by_language.items():
            if results:
                all_collected_data.extend(results)
                
        return all_collected_data
    
    def _add_to_batch(
        self,
        data_items: List[Dict[str, Any]],
        language_code: str
    ):
        """Adds data to current collection batch and saves via helper function if batch size is reached"""
        if not self.save_to_database:
            self.collected_data.extend(data_items)
            return
        
        for item in data_items:
            self.current_batch.append(item)
            
            if len(self.current_batch) >= self.batch_size:
                self._save_batch_if_needed(language_code)
                
    def _save_batch_if_needed(
        self,
        language_code: str
    ):
        """Saves current batch using DataUtils and resets batch"""
        if not self.current_batch:
            return
        
        config_id = 1 ## TODO GET THIS DYNAMICALLY VIA FINDING CLOSEST MATCH TO collector_name_id IN collection_configs
        
        success_count = self.wikipedia_saver.save_incrementally_to_database(
            collected_data=self.current_batch,
            config_id=config_id,
            language_code=language_code
        )
        
        if success_count >= 0:
            self.total_saved_to_db += len(self.current_batch)
            self.logger.info(f"Saved branch. Total articles saved until now: {self.total_saved_to_db}")
            
        self.current_batch = []
    
        
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
        
        def extract_year_from_title(event_title):
            """
            Extract year from the political events' title (e.g. get 2023 from "2023 in Politics") and attachess it as metadata.
            """
            for year_val in years:
                if str(year_val) in event_title:
                    return{'event_year': year_val}
            return {'event_year': 'unknown'}
        
        return self._handle_all_wikipedia_collection(
            events_to_collect,
            "political_events",
            extra_data_func=extract_year_from_title
        )
    
    def collect_politician_pages(self):
        """Collects specific politician pages from wikipedia."""
        def add_politician_metadata(politician_name):
            return{'politician_name': politician_name}
        
        return self._handle_all_wikipedia_collection(
            self.config['politicians'],
            "politicians_data",
            extra_data_func=add_politician_metadata
        )
        
    def collect_political_topics(self):
        """Collect wiki pages for specific political topics"""
        def add_topic_metadata(topic_name):
            return {'topic_name': topic_name}
        
        return self._handle_all_wikipedia_collection(
            self.config['political_topics'],
            "political topics",
            extra_data_func=add_topic_metadata
        )
        
    def search_political_topics(self, query, language_code, max_results=None):
        """Search wikipedia for political trends in a specific language
        
        Args:
            query (str): Search term to look for
            language_code (str): Language code to search in (e.g. 'en')
            max_results (int): Maximum number of results to return
            
        Returns:
            A list of collected page data from the specified language
        """
        self.logger.info(f"Searching wikipedia for: '{query}' in language: '{language_code}'")
        
        if max_results is None:
            max_results = self.config['api']['search_max_results']
        
        try:
            search_results = self.wiki_utils.search_using_config(query, language_code)
            
            items_by_language = {language_code: search_results[:max_results]}
            
            def add_search_data(result):
                return {'search_query': query}
            
            collected_pages = self._handle_all_wikipedia_collection(
                items_by_language,
                f"search results for '{query}'",
                extra_data_func=add_search_data
            )
            
            return collected_pages
    
        except Exception as e:
            self.logger.error(f"Search error in '{language_code}': {e}")
            return []
    
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
        
        return all_political_data
    
def main():
    collector = WikipediaPoliticalCollector()
    
    collector.logger.info("Wikipedia Political Data Collector")
    collector.logger.info("=" * 30)
        
    all_political_data = collector.wikipedia_political_data_orchestrator()
    
    if all_political_data:
        data_type = collector.data_config['data_output']['default_type_wikipedia']
        
        if collector.save_to_database:
            # Save anything left in the batch
            if collector.current_batch:
                collector._save_batch_if_needed()
            collector.logger.info(f"All data saved incrementally. Total: {collector.total_saved_to_db}")
        else:
            collector.wikipedia_saver.save_locally_at_end(
                collected_data=all_political_data,
                data_type=data_type
            )
        
        collector.wikipedia_saver.log_data_summary(all_political_data)
        
    if not all_political_data:
        collector.logger.warning("No data collected. Unknown reason.")
        
if __name__ == "__main__":
    main()