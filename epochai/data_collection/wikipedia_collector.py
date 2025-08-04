from typing import Optional, List, Dict, Any, Callable

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
        self.current_collection_type = None
        self.current_collection_name = None
        
    def _handle_all_wikipedia_collection(
        self,
        items_by_language: Dict[str, List[str]],
        collection_type: str,
        extra_data_func: Optional[Callable[[str], Dict[str, Any]]]
        ) -> List[Dict[str, Any]]:
        """
        Handles all wikipedia collection including batch saving (if saving to database)
        """
        
        if not items_by_language:
            self.logger.warning(f"No items provided for {collection_type}")
            return []
        
        self.logger.info(f"Collecting {collection_type} for {len(items_by_language)} items across {len(self.languages)} languages")
        
        self.current_collection_type = collection_type
        
        def collect_single_page_data(
            item: str,
            language_code: str
        ) -> Optional[Dict[str, Any]]:  
            """
            Gets the metadata for only one page and saves it if incrementally saving
            """
            self.logger.info(f"Collecting ({language_code}): {item}")
            
            self.current_collection_name = item
            self.current_language_code = language_code
            
            if extra_data_func:
                extra_data = extra_data_func(item)
            else:
                extra_data = None
            
            page = self.wiki_utils.get_wikipedia_metadata(item, language_code, extra_data)
            
            if page:
                self.logger.debug(f"Successfully collected ({language_code}): {item}")
                
                if self.save_to_database:
                    collection_config_id = self.wikipedia_saver.get_collection_config_id(
                        collection_type, language_code, item
                    )
                    if collection_config_id:
                        self._add_to_batch(page, collection_config_id)
                    else:
                        self.logger.error(f"Skipping save for '{item}' as no collection_config_id found")
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
                
        if self.save_to_database:
            self._save_batch_between_topic_switch()
                
        return all_collected_data
    
    def _add_to_batch(
        self,
        item_data: Dict[str, Any],
        collection_config_id: int
    ) -> None:
        """Adds a single item with its collection_config_id to the batch"""
        if not self.save_to_database:
            self.collected_data.append(item_data)
            return
        
        self.current_batch.append((item_data, collection_config_id))
        
        if len(self.current_batch) >= self.batch_size:
            self._save_current_batch()
                
    def _save_current_batch(self) -> None:
        """Saves current batch using DataUtils and resets batch"""
        if not self.current_batch:
            return
        
        items_by_config_id = {}
        for item_data, collection_config_id in self.current_batch:
            if collection_config_id not in items_by_config_id:
                items_by_config_id[collection_config_id] = []
            items_by_config_id[collection_config_id].append(item_data)
            
        total_saved_in_batch = 0
        for collection_config_id, items in items_by_config_id.items():
            success_count = self.wikipedia_saver.save_incrementally_to_database(
                collected_data=items,
                collection_config_id=collection_config_id,
                language_code=self.current_language_code
            )
            
            if success_count >= 0:
                total_saved_in_batch += len(items)
                self.logger.debug(f"Saved item with collection_config_id: {collection_config_id}")
                
        self.total_saved_to_db += total_saved_in_batch
        self.logger.info(f"Saved batch of {total_saved_in_batch} items")
        
        self.current_batch = []
        
    def _save_batch_between_topic_switch(self) -> None:
        if not self.save_to_database or not self.current_batch:
            return
        
        self.logger.info(f"Saving last items of this batch due to topic change or reaching the end")
        self._save_current_batch()
        
    def _collect_collection_config(
        self,
        collection_config_name: str,
        metadata_key_name: str
        ) -> List[Dict[str, Any]]:
        """Collects collection config pages from Wikipedia"""
        def add_metadata(metadata_value_name):
            return{f'{metadata_key_name}': metadata_value_name}
        
        return self._handle_all_wikipedia_collection(
            self.config[f'{collection_config_name}'],
            f"{collection_config_name}",
            extra_data_func=add_metadata
        )
        
    """def _____unused_for_now_____search_political_topics(self, query, language_code, max_results=None):
        "Search wikipedia for political trends in a specific language
        
        Args:
            query (str): Search term to look for
            language_code (str): Language code to search in (e.g. 'en')
            max_results (int): Maximum number of results to return
            
        Returns:
            A list of collected page data from the specified language
        "
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
            return []"""
    
    def wikipedia_political_data_orchestrator(self) -> List[Dict[str, Any]]:
        """Collect political data from wikipedia. It's the site of orchestrating the collection flow and returns the data"""
        
        all_political_data =  []
        
        self.logger.info("=== Starting comprehensive political data collection ===")
        
        self.logger.info("1. Collecting politician data...")
        politician_wiki_data = self._collect_collection_config("politicians", "politician_name")
        
        self.logger.info("2. Collecting important persons data...")
        important_persons_data = self._collect_collection_config("important_persons", "important_person_name")
        
        self.logger.info("3. Collecting political topic data...")
        topic_wiki_data = self._collect_collection_config("political_topics", "political_topic_name")
        
        all_political_data.extend(politician_wiki_data)
        all_political_data.extend(important_persons_data)
        all_political_data.extend(topic_wiki_data)
        
        if self.save_to_database:
            self._save_batch_between_topic_switch()
        
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