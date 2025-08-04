import time 
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional

import wikipedia

from epochai.common.logging_config import get_logger

class WikipediaUtils:
    """Shared Wikipedia API Utils across collection and debug files"""
    
    def __init__(
        self,
        config
        ):
        
        self.config = config
        self.logger = get_logger(__name__)
        
        self.current_language = None
        
    def _try_search_results_fallback(
        self,
        page_title: str,
        language_code: str,
    ) -> Optional[wikipedia.WikipediaPage]:
        """
        Searches pages on wikipedia and attempts to retrieve them when direct page lookup fails.7
        
        Returns:
            Successfully retrieved page from search or None
        """

        self.logger.info(f"Attempting search fallback for '{page_title}' in language '{language_code}'")
        
        search_results = self.search_using_config(page_title, language_code)
        if not search_results:
            self.logger.warning(f"No search results found for '{page_title}'")
            return None
        
        max_results = self.config['api']['search_max_results']
        
        attempted_search_results = set()
        
        for search_result in search_results[:max_results]:
            if search_result in attempted_search_results:
                self.logger.debug(f"Skipping already attempted result: {search_result}")
                continue
            
            attempted_search_results.add(search_result)
            
            try:
                self.logger.info(f"Trying search result: '{search_result}'")
                retrieved_page = wikipedia.page(search_result)
                self.logger.info(f"Successfully retreived '{page_title}' via search result '{search_result}', retrieved page: {retrieved_page}")
                return retrieved_page
            
            except wikipedia.exceptions.DisambiguationError as disambiguation_error:
                self.logger.warning(f"Search result '{search_result}' has disambiguation, calling {self.handle_any_disambiguation_error.__name__}")
                disambiguation_resolved_page = self.handle_any_disambiguation_error(
                    search_result,  
                    disambiguation_error.options,
                    language_code
                )
                if disambiguation_resolved_page:
                    return disambiguation_resolved_page
                
            except wikipedia.exceptions.PageError as page_error:
                self.logger.warning(f"Search result '{search_result}' not found: {page_error}")
                continue
                
            except Exception as general_error:
                self.logger.error(f"Error getting '{search_result}': {general_error}")
                continue
            
        self.logger.warning(f"Couldn't retrieve any page from search results for '{page_title}'")
        return None
        
    def search_using_config(
        self,
        query: str,
        language_code: str,
    ) -> List[str]:
        """
        Searches Wikipedia using config settings
        
        Returns:
            List of pages title from search results
        """
        
        search_max_results = self.config['api']['search_max_results']
        
        self.switch_language(language_code)
            
        self.logger.info(f"Searching for '{query}' in language '{language_code}' (max results: {search_max_results})")
        
        try:
            search_results = wikipedia.search(query, results=search_max_results)
            
            if search_results:
                self.logger.info(f"Found {len(search_results)} search results for '{query}': {search_results}")
                return search_results
            else:
                self.logger.warning(f"No results found for '{query}' in '{language_code}'")
                return []
            
        except Exception as general_error:
            self.logger.error(f"Search error for '{query}' in '{language_code}': {general_error}")
            return []
            
    def switch_language(
        self,
        language_code: str
        ) -> bool:
        """
        Switches language for Wikipedia API. True if successful switch and vice versa.
        """
        try: 
            if self.current_language == language_code:
                return True
            else:
                wikipedia.set_lang(language_code)
                self.logger.info(f"Language successfully switched to '{language_code}'")
                self.current_language = language_code
                
                return True
        
        except Exception as general_error:
            self.logger.error(f"Error switching to language: {language_code} - {general_error}")
            return False
        
    def process_items_by_language(
        self, 
        items_by_language_code: Dict[str, List[str]], 
        process_func: Callable
        ) -> Dict[str, List[Any]]:
        """
        Processes items by language and switches language via helper function.
        Uses process_func to call a relevant function.
        
        Args:
            process_func: Function that is called for each item. Must accept 2 parameters:
                (item: str, language_code: str) and returns result or None
        
        Returns:
            Dict mapping language codes to a nullable list 
            e.g.'language_code': [item1, item2, etc]
                'language_code': [null]
        """
        
        results_by_language = {}
        
        for language_code, items in items_by_language_code.items():
            if not items:
                self.logger.warning(f"No items for this language '{language_code}', skipping this language...")
                continue
            
            if not self.switch_language(language_code):
                results_by_language[language_code] = []
                continue
            
            results_by_language[language_code] = []
            
            for item in items:
                try:
                    result = process_func(item, language_code)
                    if result:
                        results_by_language[language_code].append(result)
                        
                    time.sleep(self.config['api']['rate_limit_delay'])
                    
                except Exception as general_error:
                    self.logger.error(f"Error processing '{item} in '{language_code}': {general_error}")
                    continue
                
        return results_by_language
                
    def handle_any_disambiguation_error(
        self,
        page_title: str,
        options: List[str],
        language_code: str,
        recursive_limit: int = None
        ) -> Optional[wikipedia.WikipediaPage]:
        """
        Handles disambiguation errors by trying different options
        """
        max_retries = self.config['api']['max_retries']
        search_max_results = self.config['api']['search_max_results']
        
        if recursive_limit == None:
            recursive_limit = self.config['api']['recursive_limit']
        elif recursive_limit <= 0:
            self.logger.warning(f"Recursive limit reached for '{page_title}' in function {self.handle_any_disambiguation_error.__name__}")
            return None
        
        self.logger.warning(f"Disambiguation for page title '{page_title}' in '{language_code}'. Options: {options[:{search_max_results}]}")
        
        for option in (options[:max_retries]):
            try:
                self.logger.info(f"Trying option: '{option}'")
                page = wikipedia.page(option)
                self.logger.info(f"Successfully resolved to: '{page.title}'")
                return page
            
            except wikipedia.exceptions.DisambiguationError as disambiguation_error:
                self.logger.warning(f"Option '{option}' also has disambiguation: {disambiguation_error} - calling {self.handle_any_disambiguation_error.__name__} recursively (remaining attempts: {recursive_limit - 1})")
                recursive_call_result = self.handle_any_disambiguation_error(
                    option,
                    disambiguation_error.options,
                    language_code,
                    recursive_limit-1
                )
                if recursive_call_result:
                    return recursive_call_result
                continue
            
            except wikipedia.exceptions.PageError as e2:
                self.logger.warning(f"Option '{option}' page not found: {e2}")
                continue
                
            except Exception as e3:
                self.logger.error(f"Error with option '{option}': {e3}")
                continue
        
        self.logger.error(f"Could not resolve disambiguation error for '{page_title}'")
        return None

    def get_wikipedia_page(
        self,
        page_title: str,
        language_code: str
    )-> Optional[wikipedia.WikipediaPage]:
        """
        Gets a wikipedia page and handles any disambiguation errors via helper method.

        Args:
            page_title (str): Title of the page to retrieve

        Returns:
            Optional[wikipedia.WikipediaPage]: Wikipedia page object if successcful or none if fail
        """
        
        if not self.switch_language(language_code):
            self.logger.error(f"Cannot get '{page_title}' due to failure to change to '{language_code}'")
            return None
        
        try:
            page = wikipedia.page(page_title)
            self.logger.debug(f"Successfully collected page: '{page_title}'")
            return page
        
        except wikipedia.exceptions.DisambiguationError as disambiguation_error:
            self.logger.warning(f"Disambiguation error for '{page_title}', trying to resolve")
            return self.handle_any_disambiguation_error(page_title, disambiguation_error.options, language_code)
        
        except wikipedia.exceptions.PageError as page_error:
            self.logger.warning(f"Page not found: '{page_title}' in '{language_code}' - {page_error}")
            return self._try_search_results_fallback(page_title, language_code)
        
        except Exception as general_error:
            self.logger.error(f"Error getting page '{page_title}' in '{language_code}': {general_error}")
            return None
        
    def get_wikipedia_metadata(
        self,
        page_title: str,
        language_code: str,
        extra_data: Optional[Dict[str, Any]] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Attempts to get meta data via helper funciton and returns that meta data

        Args:
            page_title (str): Title of wiki page
            extra_data_func (Optional[Dict[str, Any]]): Additional data to include in the result

        Returns:
            Optional[Dict[str, Any]]: Page meta data or "None" if result is null
        """
        max_retries = self.config['api']['max_retries']
        rate_limit_delay = self.config['api']['rate_limit_delay']
        
        for attempt in range(max_retries):
            try:
                page = self.get_wikipedia_page(page_title, language_code)
                
                if page is None:
                    self.logger.warning(f"Could not retrieve '{page_title}' in language '{language_code}'")
                    return None
                
                page_data = {
                    'title': page.title,
                    'summary': page.summary,
                    'content': page.content,
                    'url': page.url,
                    'categories': list(page.categories) if hasattr(page, 'categories') else [],
                    'links': list(page.links) if hasattr(page, 'links') else [],
                    'collected_at': datetime.now().isoformat(),
                    'source': f'wikipedia_{language_code}',
                    'language': language_code,
                    'page_id': getattr(page, 'pageid', None),
                    'original_search_title': page_title
                }
                
                if extra_data:
                    page_data.update(extra_data)
                    
                return page_data
                
            except Exception as general_error:
                if attempt < max_retries - 1:
                    self.logger.debug(f"Attempt {attempt + 1} failed for '{page_title}': {general_error}")
                    self.logger.debug("Retrying...")
                    time.sleep(rate_limit_delay * (attempt + 1))
                else:
                    self.logger.debug(f"Final attempt failed: {general_error}")
                    return None
        return None