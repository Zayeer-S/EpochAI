from unittest.mock import Mock, patch

import pytest
import wikipedia

from epochai.common.utils.wikipedia_utils import WikipediaUtils


@pytest.fixture
def mock_config():
    """Based off config.yml"""
    return {
        "api": {
            "rate_limit_delay": 0.1,
            "max_retries": 3,
            "search_max_results": 5,
            "recursive_limit": 2,
        },
    }


@pytest.fixture
def wiki_utils(mock_config):
    """Create WikipediaUtils instance with mocked dependencies"""
    return WikipediaUtils(mock_config)


class TestWikipediaUtilsInitialization:
    def test_initialization_with_valid_config(self, mock_config):
        utils = WikipediaUtils(mock_config)
        assert utils.config == mock_config
        assert utils.current_language is None


class TestSwitchLanguage:
    @patch("epochai.common.utils.wikipedia_utils.wikipedia.set_lang")
    def test_switch_language_success(self, mock_set_lang, wiki_utils):
        # Test successful language switch
        result = wiki_utils.switch_language("fr")

        assert result is True
        assert wiki_utils.current_language == "fr"
        mock_set_lang.assert_called_once_with("fr")

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.set_lang")
    def test_switch_language_same_language_no_api_call(self, mock_set_lang, wiki_utils):
        # Set initial language
        wiki_utils.current_language = "en"

        result = wiki_utils.switch_language("en")

        assert result is True
        assert wiki_utils.current_language == "en"
        mock_set_lang.assert_not_called()

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.set_lang")
    def test_switch_language_failure(self, mock_set_lang, wiki_utils):
        mock_set_lang.side_effect = Exception("Language not supported")

        result = wiki_utils.switch_language("invalid")

        assert result is False
        assert wiki_utils.current_language is None


class TestSearchUsingConfig:
    @patch("epochai.common.utils.wikipedia_utils.wikipedia.search")
    def test_search_success(self, mock_search, wiki_utils):
        mock_search.return_value = ["Page 1", "Page 2", "Page 3"]

        with patch.object(wiki_utils, "switch_language", return_value=True):
            results = wiki_utils.search_using_config("test query", "en")

        assert results == ["Page 1", "Page 2", "Page 3"]
        mock_search.assert_called_once_with("test query", results=5)

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.search")
    def test_search_no_results(self, mock_search, wiki_utils):
        mock_search.return_value = []

        with patch.object(wiki_utils, "switch_language", return_value=True):
            results = wiki_utils.search_using_config("nonexistent", "en")

        assert results == []

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.search")
    def test_search_language_switch_failure(self, mock_search, wiki_utils):
        with patch.object(wiki_utils, "switch_language", return_value=False):
            results = wiki_utils.search_using_config("test", "invalid")

        assert results == []
        mock_search.assert_not_called()

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.search")
    def test_search_exception(self, mock_search, wiki_utils):
        mock_search.side_effect = Exception("API Error")

        with patch.object(wiki_utils, "switch_language", return_value=True):
            results = wiki_utils.search_using_config("test", "en")

        assert results == []


class TestGetWikipediaPage:
    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_get_page_success(self, mock_page, wiki_utils):
        mock_wiki_page = Mock()
        mock_wiki_page.title = "Test Page"
        mock_page.return_value = mock_wiki_page

        with patch.object(wiki_utils, "switch_language", return_value=True):
            result = wiki_utils.get_wikipedia_page("Test Page", "en")

        assert result == mock_wiki_page
        mock_page.assert_called_once_with("Test Page")

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_get_page_language_switch_failure(self, mock_page, wiki_utils):
        with patch.object(wiki_utils, "switch_language", return_value=False):
            result = wiki_utils.get_wikipedia_page("Test Page", "invalid")

        assert result is None
        mock_page.assert_not_called()

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_get_page_disambiguation_error(self, mock_page, wiki_utils):
        disambiguation_error = wikipedia.exceptions.DisambiguationError(
            "Test Page",
            ["Option 1", "Option 2"],
        )
        mock_page.side_effect = disambiguation_error

        with patch.object(wiki_utils, "switch_language", return_value=True), patch.object(
            wiki_utils,
            "handle_any_disambiguation_error",
            return_value=Mock(),
        ) as mock_handle:
            result = wiki_utils.get_wikipedia_page("Test Page", "en")

            mock_handle.assert_called_once_with("Test Page", ["Option 1", "Option 2"], "en")
            assert result is not None

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_get_page_not_found_triggers_search_fallback(self, mock_page, wiki_utils):
        mock_page.side_effect = wikipedia.exceptions.PageError("Page not found")

        with patch.object(wiki_utils, "switch_language", return_value=True), patch.object(
            wiki_utils,
            "_try_search_results_fallback",
            return_value=Mock(),
        ) as mock_fallback:
            result = wiki_utils.get_wikipedia_page("Nonexistent Page", "en")

            mock_fallback.assert_called_once_with("Nonexistent Page", "en")
            assert result is not None


class TestGetWikipediaMetadata:
    def test_get_metadata_success(self, wiki_utils):
        # Create a mock page with all necessary attributes
        mock_page = Mock()
        mock_page.title = "Test Page"
        mock_page.summary = "Test summary"
        mock_page.content = "Test content"
        mock_page.url = "https://en.wikipedia.org/wiki/Test_Page"
        mock_page.categories = ["Category 1", "Category 2"]
        mock_page.links = ["Link 1", "Link 2"]
        mock_page.pageid = 12345

        with patch.object(wiki_utils, "get_wikipedia_page", return_value=mock_page), patch(
            "epochai.common.utils.wikipedia_utils.datetime",
        ) as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

            result = wiki_utils.get_wikipedia_metadata("Test Page", "en")

        expected_data = {
            "title": "Test Page",
            "summary": "Test summary",
            "content": "Test content",
            "url": "https://en.wikipedia.org/wiki/Test_Page",
            "categories": ["Category 1", "Category 2"],
            "links": ["Link 1", "Link 2"],
            "collected_at": "2023-01-01T12:00:00",
            "source": "wikipedia_en",
            "language": "en",
            "page_id": 12345,
            "original_search_title": "Test Page",
        }

        assert result == expected_data

    def test_get_metadata_with_extra_data(self, wiki_utils):
        mock_page = Mock()
        mock_page.title = "Test Page"
        mock_page.summary = "Test summary"
        mock_page.content = "Test content"
        mock_page.url = "https://en.wikipedia.org/wiki/Test_Page"
        mock_page.categories = []
        mock_page.links = []
        mock_page.pageid = 12345

        extra_data = {"politician_name": "John Doe", "custom_field": "custom_value"}

        with patch.object(wiki_utils, "get_wikipedia_page", return_value=mock_page), patch(
            "epochai.common.utils.wikipedia_utils.datetime",
        ) as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

            result = wiki_utils.get_wikipedia_metadata("Test Page", "en", extra_data)

        assert result["politician_name"] == "John Doe"
        assert result["custom_field"] == "custom_value"
        assert result["title"] == "Test Page"

    def test_get_metadata_page_not_found(self, wiki_utils):
        with patch.object(wiki_utils, "get_wikipedia_page", return_value=None):
            result = wiki_utils.get_wikipedia_metadata("Nonexistent Page", "en")

        assert result is None

    @patch("epochai.common.utils.wikipedia_utils.time.sleep")
    def test_get_metadata_with_retries(self, mock_sleep, wiki_utils):
        mock_page = Mock()
        mock_page.title = "Test Page"
        mock_page.summary = "Test summary"
        mock_page.content = "Test content"
        mock_page.url = "https://en.wikipedia.org/wiki/Test_Page"
        mock_page.categories = []
        mock_page.links = []
        mock_page.pageid = 12345

        # First two calls raise exceptions, third succeeds
        with patch.object(
            wiki_utils,
            "get_wikipedia_page",
            side_effect=[Exception("Error 1"), Exception("Error 2"), mock_page],
        ), patch("epochai.common.utils.wikipedia_utils.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-01T12:00:00"

            result = wiki_utils.get_wikipedia_metadata("Test Page", "en")

        assert result is not None
        assert result["title"] == "Test Page"
        # Should sleep twice (after first two failures)
        assert mock_sleep.call_count == 2


class TestProcessItemsByLanguage:
    def test_process_items_success(self, wiki_utils):
        items_by_language = {
            "en": ["Item 1", "Item 2"],
            "es": ["Item 3"],
        }

        def mock_process_func(item, language_code):
            return f"processed_{item}_{language_code}"

        with patch.object(wiki_utils, "switch_language", return_value=True), patch(
            "epochai.common.utils.wikipedia_utils.time.sleep",
        ):
            results = wiki_utils.process_items_by_language(items_by_language, mock_process_func)

        expected = {
            "en": ["processed_Item 1_en", "processed_Item 2_en"],
            "es": ["processed_Item 3_es"],
        }

        assert results == expected

    def test_process_items_empty_language(self, wiki_utils):
        items_by_language = {
            "en": ["Item 1"],
            "es": [],  # Empty list
        }

        def mock_process_func(item, language_code):
            return f"processed_{item}_{language_code}"

        with patch.object(wiki_utils, "switch_language", return_value=True), patch(
            "epochai.common.utils.wikipedia_utils.time.sleep",
        ):
            results = wiki_utils.process_items_by_language(items_by_language, mock_process_func)

        assert results == {"en": ["processed_Item 1_en"]}
        # 'es' key should not exist since there were no items

    def test_process_items_language_switch_failure(self, wiki_utils):
        items_by_language = {"invalid": ["Item 1"]}

        def mock_process_func(item, language_code):
            return f"processed_{item}_{language_code}"

        with patch.object(wiki_utils, "switch_language", return_value=False):
            results = wiki_utils.process_items_by_language(items_by_language, mock_process_func)

        assert results == {"invalid": []}

    def test_process_items_with_none_results(self, wiki_utils):
        items_by_language = {"en": ["Item 1", "Item 2"]}

        def mock_process_func(item, language_code):
            # Return None for Item 1, success for Item 2
            return f"processed_{item}_{language_code}" if item == "Item 2" else None

        with patch.object(wiki_utils, "switch_language", return_value=True), patch(
            "epochai.common.utils.wikipedia_utils.time.sleep",
        ):
            results = wiki_utils.process_items_by_language(items_by_language, mock_process_func)

        # Only successful results should be included
        assert results == {"en": ["processed_Item 2_en"]}


class TestHandleDisambiguationError:
    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_disambiguation_resolved_first_option(self, mock_page, wiki_utils):
        mock_wiki_page = Mock()
        mock_wiki_page.title = "Resolved Page"
        mock_page.return_value = mock_wiki_page

        options = ["Option 1", "Option 2", "Option 3"]

        result = wiki_utils.handle_any_disambiguation_error("Test Page", options, "en")

        assert result == mock_wiki_page
        mock_page.assert_called_once_with("Option 1")

    @patch("epochai.common.utils.wikipedia_utils.wikipedia.page")
    def test_disambiguation_all_options_fail(self, mock_page, wiki_utils):
        mock_page.side_effect = wikipedia.exceptions.PageError("Not found")

        options = ["Bad Option 1", "Bad Option 2"]

        result = wiki_utils.handle_any_disambiguation_error("Test Page", options, "en")

        assert result is None
        assert mock_page.call_count == 2  # Should try max_retries from config

    def test_disambiguation_recursive_limit_reached(self, wiki_utils):
        options = ["Option 1"]

        result = wiki_utils.handle_any_disambiguation_error("Test Page", options, "en", recursive_limit=0)

        assert result is None
