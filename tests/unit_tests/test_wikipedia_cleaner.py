# ruff: noqa: SLF001, E501, F841
import re
from unittest.mock import Mock, patch

import pytest

from epochai.common.database.models import RawData
from epochai.data_processing.cleaners.wikipedia_cleaner import WikipediaCleaner


@pytest.fixture
def mock_config():
    """Mock configuration based on config.yml"""
    return {
        "cleaners": {
            "wikipedia": {
                "cleaner_name": "wikipedia_cleaner",
                "current_version": "1.0.0",
                "schema_cache_limit": 3,
                "schema_check_interval": 5,
            },
        },
    }


@pytest.fixture
def sample_wikipedia_metadata():
    """Sample Wikipedia metadata with unicode characters and various content types"""
    return {
        "title": "Donald Trump Jr.\u201d",  # Unicode quote
        "summary": "Donald John Trump Jr. (born December 31, 1977), often nicknamed Don Jr., is an American businessman and political activist. He is the eldest child of U.S. President Donald Trump and his first wife Ivana.[1][citation needed] Trump serves as a trustee and executive vice president of the Trump Organization.",
        "content": "Donald John Trump Jr. (born December 31, 1977), often nicknamed Don Jr., is an American businessman and political activist. He is the eldest child of U.S. President Donald Trump and his first wife Ivana.\nTrump serves as a trustee and executive vice president of the Trump Organization, running the company alongside his younger brother Eric.[2][clarification needed] During their father\u2019s first presidency, the brothers continued to engage in deals and investments in foreign countries. He also served as a boardroom judge on the reality TV show featuring his father, The Apprentice.\n\n\nTrump was active in his father\u2019s 2016 presidential campaign.[3] He had a meeting with a Russian lawyer who promised damaging information about the campaign of Hillary Clinton in the 2016 presidential election. Trump campaigned for several Republicans during the 2018 midterm elections. He has promoted several conspiracy theories.\n\n\nAt the 2024 Republican National Convention, he led the introductions of JD Vance, who had been selected as Donald Trump\u2019s running mate.",
        "categories": [
            "1977 births",
            "American businesspeople",
            "   American businesspeople   ",  # Has extra whitespace
            "Living people",
            "Trump family",
            "Trump family",  # Duplicate
        ],
        "links": [
            "Donald Trump",
            "Ivana Trump",
            "Trump Organization",
            "   Eric Trump   ",  # Has extra whitespace
            "The Apprentice",
            "Hillary Clinton",
            "Hillary Clinton",  # Duplicate
            "JD Vance",
        ],
        "page_id": "5679119",
        "language": "en",
        "url": "https://en.wikipedia.org/wiki/Donald_Trump_Jr.",
        "collected_at": "2023-01-15T10:30:00Z",
        "source": "wikipedia_en",
        "original_search_title": "Donald Trump Jr",
        "word_count": 142,
    }


@pytest.fixture
def sample_raw_data(sample_wikipedia_metadata):
    """Sample RawData instance with Wikipedia metadata"""
    return RawData(
        id=123,
        title="Donald Trump Jr.",
        language_code="en",
        url="https://en.wikipedia.org/wiki/Donald_Trump_Jr.",
        metadata=sample_wikipedia_metadata,
        validation_status_id=1,
    )


@pytest.fixture
def raw_data_minimal_content():
    """RawData with minimal required content"""
    return RawData(
        id=456,
        title="Test Article",
        language_code="en",
        url="https://en.wikipedia.org/wiki/Test",
        metadata={
            "title": "Test Article",
            "content": "This is a test article with minimal content for testing purposes.",
            "page_id": "12345",
            "language": "en",
        },
        validation_status_id=1,
    )


@pytest.fixture
def raw_data_empty_metadata():
    """RawData with no metadata"""
    return RawData(
        id=789,
        title="Empty Article",
        language_code="en",
        metadata={},  # Empty metadata
        validation_status_id=1,
    )


@pytest.fixture
def raw_data_no_metadata():
    """RawData with None metadata"""
    return RawData(
        id=999,
        title="No Metadata Article",
        language_code="en",
        metadata=None,  # None metadata
        validation_status_id=1,
    )


class TestWikipediaCleanerInitialization:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_initialization_success(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        assert cleaner.cleaner_name == "wikipedia_cleaner"
        assert cleaner.cleaner_version == "1.0.0"
        assert cleaner.config == mock_config
        mock_service_class.assert_called_once_with("wikipedia_cleaner", "1.0.0")

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_regex_patterns_compiled(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        # Verify regex patterns are compiled
        assert isinstance(cleaner._multiple_whitespace_pattern, re.Pattern)
        assert isinstance(cleaner._citation_pattern, re.Pattern)
        assert isinstance(cleaner._unicode_quotes_pattern, re.Pattern)
        assert isinstance(cleaner._unicode_dashes_pattern, re.Pattern)
        assert isinstance(cleaner._multiple_newlines_pattern, re.Pattern)


class TestCleanContent:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_content_success(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
        sample_raw_data,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(sample_raw_data)

        # Verify cleaned content structure
        assert "cleaned_content" in result
        assert "cleaned_title" in result
        assert "cleaned_summary" in result
        assert "cleaned_categories" in result
        assert "cleaned_links" in result
        assert "content_word_count" in result
        assert "content_char_count" in result
        assert "summary_word_count" in result
        assert "category_count" in result
        assert "internal_link_count" in result
        assert "cleaned_at" in result
        assert "original_content_length" in result
        assert "cleaning_operations_applied" in result

        # Verify cleaning operations were applied
        expected_operations = [
            "citation_removal",
            "unicode_normalization",
            "whitespace_normalization",
            "newline_normalization",
            "category_deduplication",
            "link_deduplication",
            "content_validation",
        ]
        assert result["cleaning_operations_applied"] == expected_operations

        print(f"Expected: {'Donald Trump Jr."'!r}")
        print(f"Actual:   {result['cleaned_title']!r}")
        assert result["cleaned_title"] == "Donald Trump Jr.\u201d"
        assert result["cleaned_title"].startswith("Donald Trump Jr.")

        assert "[1]" not in result["cleaned_content"]
        assert "[citation needed]" not in result["cleaned_content"]
        assert "[clarification needed]" not in result["cleaned_content"]

        assert result["category_count"] == 4  # 6 original - 2 duplicates = 4
        assert result["internal_link_count"] == 7  # 8 original - 1 duplicate = 7

        assert isinstance(result["content_word_count"], int)
        assert isinstance(result["content_char_count"], int)
        assert isinstance(result["summary_word_count"], int)

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_content_minimal_data(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
        raw_data_minimal_content,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()
        result = cleaner.clean_content(raw_data_minimal_content)

        # Should handle minimal content gracefully
        assert "cleaned_content" in result
        assert "cleaned_title" in result
        assert result["content_word_count"] > 0
        assert result["content_char_count"] > 0

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_content_no_metadata_raises_error(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
        raw_data_no_metadata,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        with pytest.raises(ValueError, match="Raw data \\(999\\) has no metadata to clean"):
            cleaner.clean_content(raw_data_no_metadata)

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_content_empty_metadata_works(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
        raw_data_empty_metadata,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        with pytest.raises(ValueError, match="Raw data \\(789\\) has no metadata to clean"):
            cleaner.clean_content(raw_data_empty_metadata)


class TestTextCleaningMethods:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_citations(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        text = "This is a test[1] with citations[citation needed] and clarifications[clarification needed]."
        result = cleaner._clean_text_content(text)

        assert "[1]" not in result
        assert "[citation needed]" not in result
        assert "[clarification needed]" not in result
        assert result == "This is a test with citations and clarifications."

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_unicode_quotes(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        text = "This has \u201csmart quotes\u201d and \u2018single quotes\u2019."
        result = cleaner._clean_text_content(text)

        # Check that the result matches expected conversion
        expected = "This has \"smart quotes\" and 'single quotes'."
        assert result == expected

        assert "\u201c" in text
        assert "\u201d" in text
        assert "\u2018" in text
        assert "\u2019" in text

        assert "\u201c" not in result
        assert "\u201d" not in result
        assert "\u2018" not in result
        assert "\u2019" not in result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_unicode_dashes(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        text = "This has an en\u2013dash and an em\u2014dash."
        result = cleaner._clean_text_content(text)

        assert "\u2013" not in result
        assert "\u2014" not in result
        assert result == "This has an en-dash and an em-dash."

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_whitespace_normalization(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        text = "This   has    multiple     spaces and\t\ttabs."
        result = cleaner._clean_text_content(text)

        assert result == "This has multiple spaces and tabs."

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_newline_normalization(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        text = "Line 1\n\n\n\nLine 2\n\n\n\n\nLine 3"
        result = cleaner._clean_text_content(text)

        assert result == "Line 1\n\nLine 2\n\nLine 3"

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_text_content_empty_string(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        result = cleaner._clean_text_content("")
        assert result == ""

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_title(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        title = "Donald Trump Jr.[1]   "
        result = cleaner._clean_title(title)

        assert result == "Donald Trump Jr."
        assert "[1]" not in result


class TestCategoryAndLinkCleaning:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_categories_deduplication(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        categories = [
            "American businesspeople",
            "Living people",
            "American businesspeople",  # Duplicate
            "   Trump family   ",  # Whitespace
            "Trump family",  # Duplicate after cleaning
        ]

        result = cleaner._clean_categories(categories)

        assert len(result) == 3
        assert "American businesspeople" in result
        assert "Living people" in result
        assert "Trump family" in result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_categories_non_string_items(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        categories = [
            "Valid category",
            123,  # Non-string
            None,  # Non-string
            {"invalid": "dict"},  # Non-string
            "Another valid category",
        ]

        result = cleaner._clean_categories(categories)

        assert len(result) == 2
        assert "Valid category" in result
        assert "Another valid category" in result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_categories_empty_list(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        result = cleaner._clean_categories([])
        assert result == []

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_categories_none_input(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        result = cleaner._clean_categories(None)
        assert result == []

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_links_deduplication(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        links = [
            "Donald Trump",
            "Ivana Trump",
            "Donald Trump",  # Duplicate
            "   Eric Trump   ",  # Whitespace
            "Eric Trump",  # Duplicate after cleaning
        ]

        result = cleaner._clean_links(links)

        assert len(result) == 3
        assert "Donald Trump" in result
        assert "Ivana Trump" in result
        assert "Eric Trump" in result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_links_non_string_items(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        links = [
            "Valid link",
            456,  # Non-string
            [],  # Non-string
            "Another valid link",
        ]

        result = cleaner._clean_links(links)

        assert len(result) == 2
        assert "Valid link" in result
        assert "Another valid link" in result


class TestUtilityMethods:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_count_words(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        assert cleaner._count_words("Hello world") == 2
        assert cleaner._count_words("One") == 1
        assert cleaner._count_words("") == 0
        assert cleaner._count_words("   ") == 0
        assert cleaner._count_words("Multiple   spaces   between") == 3

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_get_cleaning_operations_list(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        operations = cleaner._get_cleaning_operations_list()
        expected_operations = [
            "citation_removal",
            "unicode_normalization",
            "whitespace_normalization",
            "newline_normalization",
            "category_deduplication",
            "link_deduplication",
            "content_validation",
        ]

        assert operations == expected_operations


class TestCleanWikipediaBatch:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_wikipedia_batch_success(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Mock raw data records
        mock_raw_records = [
            Mock(id=1),
            Mock(id=2),
            Mock(id=3),
        ]
        mock_service.raw_data_dao.get_by_validation_status.return_value = mock_raw_records

        cleaner = WikipediaCleaner()

        with patch.object(
            cleaner,
            "clean_multiple_records",
            return_value={"success_count": 3},
        ) as mock_clean_multiple:
            result = cleaner.clean_wikipedia_batch()

        mock_service.raw_data_dao.get_by_validation_status.assert_called_once_with("valid")
        mock_clean_multiple.assert_called_once_with([1, 2, 3])
        assert result == {"success_count": 3}

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_wikipedia_batch_with_limit(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Mock 5 records but limit to 2
        mock_raw_records = [Mock(id=i) for i in range(1, 6)]
        mock_service.raw_data_dao.get_by_validation_status.return_value = mock_raw_records

        cleaner = WikipediaCleaner()

        with patch.object(
            cleaner,
            "clean_multiple_records",
            return_value={"success_count": 2},
        ) as mock_clean_multiple:
            result = cleaner.clean_wikipedia_batch(limit=2)

        mock_clean_multiple.assert_called_once_with([1, 2])  # Only first 2 IDs
        assert result == {"success_count": 2}

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_clean_wikipedia_batch_no_records(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        mock_service.raw_data_dao.get_by_validation_status.return_value = []

        cleaner = WikipediaCleaner()
        result = cleaner.clean_wikipedia_batch()

        expected_result = {
            "success_count": 0,
            "error_count": 0,
            "cleaned_ids": [],
            "error_ids": [],
        }
        assert result == expected_result


class TestIntegrationScenarios:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_full_cleaning_process_complex_content(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Complex raw data with many edge cases
        complex_metadata = {
            "title": "Test Article[1]   ",
            "summary": "This is a summary[citation needed] with \u201csmart quotes\u201d and em\u2014dashes.",
            "content": "This is content[1][2][clarification needed] with\n\n\n\nmultiple newlines and   extra   spaces. It has \u2018single quotes\u2019 and en\u2013dashes too.",
            "categories": [
                "Category 1",
                "Category 1",  # Duplicate
                123,  # Non-string
                "   Category 2   ",  # Whitespace
                "",  # Empty string
                "Category 2",  # Duplicate after cleaning
            ],
            "links": [
                "Link 1",
                "Link 1",  # Duplicate
                None,  # Non-string
                "   Link 2   ",  # Whitespace
                "Link 2",  # Duplicate after cleaning
            ],
            "page_id": "12345",
            "language": "en",
        }

        raw_data = RawData(
            id=999,
            title="Test Article",
            language_code="en",
            metadata=complex_metadata,
            validation_status_id=1,
        )

        cleaner = WikipediaCleaner()

        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(raw_data)

        # Verify comprehensive cleaning
        assert result["cleaned_title"] == "Test Article"
        assert "[citation needed]" not in result["cleaned_summary"]
        assert "\u201c" not in result["cleaned_summary"]  # Smart quotes normalized
        assert "\u2014" not in result["cleaned_summary"]  # Em dash normalized

        assert "[1]" not in result["cleaned_content"]
        assert "[clarification needed]" not in result["cleaned_content"]
        assert "\n\n\n\n" not in result["cleaned_content"]  # Multiple newlines normalized
        assert "   extra   " not in result["cleaned_content"]  # Multiple spaces normalized

        # Verify deduplication and filtering
        assert len(result["cleaned_categories"]) == 2  # Only "Category 1" and "Category 2"
        assert len(result["cleaned_links"]) == 2  # Only "Link 1" and "Link 2"

        # Verify counts
        assert result["category_count"] == 2
        assert result["internal_link_count"] == 2
        assert result["content_word_count"] > 0
        assert result["summary_word_count"] > 0

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_missing_optional_fields(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Metadata with only required fields
        minimal_metadata = {
            "title": "Minimal Article",
            "content": "This is minimal content.",
            "page_id": "67890",
            "language": "en",
            # Missing: summary, categories, links
        }

        raw_data = RawData(
            id=888,
            title="Minimal Article",
            language_code="en",
            metadata=minimal_metadata,
            validation_status_id=1,
        )

        cleaner = WikipediaCleaner()

        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(raw_data)

        # Should handle missing optional fields gracefully
        assert result["cleaned_title"] == "Minimal Article"
        assert result["cleaned_content"] == "This is minimal content."
        assert "cleaned_summary" not in result
        assert "cleaned_categories" not in result
        assert "cleaned_links" not in result

        # Counts should still be calculated
        assert result["content_word_count"] == 4
        assert result["content_char_count"] == len("This is minimal content.")
        assert result["original_content_length"] == len("This is minimal content.")


class TestErrorHandling:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_malformed_metadata_structure(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Metadata with unexpected structure
        malformed_metadata = {
            "title": {"nested": "title"},  # Should be string
            "content": ["list", "instead", "of", "string"],  # Should be string
            "categories": "string instead of list",  # Should be list
            "links": {"dict": "instead of list"},  # Should be list
            "page_id": "12345",
            "language": "en",
        }

        raw_data = RawData(
            id=777,
            title="Malformed Article",
            language_code="en",
            metadata=malformed_metadata,
            validation_status_id=1,
        )

        cleaner = WikipediaCleaner()

        # Should handle malformed data without crashing
        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(raw_data)

        # Should still return a valid result structure
        assert "cleaning_operations_applied" in result
        assert "cleaned_at" in result
        assert "original_content_length" in result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_extremely_long_content(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Very long content to test performance
        long_content = "This is a very long article. " * 1000  # 30,000 characters
        long_metadata = {
            "title": "Long Article",
            "content": long_content,
            "summary": "This is a long summary. " * 50,  # 1,200 characters
            "categories": [f"Category {i}" for i in range(100)],  # 100 categories
            "links": [f"Link {i}" for i in range(200)],  # 200 links
            "page_id": "99999",
            "language": "en",
        }

        raw_data = RawData(
            id=666,
            title="Long Article",
            language_code="en",
            metadata=long_metadata,
            validation_status_id=1,
        )

        cleaner = WikipediaCleaner()

        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(raw_data)

        # Should handle large content without issues
        assert len(result["cleaned_content"]) > 0
        assert result["content_word_count"] > 0
        assert result["category_count"] == 100
        assert result["internal_link_count"] == 200
        assert result["original_content_length"] == len(long_content)


class TestRegexPatternMatching:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_citation_pattern_comprehensive(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        # Test various citation formats
        test_cases = [
            ("Text[1]", "Text"),
            ("Text[123]", "Text"),
            ("Text[citation needed]", "Text"),
            ("Text[clarification needed]", "Text"),
            ("Text[1][2][3]", "Text"),
            ("Text[citation needed][clarification needed]", "Text"),
            ("Text [1] more text [2]", "Text  more text "),
        ]

        for input_text, expected in test_cases:
            result = cleaner._clean_text_content(input_text)
            # Remove extra spaces that might be left after citation removal
            result = " ".join(result.split())
            expected = " ".join(expected.split())
            assert result == expected, f"Failed for input: {input_text}"

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_unicode_normalization_comprehensive(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        # Test various unicode characters
        test_cases = [
            # Smart quotes
            ("\u201cquoted text\u201d", '"quoted text"'),
            ("\u2018single quoted\u2019", "'single quoted'"),
            # Dashes
            ("en\u2013dash", "en-dash"),
            ("em\u2014dash", "em-dash"),
            # Mixed
            ("\u201cHe said\u2014\u2018hello\u2019\u201d", "\"He said-'hello'\""),
        ]

        for input_text, expected in test_cases:
            result = cleaner._clean_text_content(input_text)
            assert result == expected, f"Failed for input: {input_text}"

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_whitespace_normalization_edge_cases(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        # Test various whitespace scenarios
        test_cases = [
            ("word1   word2", "word1 word2"),
            ("word1\t\tword2", "word1 word2"),
            ("word1\n word2", "word1\n word2"),
            ("word1 \t\n  word2", "word1 \n word2"),
            ("   leading and trailing   ", "leading and trailing"),
        ]

        for input_text, expected in test_cases:
            result = cleaner._clean_text_content(input_text)
            assert result == expected, f"Failed for input: {input_text!r}"


class TestConfigurationIntegration:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_config_values_used_correctly(self, mock_service_class, mock_config_loader, mock_config):
        modified_config = {
            "cleaners": {
                "wikipedia": {
                    "cleaner_name": "custom_wikipedia_cleaner",
                    "current_version": "2.5.0",
                    "schema_cache_limit": 10,
                    "schema_check_interval": 15,
                },
            },
        }

        mock_config_loader.return_value = modified_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        # Verify the custom config values are used
        assert cleaner.cleaner_name == "custom_wikipedia_cleaner"
        assert cleaner.cleaner_version == "2.5.0"
        assert cleaner.config == modified_config

        # Verify CleaningService was initialized with correct values
        mock_service_class.assert_called_once_with("custom_wikipedia_cleaner", "2.5.0")

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_missing_config_section_raises_error(self, mock_service_class, mock_config_loader):
        # Config missing wikipedia section
        incomplete_config = {
            "cleaners": {
                "other_cleaner": {
                    "cleaner_name": "other",
                    "current_version": "1.0.0",
                },
            },
        }

        mock_config_loader.return_value = incomplete_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Should raise error when trying to access missing config
        with pytest.raises((KeyError, AttributeError)):
            WikipediaCleaner()


class TestBatchOperationsIntegration:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_batch_cleaning_with_mixed_record_types(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        # Mock records with different characteristics
        mock_records = [
            Mock(id=1),  # Normal record
            Mock(id=2),  # Record that might fail
            Mock(id=3),  # Another normal record
            Mock(id=None),  # Record with no ID (should be filtered)
        ]
        mock_service.raw_data_dao.get_by_validation_status.return_value = mock_records

        cleaner = WikipediaCleaner()

        expected_batch_result = {
            "success_count": 2,
            "error_count": 1,
            "cleaned_ids": [101, 103],
            "error_ids": [2],
        }

        with patch.object(
            cleaner,
            "clean_multiple_records",
            return_value=expected_batch_result,
        ) as mock_clean_multiple:
            result = cleaner.clean_wikipedia_batch()

        # Should filter out None IDs
        mock_clean_multiple.assert_called_once_with([1, 2, 3])
        assert result == expected_batch_result

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_batch_cleaning_zero_limit(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        mock_records = [Mock(id=i) for i in range(1, 6)]
        mock_service.raw_data_dao.get_by_validation_status.return_value = mock_records

        cleaner = WikipediaCleaner()

        with patch.object(cleaner, "clean_multiple_records") as mock_clean_multiple:
            result = cleaner.clean_wikipedia_batch(limit=0)

        mock_clean_multiple.assert_not_called()

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_batch_cleaning_negative_limit(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        mock_records = [Mock(id=i) for i in range(1, 6)]
        mock_service.raw_data_dao.get_by_validation_status.return_value = mock_records

        cleaner = WikipediaCleaner()

        with patch.object(cleaner, "clean_multiple_records") as mock_clean_multiple:
            result = cleaner.clean_wikipedia_batch(limit=-1)

        # Negative limit should be treated same as no limit - process all
        mock_clean_multiple.assert_called_once_with([1, 2, 3, 4, 5])


class TestDataIntegrityValidation:
    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_cleaned_data_preserves_original_metadata(
        self,
        mock_service_class,
        mock_config_loader,
        mock_config,
        sample_raw_data,
    ):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        cleaner = WikipediaCleaner()

        with patch("epochai.data_processing.cleaners.wikipedia_cleaner.datetime") as mock_datetime:
            mock_datetime.now.return_value.isoformat.return_value = "2023-01-15T12:00:00Z"
            result = cleaner.clean_content(sample_raw_data)

        # Should preserve original metadata fields
        original_metadata = sample_raw_data.metadata
        for key in original_metadata:
            assert key in result, f"Original field {key} should be preserved"
            assert result[key] == original_metadata[key], f"Original value for {key} should be unchanged"

        assert "cleaned_title" in result
        assert "cleaned_content" in result
        assert "content_word_count" in result

        assert result["title"] == original_metadata["title"]  # Original should be preserved
        assert result["cleaned_title"] == cleaner._clean_title(
            original_metadata["title"],
        )  # Cleaned should be processed

    @patch("epochai.data_processing.cleaners.wikipedia_cleaner.ConfigLoader.get_data_config")
    @patch("epochai.data_processing.cleaners.base_cleaner.CleaningService")
    def test_character_counts_accuracy(self, mock_service_class, mock_config_loader, mock_config):
        mock_config_loader.return_value = mock_config
        mock_service = Mock()
        mock_service_class.return_value = mock_service

        test_metadata = {
            "title": "Test Article",
            "content": "This is exactly twenty-five characters long.",  # 44 chars
            "summary": "Short summary.",  # 14 chars
            "page_id": "12345",
            "language": "en",
        }

        raw_data = RawData(
            id=555,
            title="Test Article",
            language_code="en",
            metadata=test_metadata,
            validation_status_id=1,
        )

        cleaner = WikipediaCleaner()
        result = cleaner.clean_content(raw_data)

        # Verify accurate character counting
        assert result["content_char_count"] == len(result["cleaned_content"])
        assert result["original_content_length"] == len(test_metadata["content"])

        # Verify word counting
        expected_words = len(result["cleaned_content"].split())
        assert result["content_word_count"] == expected_words

        expected_summary_words = len(result["cleaned_summary"].split())
        assert result["summary_word_count"] == expected_summary_words
