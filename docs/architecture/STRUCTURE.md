# EpochAI File Structure
This document expplains the file structure of the project

EpochAI/
├── .env                                                    # Hide API keys and any other confidential information
├── .gitignore                                              # Files to exclude from version control
├── .pre-commit-config.yaml                                 # pre-commit's config
├── config.yml                                              # Configuration settings
├── constraints.yml                                         # Constraint configuration settings
├── pyproject.toml                                          # Pre-commit, ruff and mypy
├── README.md                                               # Project overview and documentation
├── requirements.txt                                        # Python dependencies
├── setup.py                                                # Package installation setup
│
├── .github/
│   └── workflows/
│       └── lint.yml                                        # Automated lint and unit tests via github actions
│
├── app/                                                    # Web application
│   ├── __init__.py
│   ├── app.py                                              # Flask application
│   ├── routes.py                                           # API endpoints
│   ├── static/                                             # Static assets
│   │   ├── css/
│   │   │   └── styles.css
│   │   ├── js/
│   │   │   └── scripts.js
│   │   └── images/
│   └── templates/                                          # HTML templates
│       └── index.html
│
├── data/                                                   # Data directory
│   ├── raw/                                                # Raw collected data
│   ├── processed/                                          # Cleaned and processed data
│   ├── features/                                           # Extracted features
│   └── models/                                             # Trained models
│
├── docs/                                                   # Documentation
│   ├── architecture/                                       # Architecture diagrams
│   │   ├── STRUCTURE.md                                    # File structure
│   ├── cheatsheets/                                        # Command cheatsheets of various packages
│   │   ├── ALEMBIC_CHEATSHEET.md
│   │   ├── PRECOMMIT_CHEATSHEET.md
│   ├── diagrams/                                           # Architecture diagrams
│   │   ├── ERD.drawio.png                                  # File structure in png format
│   │   ├── ERD.drawio.xml                                  # File structure in xml to allow for future editing
│   ├── ENV_FILE_TEMPLATE.md
│   └── USER_GUIDE.md
│
├── notebooks/                                              # Jupyter notebooks
│   └── (empty)
│
├── epochai/                                                # Main package directory
│   ├── __init__.py
│   ├── common/                                             # Shared utilities
│   │   ├── __init__.py
│   │   ├── enums.py                                        # Centralized store of all enums
│   │   ├── logging_config.py                               # Creates a centralized logger for the project
│   │   ├── config/                                         # Config loading and validation
│   │   │   ├── __init__.py
│   │   │   ├── config_loader.py                            # Loads config.yml and validates by calling config_validator.py
│   │   │   └── config_validator.py                         # Validates config via pydantic and returns error messages if invalid
│   │   ├── database/                                       # Everything database related
│   │   │   ├── __init__.py
│   │   │   ├── collection_config_manager.py                # Data service layer file for various collection_config_id related functionality
│   │   │   ├── database.py                                 # Sets up database connection and has functions allowing DAOs to access CRUD queries
│   │   │   ├── models.py                                   # Sets up models for the database
│   │   │   ├── dao/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── attempt_statuses_dao.py                 # attempt_statuses_table DAO
│   │   │   │   ├── check_collection_targets_dao.py         # check_collection_targets table DAO
│   │   │   │   ├── cleaned_data_dao.py                     # cleaned_data table DAO
│   │   │   │   ├── cleaned_data_metadata_schemas_dao.py    # cleaned_data_metadata_schemas table DAO
│   │   │   │   ├── collection_attempts_dao.py              # collection_attempts table DAO
│   │   │   │   ├── collection_targets_dao.py               # collection_configs table DAO
│   │   │   │   ├── collection_types_dao.py                 # collection_types table DAO
│   │   │   │   ├── collector_names_dao.py                  # collector_names table DAO
│   │   │   │   ├── error_types_dao.py                      # error_types table DAO
│   │   │   │   ├── link_attempts_to_runs_dao.py            # link_attempts_to_runs table DAO
│   │   │   │   ├── raw_data_dao.py                         # raw_data_dao table DAO
│   │   │   │   ├── raw_data_metadata_schemas_dao.py        # raw_data_metadata_schemas table DAO
│   │   │   │   ├── run_collection_metadata_dao.py          # run_collection_metadata table DAO
│   │   │   │   ├── run_statuses_dao.py                     # run_statuses table DAO
│   │   │   │   ├── run_types_dao.py                        # run_types table DAO
│   │   │   │   └── validation_statuses_dao.py              # validation_statuses table DAO
│   │   │   └── migrations/
│   │   │       ├── __init__.py
│   │   │       ├── versions/
│   │   │       │   └── 001_initial_schema.py               # Initial schema setup using alembic
│   │   │       ├── COMMANDS.md                             # Alembic commands cheat sheet
│   │   │       ├── env.py                                  # Alembic created file, contains custom code to get database credentials from .env
│   │   │       └── script.py.maki                          # Alembic created file on install
│   │   ├── protocols/
│   │   │   ├── __init__.py
│   │   │   └── metadata_schema_dao_protocol.py             # Protocol ensuring schema_utils has consistent access to metadata schema DAOs
│   │   ├── services/                                       # Database service layers
│   │   │   ├── __init__.py
│   │   │   ├── cleaning_service.py
│   │   │   ├── collection_attempts_service.py
│   │   │   ├── collection_reports_service.py
│   │   │   ├── collection_targets_query_service.py
│   │   │   ├── raw_data_service.py
│   │   │   ├── target_status_management_service.py
│   │   └── utils/                                          # Collector Utils
│   │       ├── __init.py__
│   │       ├── data_utils.py                               # Save functionality for collectors
│   │       ├── database_utils.py
│   │       ├── decorators.py
│   │       ├── dynamic_schema_utils.py
│   │       ├── evaluation.py
│   │       └── wikipedia_utils.py
│   ├── data_collection/                                    # Data collection modules
│   │   ├── __init__.py
│   │   ├── checker.py/                                     # Checks if collection targets are valid or not
│   │   ├── collector.py                                    # All collectors orchestrator and CLI
│   │   │   ├── __init__.py
│   │   │   ├── base_collector.py                           # Abstract base collector
│   │   │   └── wikipedia_collector.py                      # Wikipedia collector - inherits base_collector
│   │   └── savers/
│   │       ├── __init__.py
│   │       ├── base_saver.py                               # Abstract base saver
│   │       └── wikipedia_saver.py                          # Wikipedia saver - inherits base_saver
│   ├── data_processing/                                    # Data processing modules
│   │   ├── __init__.py
│   │   ├── cleaner.py                                      # All cleaners orchestrator and CLI
│   │   ├── feature_extractor.py                            # Feature extraction
│   │   ├── data_classifier.py                              # Classifying data types
│   │   └── cleaners/                                       # Collector Utils
│   │       ├── __init__.py
│   │       ├── base_cleaner.py                             # Abstract base cleaner
│   │       └── wikipedia_cleaner.py                        # Wikipedia cleaner - inherits base_cleaner
│   ├── politicsai/                                         # PoliticsAI component
│   │   ├── __init__.py
│   ├── stocksai/                                           # StocksAI component
│   │   ├── __init__.py
│   └── visualization/                                      # Data visualization
│       ├── __init__.py
│
└── tests/                                                  # Unit and integration tests
    ├── conftest.py                                         # Tells Pytest where project filepath is
    ├── integration_tests
    │   ├── currently_empty
    └── unit_tests.py
        ├── test_base_cleaner.py
        ├── test_config_loader.py
        ├── test_config_validator.py
        ├── test_data_utils.py
        ├── test_database.py
        ├── test_wikipedia_cleaner.py
        └── test_wikipedia_utils.py
