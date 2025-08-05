# EpochAI File Structure
This document expplains the file structure of the project

EpochAI/
├── .env                                                # Hide API keys and any other confidential information
├── .gitignore                                          # Files to exclude from version control
├── config.yml                                          # Configuration settings
├── constraints.yml                                     # Constraint configuration settings
├── README.md                                           # Project overview and documentation
├── requirements.txt                                    # Python dependencies
├── setup.py                                            # Package installation setup
│
├── app/                                                # Web application
│   ├── __init__.py
│   ├── app.py                                          # Flask application
│   ├── routes.py                                       # API endpoints
│   ├── static/                                         # Static assets
│   │   ├── css/
│   │   │   └── styles.css
│   │   ├── js/
│   │   │   └── scripts.js
│   │   └── images/
│   └── templates/                                      # HTML templates
│       └── index.html
│
├── data/                                               # Data directory
│   ├── raw/                                            # Raw collected data
│   ├── processed/                                      # Cleaned and processed data
│   ├── features/                                       # Extracted features
│   └── models/                                         # Trained models
│
├── docs/                                               # Documentation
│   ├── architecture/                                   # Architecture diagrams
│   │   ├── STRUCTURE.md                                # File structure
│   ├── diagrams/                                       # Architecture diagrams
│   │   ├── ERD.drawio.png                              # File structure in png format
│   │   ├── ERD.drawio.xml                              # File structure in xml to allow for future editing
│   └── USER_GUIDE.md
│
├── notebooks/                                          # Jupyter notebooks
│   ├── data_exploration.ipynb
│   ├── model_evaluation.ipynb
│   └── visualization.ipynb
│
├── epochai/                                            # Main package directory
│   ├── __init__.py
│   ├── common/                                         # Shared utilities
│   │   ├── __init__.py
│   │   ├── logging_config.py                       # Creates a centralized logger for the project
│   │   ├── config/                                     # Config loading and validation
│   │   │   ├── __init__.py
│   │   │   ├── config_loader.py                        # Loads config.yml and validates by calling config_validator.py
│   │   │   └── config_validator.py                     # Validates config and returns error messages if invalid
│   │   ├── database/                                   # Everything database related
│   │   │   ├── __init__.py
│   │   │   ├── collection_config_manager.py            # Data service layer file for various collection_config_id related functionality
│   │   │   ├── database.py                             # Sets up database connection and has functions allowing DAOs to access CRUD queries
│   │   │   ├── models.py                               # Sets up models for the database
│   │   │   ├── dao/
│   │   │   │   ├── __init__.py
│   │   │   │   ├── attempt_statuses_dao.py             # attempt_statuses_table DAO
│   │   │   │   ├── collected_content_metadata_dao.py   # collected_content_metadata table DAO
│   │   │   │   ├── collected_content_types_dao.py      # collected_content_types table DAO
│   │   │   │   ├── collected_contents_dao.py           # collected_contents table DAO
│   │   │   │   ├── collection_attempts_dao.py          # collection_attempts table DAO
│   │   │   │   ├── collection_configs_dao.py           # collection_configs table DAO
│   │   │   │   ├── collection_types_dao.py             # collection_types table DAO
│   │   │   │   ├── collector_names_dao.py              # collector_names table DAO
│   │   │   │   ├── content_metadata_schemas_dao.py     # content_metadata_schemas table DAO
│   │   │   │   ├── debug_wikipedia_results_dao.py      # debug_wikipedia_results table DAO
│   │   │   │   ├── error_types_dao.py                  # error_types table DAO
│   │   │   │   ├── link_attempts_to_runs_dao.py        # link_attempts_to_runs table DAO
│   │   │   │   ├── run_collection_metadata_dao.py      # run_collection_metadata table DAO
│   │   │   │   ├── run_statuses_dao.py                 # run_statuses table DAO
│   │   │   │   ├── run_types_dao.py                    # run_types table DAO
│   │   │   │   ├── track_schema_migrations_dao.py      # run_types table DAO
│   │   │   │   └── validation_statuses_dao.py          # validation_statuses table DAO
│   │   │   │   ├── database.py                         # Sets up database connection and has functions to access CRUD queries
│   │   │   │   └── models.py                           # Sets up models for the database
│   │   │   └── migrations/
│   │   │       ├── __init__.py
│   │   │       ├── versions/
│   │   │       │   └── 001_initial_schema.py           # Initial schema setup using alembic
│   │   │       ├── COMMANDS.md                         # Alembic commands cheat sheet
│   │   │       ├── env.py                              # Alembic created file, contains custom code to get database credentials from .env
│   │   │       └── script.py.maki                      # Alembic created file on install
│   │   └── utils/                                      # Collector Utils
│   │       ├── __init.py__
│   │       ├── data_utils.py                           # Save functionality for collectors
│   │       ├── evaluation.py
│   │       └── wikipedia_utils.py
│   ├── data_collection/                                # Data collection modules
│   │   ├── __init__.py
│   │   ├── debug.py                                    # Debug script (checks if collection config returns success or fail)
│   │   ├── wikipedia_collector.py                      # Political data collections from Wikipedia API
│   │   ├── social_collector.py                         # Social media data collection
│   │   └── market_collector.py                         # Financial data collection
│   ├── data_processing/                                # Data processing modules
│   │   ├── __init__.py
│   │   ├── cleaner.py                                  # Data cleaning functions
│   │   ├── feature_extractor.py                        # Feature extraction
│   │   └── data_classifier.py                          # Classifying data types
│   ├── politicsai/                                     # PoliticsAI component
│   │   ├── __init__.py
│   │   ├── personality_profiler.py
│   │   ├── relationship_analyzer.py
│   │   ├── opinion_analyzer.py
│   │   ├── historical_analyzer.py
│   │   └── prediction_engine.py
│   ├── stocksai/                                       # StocksAI component
│   │   ├── __init__.py
│   │   ├── event_analyzer.py
│   │   ├── innovation_analyzer.py
│   │   ├── sector_analyzer.py
│   │   ├── historical_analyzer.py
│   │   └── prediction_engine.py
│   └── visualization/                                  # Data visualization
│       ├── __init__.py
│       ├── plot_utils.py
│       └── dashboard.py
│
└── tests/                                              # Unit and integration tests
    ├── test_data_collection.py
    ├── test_politicsai.py
    └── test_stocksai.py
