# PredictAI File Structure
This document expplains the file structure of the project

PredictAI/      
├── .env                                # Hide API keys and any other confidential information
├── .gitignore                          # Files to exclude from version control
├── config.yml                          # Configuration settings
├── README.md                           # Project overview and documentation
├── requirements.txt                    # Python dependencies
├── setup.py                            # Package installation setup
│       
├── app/                                # Web application
│   ├── __init__.py     
│   ├── app.py                          # Flask application
│   ├── routes.py                       # API endpoints
│   ├── static/                         # Static assets
│   │   ├── css/ 
│   │   │   └── styles.css       
│   │   ├── js/  
│   │   │   └── scripts.js
│   │   └── images/     
│   └── templates/                      # HTML templates
│       └── index.html      
│       
├── data/                               # Data directory
│   ├── raw/                            # Raw collected data
│   ├── processed/                      # Cleaned and processed data
│   ├── features/                       # Extracted features
│   └── models/                         # Trained models
│       
├── docs/                               # Documentation
│   ├── architecture/                   # Architecture diagrams
│   │   ├── STRUCTURE.md                # File structure
│   └── USER_GUIDE.md
│
├── notebooks/                          # Jupyter notebooks
│   ├── data_exploration.ipynb      
│   ├── model_evaluation.ipynb      
│   └── visualization.ipynb     
│       
├── predictai/                          # Main package directory
│   ├── __init__.py     
│   ├── common/                         # Shared utilities
│   │   ├── __init__.py     
│   │   ├── data_utils.py               # Shared data processing functions
│   │   └── evaluation.py               # Model evaluation metrics
│   ├── data_collection/                # Data collection modules
│   │   ├── __init__.py     
│   │   ├── news_collector.py           # News API data collection
│   │   ├── social_collector.py         # Social media data collection
│   │   └── market_collector.py         # Financial data collection
│   ├── data_processing/                # Data processing modules
│   │   ├── __init__.py     
│   │   ├── cleaner.py                  # Data cleaning functions
│   │   ├── feature_extractor.py        # Feature extraction 
│   │   └── data_classifier.py          # Classifying data types
│   ├── politicsai/                     # PoliticsAI component
│   │   ├── __init__.py     
│   │   ├── personality_profiler.py
│   │   ├── relationship_analyzer.py
│   │   ├── opinion_analyzer.py
│   │   ├── historical_analyzer.py
│   │   └── prediction_engine.py
│   ├── stocksai/                       # StocksAI component
│   │   ├── __init__.py
│   │   ├── event_analyzer.py
│   │   ├── innovation_analyzer.py
│   │   ├── sector_analyzer.py
│   │   ├── historical_analyzer.py
│   │   └── prediction_engine.py
│   └── visualization/                  # Data visualization
│       ├── __init__.py
│       ├── plot_utils.py
│       └── dashboard.py
│
└── tests/                              # Unit and integration tests
    ├── test_data_collection.py
    ├── test_politicsai.py
    └── test_stocksai.py