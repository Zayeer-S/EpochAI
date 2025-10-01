# EpochAI

A modular data collection, cleaning, and machine learning framework for analyzing political and financial data. EpochAI provides a complete pipeline from web scraping to predictive modeling, with a focus on election forecasting using XGBoost and Monte Carlo simulation.

## Overview

EpochAI is a Python-based framework that handles the complete data science lifecycle:
- **Collection**: Extensible collector system supporting multiple data sources (Wikipedia, FiveThirtyEight, etc.)
- **Validation**: Schema-based validation with dynamic metadata schemas
- **Cleaning**: Pluggable cleaners that transform and validate raw data
- **Analysis**: ML-powered election nowcasting with XGBoost models and Monte Carlo simulation
- **Storage**: PostgreSQL database with SQLAlchemy ORM and Alembic migrations
- **Monitoring**: Comprehensive logging and collection statistics

## Features

### Data Pipeline
- **Modular Architecture**: Abstract base classes for collectors and cleaners make adding new sources straightforward and quick
- **Database-First Design**: PostgreSQL backend with full migration support via Alembic
- **Flexible Configuration**: YAML-based configuration with Pydantic validation
- **Batch Processing**: Configurable batch sizes for efficient database operations
- **Status Tracking**: Track collection attempts, validation status, and cleaning progress
- **Schema Versioning**: Dynamic metadata schemas with version tracking
- **Error Handling**: Comprehensive error tracking and retry mechanismss

### Machine Learning
- **Election Nowcasting**: XGBoost-based prediction models for electoral outcomes
- **Monte Carlo Simulation**: Probabilistic election forecasting with 10,000+ simulations
- **Temporal Weighing**: Exponential decay for poll recency
- **Feature Engineering**: Automated feature creation including rolling averages, state encoding, and quality metrics
- **Shy Voter Adjustment**: Configurable corrections for polling biases

## Project Structure Overview

```
EpochAI/
├── epochai/                    # Main package
│   ├── common/                 # Shared utilities and services
│   │   ├── config/            # Configuration loading and validation
│   │   ├── database/          # Database models, DAOs, and migrations
│   │   ├── services/          # Business logic layer
│   │   └── utils/             # Helper utilities and decorators
│   ├── data_collection/       # Collection framework
│   │   ├── collectors/        # Source-specific collectors
│   │   └── savers/            # Data persistence handlers
│   ├── data_processing/       # Processing framework
│   │   └── cleaners/          # Data transformation and validation
│   ├── politicsai/            # Political analysis components
│   │   └── election_nowcaster.py  # XGBoost election prediction
│   └── stocksai/              # Financial analysis components
├── app/                       # Flask web application
├── data/                      # Data storage (raw, processed, features, models)
├── docs/                      # Documentation and guides
├── tests/                     # Unit and integration tests
└── config.yml                 # Main configuration file
```

## Requirements

- Python 3.12+
- PostgreSQL database
- Core dependencies:
  - SQLAlchemy 2.0+
  - Alembic 1.16+
  - Pydantic 2.11+
  - PyYAML 6.0+
  - requests 2.32+
  - pandas 2.2+
  - numpy 1.26+
  - scikit-learn 1.7+
  - xgboost 3.0+

## Installation

### 1. Clone the repository
```bash
git clone <repository-url>
cd EpochAI
```

### 2. Create Virtual Environment
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Set Up Environment Variables
Create a `.env` file in the project root (see `docs/ENV_FILE_TEMPLATE.md` for reference):
```env
# Database Configuration
DB_HOST=localhost
DB_PORT=5432
DB_NAME=epochai
DB_USER=your_username
DB_PASSWORD=your_password

# API Keys (if needed)
# Add any API keys for data sources here
```

### 5. Initialize Database
```bash
# Run Alembic migrations
alembic upgrade head
```

### 6. Configure the Project
Edit `config.yml` to customize:
- Logging settings
- Data output preferences
- Collector-specific configurations
- Validation rules
- Cleaner settings

## Quick Start

### Collecting Data

The collector CLI provides a unified interface for all data sources:

```bash
# Check available collectors
python -m epochai.data_collection.collector

# Get status of a collector
python -m epochai.data_collection.collector status wikipedia

# Collect all uncollected data
python -m epochai.data_collection.collector collect wikipedia

# Collect specific collection types
python -m epochai.data_collection.collector collect fivethirtyeight --type polls-2016

# Filter by language
python -m epochai.data_collection.collector collect wikipedia --language en,fr

# Check targets before collecting
python -m epochai.data_collection.collector check wikipedia --type political_events

# Retry failed collections
python -m epochai.data_collection.collector retry wikipedia

# Dry run to preview what would be collected
python -m epochai.data_collection.collector collect wikipedia --dry-run
```

### Cleaning Data

The cleaner CLI transforms and validates collected data:

```bash
# Clean specific raw data IDs
python -m epochai.data_processing.cleaner clean fivethirtyeight --ids "1,2,3"

# Clean by ID range
python -m epochai.data_processing.cleaner clean fivethirtyeight --ids "10-30"

# Clean by validation status
python -m epochai.data_processing.cleaner clean-by-status fivethirtyeight --status valid

# Clean recent data (last 24 hours)
python -m epochai.data_processing.cleaner clean-recent fivethirtyeight --hours 24

# Get cleaning statistics
python -m epochai.data_processing.cleaner stats fivethirtyeight

# Get schema information
python -m epochai.data_processing.cleaner schema-info fivethirtyeight

# List available cleaners
python -m epochai.data_processing.cleaner list-cleaners
```

### Running Election Predictions

Use the ElectionNowcaster for ML-powered election forecasting:

```python
from epochai.politicsai.election_nowcaster import ElectionNowcaster

# Initialize nowcaster for 2016 election
predictor = ElectionNowcaster(election_year=2016)

# Run complete prediction pipeline
results = predictor.run_prediction_pipeline(
    candidates=["Donald Trump", "Hillary Clinton"],
    current_date="2016-11-08",
    lookback_days=60,
    n_simulations=10000,
    shy_voter_adjustment=2.0,  # Add 2% shy voter effect
    shy_candidate="Donald Trump",
    uncertainty_std=3.0
)

# Display results
for i, candidate in enumerate(results['candidates']):
    win_prob = results['win_probabilities'][i]
    expected_ec = results['expected_electoral_votes'][i]
    print(f"{candidate}: {win_prob:.1%} win probability, {expected_ec:.1f} EC votes")
```

**Example Output (1 day before 2016 Election):**
```
=== 2016 Election Nowcast ===
Nowcast Date: 2016-11-07
Data Source: pct_estimate
Shy voter adjustment: +2.0 points for Donald Trump

Donald Trump:
    Win Probability: 54.3%
    Expected EC Votes: 289.2

Hillary Clinton:
    Win Probability: 45.7%
    Expected EC Votes: 251.8

Total EC Votes: 541.0

Model used 847 polls
Poll recency: 0 to 60 days ago
XGBoost Model Scores:
    Donald Trump: R² = 0.876 (423 samples)
    Hillary Clinton: R² = 0.891 (424 samples)
```

## Architecture

### Collectors

Collectors inherit from `BaseCollector` and implement:
- `_collect_and_save()`: Core collection logic
- Source-specific API interactions
- Batch processing and database saving

**Available Collectors:**
- `wikipedia_collector`: Collects Wikipedia articles
- `fivethirtyeight_collector`: Collects polling data from FiveThirtyEight

### Cleaners

Cleaners inherit from `BaseCleaner` and implement:
- `transform_content()`: Transform raw data into validated format
- Schema-based validation
- Metadata extraction and enrichment

**Available Cleaners:**
- `wikipedia_cleaner`: Processes Wikipedia articles
- `fivethirtyeight_cleaner`: Processes and normalizes polling data

### ML Models

#### ElectionNowcaster

The `ElectionNowcaster` class provides comprehensive election forecasting:

**Key Features:**
- **XGBoost Regression**: Separate models trained for each candidate
- **Feature Engineering**: 11 engineered features including temporal, quality, and geographic metrics
- **Temporal Weighting**: Exponential decay (5% per 30 days) for poll recency
- **Quality Scoring**: Incorporates FiveThirtyEight pollster ratings and data quality metrics
- **State Encoding**: Label-encoded state features with historical lean indicators
- **Monte Carlo Simulation**: 10,000+ simulations for probabilistic forecasting
- **Uncertainty Modeling**: Gaussian noise injection with configurable standard deviation
- **Shy Voter Correction**: Optional adjustment for systematic polling biases

**Pipeline:**
1. Load polling data from cleaned_data table
2. Apply temporal weights and quality scores
3. Engineer ML features (rolling averages, state characteristics)
4. Train XGBoost models per candidate
5. Generate state-level predictions
6. Run Monte Carlo simulations
7. Calculate win probabilities and expected electoral votes

### Database Schema

Key tables:
- `collection_targets`: Items to collect
- `collection_attempts`: Individual collection attempts
- `raw_data`: Collected raw content
- `cleaned_data`: Processed and validated content
- `*_metadata_schemas`: Dynamic schemas for validation

See `docs/diagrams/ERD.drawio.png` for the complete entity relationship diagram.

## Configuration

### See config.yml file

### Database Migrations

```bash
# Create a new migration
alembic revision -m "description"

# Apply migrations
alembic upgrade head

# Rollback migration
alembic downgrade -1

# See migration history
alembic history
```

See `docs/cheatsheets/ALEMBIC_CHEATSHEET.md` for more commands.

## Development

### Code Quality

The project uses:
- **Ruff**: Fast Python linter and formatter
- **MyPy**: Static type checking
- **Pre-commit hooks**: Automated code quality checks

```bash
# Install pre-commit hooks
pre-commit install

# Run pre-commit manually
pre-commit run --all-files
```

See `docs/cheatsheets/PRECOMMIT_CHEATSHEET.md` for more information.

### Running Tests

```bash
# Run all tests
pytest

# Run with coverage
pytest --cov=epochai

# Run specific test file
pytest tests/unit_tests/test_wikipedia_cleaner.py
```

## ML Model Details

### Feature Engineering

The ElectionNowcaster creates 11 features:

**Temporal Features:**
- `days_since_poll_norm`: Normalized days since poll (0-1 scale)
- `weeks_since_poll`: Time decay in weeks
- `time_weight`: Exponential decay weight (e^(-0.05 * days/30))

**Geographic Features:**
- `state_encoded`: Label-encoded state identifier
- `state_lean`: Historical state partisan lean (-1: Dem, 0: Neutral, 1: Rep)
- `is_swing_state`: Binary indicator for battleground states

**Rolling Aggregates:**
- `poll_avg_7d`: 7-day rolling average
- `poll_avg_14d`: 14-day rolling average  
- `poll_avg_30d`: 30-day rolling average

**Quality Metrics:**
- `quality_score_norm`: Normalized data quality score
- `influence_norm`: Normalized FiveThirtyEight pollster influence

### Model Training

- **Algorithm**: XGBoost Regressor
- **Hyperparameters**: 100 estimators, max_depth=4, learning_rate=0.1
- **Evaluation**: Train/test split with R² scoring
- **Per-Candidate Models**: Separate model for each candidate

### Simulation Details

- **Method**: Monte Carlo simulation
- **Iterations**: 10,000 (configurable)
- **Uncertainty**: Gaussian noise with configurable std (default: 3.0%)
- **Output**: Win probabilities, expected electoral votes, state-level predictions

## Logging

Logs are written to the `logs/` directory by default. Configure logging in `config.yml`:

```yaml
logging:
  level: "INFO"          # DEBUG, INFO, WARNING, ERROR, CRITICAL
  log_to_file: True
  log_directory: "logs"
```

Disable file logging with CLI flags:
```bash
python -m epochai.data_collection.collector collect wikipedia --no-log-file
```

## Troubleshooting

### Database Connection Issues
- Verify PostgreSQL is running
- Check `.env` file credentials
- Ensure database exists: `createdb epochai`

### Collection Failures
- Check API rate limits in `config.yml`
- Verify network connectivity
- Review logs in `logs/` directory
- Use `--log-level DEBUG` for verbose output

### Schema Validation Errors
- Run `schema-info` to check current schema
- Use `reload-schema` to refresh from database
- Check `cleaned_data_metadata_schemas` table

### ML Model Issues
- Ensure sufficient cleaned polling data exists
- Check date range covers the election period
- Verify candidate names match database entries
- Increase `lookback_days` if insufficient data

## Documentation

- **Architecture**: `docs/architecture/STRUCTURE.md`
- **User Guide**: `docs/USER_GUIDE.md`
- **Alembic Commands**: `docs/cheatsheets/ALEMBIC_CHEATSHEET.md`
- **Pre-commit Guide**: `docs/cheatsheets/PRECOMMIT_CHEATSHEET.md`
- **Entity Relationship Diagram**: `docs/diagrams/ERD.drawio.png`

## Future Enhancements

- LLM model for legislative predictions
- Expand Nowcaster to international elections
- React/Flask website for LLM-
- Automated data collection and cleaning

## Author

Personal learning project built to explore:
- Database design and ORM patterns
- Abstract base classes and protocols
- Service layer architecture
- Data pipeline patterns
- CLI development
- Machine learning model deployment
- XGBoost and scikit-learn
- Monte Carlo simulation techniques
- LLM models
- React for frontend development
- Flask for backend development
