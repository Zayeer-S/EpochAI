# PredictAI: Political Action and Stock Market Prediction System

## Project Overview
PredictAI is a system that predicts financial markets by using historical data, recent political actions, social media data, and predictions of future political actions (from PoliticsAI). The system consists of two AI components, with the PoliticsAI feeding into StocksAI:

1. **PoliticsAI**: Models political behaviour patterns, using a personality profile, relationships between the leaders, opinion polls, and historical patterns, to predict actions of certain political figures, governments and influential individuals
2. **StocksAI**: Predicts stock market movements using historical data, potential technological breakthroughs, confirmed political events (from itself) and predicted political actions (from PoliticsAI)

## System Architecture

### PoliticsAI Component
The PoliticsAI uses machine learning to forcast political actions with probabilities through:

- **Personality Profiling**: Models individual politician decision patterns and behavioural tendencies
- **Inter leadership Relationship Profiling**: Analyses personal relationships between political figures
- **Opinion Poll Analysis**: Analyses opinion polls to incorporate public sentiment and analyses minor election results (e.g. byelections)
- **Historical Pattern Analysis**: Identifies recurring patterns in political decision making
- **Probabilty Calculation**: Assigns confidence scores to predictions based on (TBD) factors
- **Temporal Forecasting**: Provides short-term (1-7 days), medium-term (1-4 weeks), and long-term (1-3 months) predictions

### StocksAI Component
The StocksAI transforms political predictions into market forecasts by:

- **Confirmed Political Event Analysis**: Identifies current, confirmed/factual political events
- **Innovations Analysis**: Identifies potential and confirmed innovation across sectors that could affect stock performance
- **Sector-Specific Analysis**: Evaluates differing impacts across market sectors
- **Historical Pattern Analyiss**: Identifies historical trends and reasons for those trends
- **Probabilty Calculation**: Assigns confidence scores to predictions based on (TBD) factors
- **Togglable Prediction Integration**: Allows switching PoliticsAI inputs on/off for comparative analysis
- **Temporal Forecasting**: Provides short-term (1-7 days), medium-term (1-4 weeks), and long-term (1-3 months) predictions

## Planned Development Phases	

### Phase 1: Overall Project Planning (In Progress)
- [x] Project scoping and planning
- [x] Initial data collection strategy
- [x] System architecture design and documentation
- [] Finalise Data Sources
- [] Design file structure

### Phase 2: PoliticsAI Development
- [] Politician profile modelling component
- [] Inter-leader relationship modelling component
- [] Opinion poll and minor election 
- [] Historical political action analysis component
- [] Feature engineering pipeline
- [] Initial model prototyping
- [] Performance evaluation

### Phase 3: StocksAI Development (Planned)
- [] Market data integration 
- [] Sector specific analysis component
- [] Innovations analysis component
- [] Confirmed political event analysis component
- [] Political-financial correlation analysis component
- [] Integrating PoliticsAI outputs
- [] Initial model prototyping
- [] Back testing framework
- [] Performance evaluation

## Training and Inference Pipelines
**The system has the following two pipelines:**
- **Training Pipeline**: Runs periodically to build and update prediction models
- **Inference Pipeline**: Executes on-demand to create predictions using pre-trained models and any data inputs


## Data Sources (TBD specific sources which will be listed)
- Official government communications
- Press releases and statements
- Historical political action records
- News articles and analysis
- Economic indicators
- Historical market data
- Social Media
- Opinion Polls

## Data Processing
- **Data Cleaning**: Remove noise and standardise formats across sources. Translate texts into English using Google translate if necessary.
- **Text Processing**: Extract meaningful information from unstructured texts
- **Data Classification**: Classify political events into confirmed and speculative information. Classify financial information according to different sectors.
- **Feature Extraction**: Transformr aw data into model-ready inputs

## Technologies
- Python (as the primary language)
- pandas & numpy (for data processing)
- scikit-learn (for initial modeling)
- PyTorch/TensorFlow (for advanced modeling)
- NLP libraries (for text processing)
- Web scraping tools (for data collection)
- Git (for version control)
- Database technologies (TBD) (for storing data and predictions)

## Project Limitations & Ethical Considerations
- This system is developed for educational purposes and research
- Market predictions come with inherent uncertainty
- The system is not intended for actual investment decisions
- All predictions should be interpreted as probabilities and never as certainties
- Data privacy and ethical use of public information are prioritized
- Clear distinction shall be maintained between confirmed facts and AI-generated predictions
- The system may not have access to all facts and might not be able to display all facts

## Roadmap
1. Complete Phase 1 of development (Ongoing, 03/05/2025 - Present)
2. Start Phase 2 of development (Expected Start 19/05/2025)
3. Start Phase 3 of development (Expected Start 19/06/2025)

## Project Goals
- Demonstrate practical application of machine learning for real-world forecasting
- Explore the relationship between political actions and market movements
- Create a functional prototype that showcases computer science principles
- Develop a meaningful portfolio project for academic and career development