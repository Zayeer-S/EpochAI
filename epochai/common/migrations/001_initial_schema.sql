BEGIN; -- THE SQL!

--  LOOKUP TABLES
CREATE TABLE IF NOT EXISTS collector_names (
    id SERIAL PRIMARY KEY,
    collector_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collection_types(
    id SERIAL PRIMARY KEY,
    collection_type  TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS attempt_statuses (
    id SERIAL PRIMARY KEY,
    attempt_status_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS error_types (
    id SERIAL PRIMARY KEY,
    error_type_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS validation_statuses (
    id SERIAL PRIMARY KEY,
    validation_status_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collected_content_types (
    id SERIAL PRIMARY KEY,
    collected_content_type_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_types (
    id SERIAL PRIMARY KEY,
    run_type_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_statuses (
    id SERIAL PRIMARY KEY,
    run_status_name TEXT NOT NULL UNIQUE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS content_metadata_schemas (
    id SERIAL PRIMARY KEY,
    content_metadata_schema JSONB NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);



-- MAIN TABLES
CREATE TABLE IF NOT EXISTS collection_configs(
    id SERIAL PRIMARY KEY,
    collector_name_id INTEGER NOT NULL REFERENCES collector_names(id) ON DELETE RESTRICT,
    collection_type_id INTEGER NOT NULL REFERENCES collection_types(id) ON DELETE RESTRICT,
    language_code TEXT NOT NULL,
    collection_name TEXT NOT NULL,
    is_collected BOOLEAN NOT NULL DEFAULT FALSE,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

    CONSTRAINT ensure_collection_config_is_unique UNIQUE(collector_name_id, collection_type_id, language_code, collection_name)
);

CREATE TABLE IF NOT EXISTS collection_attempts(
    id SERIAL PRIMARY KEY,
    collection_config_id INTEGER NOT NULL REFERENCES collection_configs(id) ON DELETE RESTRICT,
    language_code_used TEXT NOT NULL,
    search_term_used TEXT NOT NULL,
    attempt_status_id INTEGER NOT NULL REFERENCES attempt_statuses(id) ON DELETE RESTRICT,
    error_type_id INTEGER REFERENCES error_types(id) ON DELETE RESTRICT,
    error_message TEXT,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collected_contents (
    id SERIAL PRIMARY KEY,
    collection_attempt_id INTEGER NOT NULL REFERENCES collection_attempts(id) ON DELETE CASCADE,
    content_type_id INTEGER NOT NULL REFERENCES collected_content_types(id) ON DELETE RESTRICT,
    content_metadata_schema_id INTEGER NOT NULL REFERENCES content_metadata_schemas(id) ON DELETE RESTRICT,
    title TEXT NOT NULL,
    main_content TEXT NOT NULL,
    url TEXT,
    validation_status_id INTEGER NOT NULL REFERENCES validation_statuses(id) ON DELETE RESTRICT,
    validation_error JSON,
    filepath_of_save TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS collected_content_metadata (
    id SERIAL PRIMARY KEY,
    collected_content_id INTEGER NOT NULL REFERENCES collected_contents(id) ON DELETE CASCADE,
    metadata_key TEXT NOT NULL,
    metadata_value TEXT NOT NULL,

    CONSTRAINT unique_content_metadata UNIQUE (collected_content_id, metadata_key)
);

CREATE TABLE IF NOT EXISTS debug_wikipedia_results ( 
    id SERIAL PRIMARY KEY,
    collection_config_id INTEGER NOT NULL REFERENCES collection_configs(id) ON DELETE CASCADE,
    search_term_used TEXT NOT NULL,
    language_code_used TEXT NOT NULL,
    test_status TEXT NOT NULL,
    search_results_found JSON NOT NULL DEFAULT '[]',
    error_message TEXT NOT NULL DEFAULT '',
    test_duration INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS run_collection_metadata (
    id SERIAL PRIMARY KEY,
    collection_attempt_id INTEGER REFERENCES collection_attempts(id) ON DELETE SET NULL,
    run_type_id INTEGER NOT NULL  REFERENCES run_types(id) ON DELETE RESTRICT,
    run_status_id INTEGER NOT NULL REFERENCES run_statuses(id) ON DELETE RESTRICT,
    attempts_successful INTEGER NOT NULL DEFAULT 0,
    attempts_failed INTEGER NOT NULL DEFAULT 0,
    config_used JSON,
    completed_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS link_attempts_to_runs (
    id SERIAL PRIMARY KEY,
    collection_attempt_id INTEGER NOT NULL REFERENCES collection_attempts(id) ON DELETE CASCADE,
    run_collection_metadata_id INTEGER NOT NULL REFERENCES run_collection_metadata(id) ON DELETE CASCADE,

    CONSTRAINT unique_attempt_to_runs UNIQUE (collection_attempt_id, run_collection_metadata_id)
);

CREATE TABLE IF NOT EXISTS schema_migrations (
    id SERIAL PRIMARY KEY,
    version TEXT NOT NULL UNIQUE,
    filename TEXT NOT NULL,
    checksum TEXT,
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    execution_time_seconds DECIMAL(10,3) NOT NULL,
    status TEXT NOT NULL,
    error_message TEXT,
    rolled_back_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);


-- INDEX TIME
CREATE INDEX IF NOT EXISTS idx_collection_configs_collector_types ON collection_configs(collector_name_id, collection_type_id);
CREATE INDEX IF NOT EXISTS idx_collection_configs_is_collected ON collection_configs(is_collected);
CREATE INDEX IF NOT EXISTS idx_collection_configs_language_code ON collection_configs(language_code);

CREATE INDEX IF NOT EXISTS idx_collection_attempts_config_id ON collection_attempts(collection_config_id);
CREATE INDEX IF NOT EXISTS idx_collection_attempts_status_id ON collection_attempts(attempt_status_id);
CREATE INDEX IF NOT EXISTS idx_collection_attempts_created_at ON collection_attempts(created_at);
CREATE INDEX IF NOT EXISTS idx_collection_attempts_error_type_id ON collection_attempts(error_type_id);

CREATE INDEX IF NOT EXISTS idx_collected_contents_attempt_id ON collected_contents(collection_attempt_id);
CREATE INDEX IF NOT EXISTS idx_collected_contents_validation_status_id ON collected_contents(validation_status_id);
CREATE INDEX IF NOT EXISTS idx_collected_contents_title ON collected_contents(title);
CREATE INDEX IF NOT EXISTS idx_collected_contents_created_at ON collected_contents(created_at);

CREATE INDEX IF NOT EXISTS idx_content_metadata_content_id ON collected_content_metadata(collected_content_id);
CREATE INDEX IF NOT EXISTS idx_content_metadata_key ON collected_content_metadata(metadata_key);

CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_collection_config_id ON debug_wikipedia_results(collection_config_id);
CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_test_status ON debug_wikipedia_results(test_status);
CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_created_at ON debug_wikipedia_results(created_at);

CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_run_type_id ON run_collection_metadata(run_type_id);
CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_run_status_id ON run_collection_metadata(run_status_id);
CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_created_at ON run_collection_metadata(created_at);

CREATE INDEX IF NOT EXISTS idx_link_attempts_to_runs_collection_attempt_id ON link_attempts_to_runs(collection_attempt_id);
CREATE INDEX IF NOT EXISTS idx_link_attempts_to_runs_run_collection_metadata_id ON link_attempts_to_runs(run_collection_metadata_id);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_version ON schema_migrations(version);
CREATE INDEX IF NOT EXISTS idx_schema_migrations_status ON schema_migrations(status);
CREATE INDEX IF NOT EXISTS idx_schema_migrations_executed_at ON schema_migrations(executed_at);



-- Table comments
COMMENT ON TABLE collector_names IS 'Lookup table for different collectors'
COMMENT ON TABLE collection_types IS 'Lookup table for collection types'
COMMENT ON TABLE collection_configs IS 'Configuration of what needs to be collected'
COMMENT ON TABLE collection_attempts IS 'Log of each collection attempt'
COMMENT ON TABLE collected_contents IS 'Table storing collected content from collectors'
COMMENT ON TABLE collected_content_metadata IS 'Key : Value metadata for collected data'
COMMENT ON TABLE debug_wikipedia_results IS 'Results from debug testing of wikipedia collector'
COMMENT ON TABLE run_collection_metadata IS 'Metadata about collection runs and their performance'
COMMENT ON TABLE link_attempts_to_runs IS 'Link table between collection_attempts and run_collection_metadata'



--- Column Comments
COMMENT ON COLUMN collection_configs.is_collected IS 'Whether or not this configuration has beeen successfully collected (True = Collected)'
COMMENT ON COLUMN collection_attempts.language_code_used IS 'Actual language code used during the collection attempt'
COMMENT ON COLUMN collection_attempts.search_term_used IS 'Actual search term used during collection attempt'
COMMENT ON COLUMN collected_contents.validation_error IS 'JSON object containing the validation error details if the validation has failed'
COMMENT ON COLUMN debug_wikipedia_results.test_duration IS 'Test duration in miliseconds'
COMMENT ON COLUMN run_collection_metadata.config_used IS 'JSON of configuration used during the run'



COMMIT;



-- Table check
SELECT table_name
FROM information_schema.tables
WHERE table_schema = 'public'
AND table_name in (
    'collector_names', 'collection_types', 'collection_configs', 'attempt_statuses', 'error_types', 'collection_attempts',
    'validation_statuses', 'collected_content_types', 'collected_contents', 'collected_content_metadata', 'content_metadata_schemas',
    'debug_wikipedia_results', 'run_types', 'run_statuses', 'run_collection_metadata', 'link_attempts_to_runs'
)
ORDER BY table_name