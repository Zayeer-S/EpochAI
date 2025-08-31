from alembic import op
import sqlalchemy as sa

revision = '001'
down_revision = None
branch_labels = None
depends_on = None

def upgrade():
    # LOOKUP TABLES
    op.execute("""
        CREATE TABLE IF NOT EXISTS collector_names (
            id SERIAL PRIMARY KEY,
            collector_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS collection_types(
            id SERIAL PRIMARY KEY,
            collection_type  TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS collection_statuses (
            id SERIAL PRIMARY KEY,
            collection_status_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS attempt_statuses (
            id SERIAL PRIMARY KEY,
            attempt_status_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS error_types (
            id SERIAL PRIMARY KEY,
            error_type_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS validation_statuses (
            id SERIAL PRIMARY KEY,
            validation_status_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS run_types (
            id SERIAL PRIMARY KEY,
            run_type_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS run_statuses (
            id SERIAL PRIMARY KEY,
            run_status_name TEXT NOT NULL UNIQUE,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS raw_data_metadata_schemas (
            id SERIAL PRIMARY KEY,
            metadata_schema JSONB NOT NULL,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS cleaned_data_metadata_schemas (
        id SERIAL PRIMARY KEY,
        metadata_schema JSONB NOT NULL,
        updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
    );
    """)

    # MAIN TABLES
    op.execute("""
        CREATE TABLE IF NOT EXISTS collection_targets(
            id SERIAL PRIMARY KEY,
            collector_name_id INTEGER NOT NULL REFERENCES collector_names(id) ON DELETE RESTRICT,
            collection_type_id INTEGER NOT NULL REFERENCES collection_types(id) ON DELETE RESTRICT,
            language_code TEXT NOT NULL,
            collection_name TEXT NOT NULL,
            collection_status_id INTEGER NOT NULL REFERENCES collection_statuses(id) ON DELETE RESTRICT,
            updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),

            CONSTRAINT ensure_collection_target_is_unique UNIQUE(collector_name_id, collection_type_id, language_code, collection_name)
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS collection_attempts(
            id SERIAL PRIMARY KEY,
            collection_target_id INTEGER NOT NULL REFERENCES collection_targets(id) ON DELETE RESTRICT,
            language_code TEXT NOT NULL,
            search_term_used TEXT NOT NULL,
            attempt_status_id INTEGER NOT NULL REFERENCES attempt_statuses(id) ON DELETE RESTRICT,
            error_type_id INTEGER REFERENCES error_types(id) ON DELETE RESTRICT,
            error_message TEXT,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS raw_data (
            id SERIAL PRIMARY KEY,
            collection_attempt_id INTEGER NOT NULL REFERENCES collection_attempts(id) ON DELETE CASCADE,
            raw_data_metadata_schema_id INTEGER NOT NULL REFERENCES raw_data_metadata_schemas(id) ON DELETE RESTRICT,
            title TEXT NOT NULL,
            language_code TEXT NOT NULL,
            url TEXT NULL,
            metadata jsonb NOT NULL,
            validation_status_id INTEGER NOT NULL REFERENCES validation_statuses(id) ON DELETE RESTRICT,
            validation_error JSON,
            filepath_of_save TEXT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS debug_wikipedia_results (
            id SERIAL PRIMARY KEY,
            collection_target_id INTEGER NOT NULL REFERENCES collection_targets(id) ON DELETE CASCADE,
            search_term_used TEXT NOT NULL,
            language_code TEXT NOT NULL,
            test_status TEXT NOT NULL,
            search_results_found JSON NOT NULL DEFAULT '[]',
            error_message TEXT NOT NULL DEFAULT '',
            test_duration INTEGER NOT NULL,
            created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );
    """)

    op.execute("""
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
    """)

    op.execute("""
        CREATE TABLE IF NOT EXISTS link_attempts_to_runs (
            id SERIAL PRIMARY KEY,
            collection_attempt_id INTEGER NOT NULL REFERENCES collection_attempts(id) ON DELETE CASCADE,
            run_collection_metadata_id INTEGER NOT NULL REFERENCES run_collection_metadata(id) ON DELETE CASCADE,

            CONSTRAINT unique_attempt_to_runs UNIQUE (collection_attempt_id, run_collection_metadata_id)
        );
    """)

    op.execute("""
    CREATE TABLE IF NOT EXISTS cleaned_data (
        id SERIAL PRIMARY KEY,
        raw_data_id INTEGER NOT NULL REFERENCES raw_data(id) ON DELETE CASCADE,
        cleaned_data_metadata_schema_id INTEGER NOT NULL REFERENCES cleaned_data_metadata_schemas(id) ON DELETE RESTRICT,
        title TEXT NOT NULL,
        language_code TEXT NOT NULL,
        url TEXT NULL,
        metadata jsonb NOT NULL,
        validation_status_id INTEGER NOT NULL REFERENCES validation_statuses(id) ON DELETE RESTRICT,
        validation_error JSON,
        cleaner_used TEXT NOT NULL,
        cleaner_version TEXT NOT NULL,
        cleaning_time_ms INTEGER NOT NULL,
        cleaned_at TIMESTAMP WITH TIME ZONE NOT NULL,
        created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()

        CONSTRAINT uk_cleaned_data_raw_cleaner_version UNIQUE (cleaner_used, cleaner_version, raw_data_id)
    );
    """)

    # INDEXES
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_targets_collector_types ON collection_targets(collector_name_id, collection_type_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_targets_collection_statuses ON collection_targets(collection_status_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_targets_language_code ON collection_targets(language_code);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_attempts_config_id ON collection_attempts(collection_target_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_attempts_status_id ON collection_attempts(attempt_status_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_attempts_created_at ON collection_attempts(created_at);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_collection_attempts_error_type_id ON collection_attempts(error_type_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_data_attempt_id ON raw_data(collection_attempt_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_data_validation_status_id ON raw_data(validation_status_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_data_title ON raw_data(title);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_raw_data_created_at ON raw_data(created_at);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_collection_target_id ON debug_wikipedia_results(collection_target_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_test_status ON debug_wikipedia_results(test_status);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_debug_wikipedia_results_created_at ON debug_wikipedia_results(created_at);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_run_type_id ON run_collection_metadata(run_type_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_run_status_id ON run_collection_metadata(run_status_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_run_collection_metadata_created_at ON run_collection_metadata(created_at);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_link_attempts_to_runs_collection_attempt_id ON link_attempts_to_runs(collection_attempt_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_link_attempts_to_runs_run_collection_metadata_id ON link_attempts_to_runs(run_collection_metadata_id);
    """)
    op.execute("""
    CREATE INDEX IF NOT EXISTS idx_cleaned_data_raw_data_id ON cleaned_data(raw_data_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_cleaned_data_validation_status_id ON cleaned_data(validation_status_id);
    """)
    op.execute("""
        CREATE INDEX IF NOT EXISTS idx_cleaned_data_created_at ON cleaned_data(created_at);
    """)

    # COMMENTS
    op.execute("""
        COMMENT ON TABLE collector_names IS 'Lookup table for different collectors';
    """)
    op.execute("""
        COMMENT ON TABLE collection_types IS 'Lookup table for collection types';
    """)
    op.execute("""
        COMMENT ON TABLE collection_targets IS 'Configuration of what needs to be collected';
    """)
    op.execute("""
        COMMENT ON TABLE collection_statuses IS 'Lookup table for collection target statuses';
    """)
    op.execute("""
        COMMENT ON TABLE collection_attempts IS 'Log of each collection attempt';
    """)
    op.execute("""
        COMMENT ON TABLE raw_data IS 'Table storing collected content from collectors';
    """)
    op.execute("""
        COMMENT ON TABLE debug_wikipedia_results IS 'Results from debug testing of wikipedia collector';
    """)
    op.execute("""
        COMMENT ON TABLE run_collection_metadata IS 'Metadata about collection runs and their performance';
    """)
    op.execute("""
        COMMENT ON TABLE link_attempts_to_runs IS 'Link table between collection_attempts and run_collection_metadata';
    """)
    op.execute("""
    COMMENT ON TABLE cleaned_data IS 'Table storing cleaned data derived from raw_data table';
    """)
    op.execute("""
        COMMENT ON TABLE cleaned_data_metadata_schemas IS 'Schema definitions for cleaned data metadata validation';
    """)

    # COLUMN COMMENTS
    op.execute("""
        COMMENT ON COLUMN collection_attempts.language_code IS 'Actual language code used during the collection attempt';
    """)
    op.execute("""
        COMMENT ON COLUMN collection_attempts.search_term_used IS 'Actual search term used during collection attempt';
    """)
    op.execute("""
        COMMENT ON COLUMN raw_data.validation_error IS 'JSON object containing the validation error details if the validation has failed';
    """)
    op.execute("""
        COMMENT ON COLUMN debug_wikipedia_results.test_duration IS 'Test duration in miliseconds';
    """)
    op.execute("""
        COMMENT ON COLUMN run_collection_metadata.config_used IS 'JSON of configuration used during the run';
    """)

def downgrade():
    op.execute("DROP TABLE IF EXISTS link_attempts_to_runs CASCADE;")
    op.execute("DROP TABLE IF EXISTS run_collection_metadata CASCADE;")
    op.execute("DROP TABLE IF EXISTS debug_wikipedia_results CASCADE;")
    op.execute("DROP TABLE IF EXISTS raw_data CASCADE;")
    op.execute("DROP TABLE IF EXISTS collection_attempts CASCADE;")
    op.execute("DROP TABLE IF EXISTS collection_targets CASCADE;")
    op.execute("DROP TABLE IF EXISTS cleaned_data CASCADE;")

    op.execute("DROP TABLE IF EXISTS raw_data_metadata_schemas CASCADE;")
    op.execute("DROP TABLE IF EXISTS cleaned_data_metadata_schemas CASCADE;")
    op.execute("DROP TABLE IF EXISTS run_statuses CASCADE;")
    op.execute("DROP TABLE IF EXISTS run_types CASCADE;")
    op.execute("DROP TABLE IF EXISTS validation_statuses CASCADE;")
    op.execute("DROP TABLE IF EXISTS error_types CASCADE;")
    op.execute("DROP TABLE IF EXISTS attempt_statuses CASCADE;")
    op.execute("DROP TABLE IF EXISTS collection_statuses CASCADE;")
    op.execute("DROP TABLE IF EXISTS collection_types CASCADE;")
    op.execute("DROP TABLE IF EXISTS collector_names CASCADE;")
