INSERT INTO collector_names (collector_name) VALUES 
    ('wikipedia_collector')
ON CONFLICT (collector_name) DO NOTHING;

INSERT INTO collection_types (collection_type) VALUES 
    ('politicians'),
    ('important_persons'),
    ('political_topics')
ON CONFLICT (collection_type) DO NOTHING;

INSERT INTO attempt_statuses (attempt_status_name) VALUES
    ('success'),
    ('failed'),
    ('pending'),
    ('timed out'),
    ('cancelled')
ON CONFLICT (attempt_status_name) DO NOTHING;

INSERT INTO error_types (error_type_name) VALUES
    ('page_not_found'),
    ('disambiguation_error'),
    ('network_timeout'),
    ('api_rate_limit'),
    ('invalid_language_code'),
    ('content_too_short'),
    ('access_denied_error'),
    ('server_error'),
    ('parsing_error'),
    ('encoding_error'),
    ('unknown_error')
ON CONFLICT (error_type_name) DO NOTHING;

INSERT INTO validation_statuses (validation_status_name) VALUES
    ('valid'),
    ('invalid'),
    ('pending'),
    ('warning'),
    ('skipped')
ON CONFLICT (validation_status_name) DO NOTHING;

INSERT INTO collected_content_types (collected_content_type_name) VALUES
    ('wikipedia_page')
ON CONFLICT (collected_content_type_name) DO NOTHING;

INSERT INTO run_types (run_type_name) VALUES
    ('full_collection'),
    ('incremental_update'),
    ('validation_run'),
    ('retry_failed'),
    ('save_at_end_collection'),
    ('incremental_save_collection')
ON CONFLICT (run_type_name) DO NOTHING;

INSERT INTO run_statuses (run_status_name) VALUES
    ('completed'),
    ('failed'),
    ('running'),
    ('cancelled'),
    ('paused')
ON CONFLICT (run_status_name) DO NOTHING;

INSERT INTO content_metadata_schemas (content_metadata_schema) VALUES
    ('{
        "type": "object",
        "title": "Title of Wikipedia Article",
        "required": ["page_id", "language", "title"],

        "properties": {

            "page_id": {
                "type": "integer",
                "description": "Wikipedia page ID"
            },

            "language": {
                "type": "string",
                "pattern": "^[a-z]{2}$",
                "description": "Language code (ISO 639-1)"
            },

            "title": {
                "type": "string",
                "minLength": 1,
                "description": "Article Title"
            },

            "categories": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Wikipedia categories"
            },

            "links": {
                "type": "array",
                "items": {"type": "string"},
                "description": "Internal Wikipedia Links"
            },

            "word_count": {
                "type": "integer",
                "minimum": 0,
                "description": "Word count of collected data"
            },

            "last_modified": {
                "type": "string",
                "format": "date-time",
                "description": "Last modification date of Wikipedia page" 
            }

        }
    
    }')
    ON CONFLICT DO NOTHING;



-- Function to get collector_name_id by its name
CREATE OR REPLACE FUNCTION get_collector_id(collector_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM collector_names WHERE collector_name = collector_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get collection_type_id from its name
CREATE OR REPLACE FUNCTION get_collection_type_id(collection_type_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM collection_types WHERE collection_type = collection_type_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get attempt_status_id by name
CREATE OR REPLACE FUNCTION get_attempt_status_id(attempt_status_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM attempt_statuses WHERE attempt_status_name = attempt_status_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get error_type_id by name
CREATE OR REPLACE FUNCTION get_error_type_id(error_type_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM error_types WHERE error_type_name = error_type_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get validation_status_id by name
CREATE OR REPLACE FUNCTION get_validation_status_id(validation_status_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM validation_statuses WHERE validation_status_name = validation_status_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get collected_content_type_id by name
CREATE OR REPLACE FUNCTION get_content_type_id(content_type_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM collected_content_types WHERE collected_content_type_name = content_type_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get run_type_id by name
CREATE OR REPLACE FUNCTION get_run_type_id(run_type_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM run_types WHERE run_type_name = run_type_name_param);
END;
$$ LANGUAGE plpgsql;

-- Function to get run_status_id by name
CREATE OR REPLACE FUNCTION get_run_status_id(run_status_name_param TEXT)
RETURNS INTEGER AS $$
BEGIN
    RETURN (SELECT id FROM run_statuses WHERE run_status_name = run_status_name_param);
END;
$$ LANGUAGE plpgsql;