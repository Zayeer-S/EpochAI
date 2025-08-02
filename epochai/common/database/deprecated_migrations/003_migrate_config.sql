-- moves collection stuff from config.yml, doesn't move political_events_template (planning to deprecate it and replace with important_persons)

-- Politicians
INSERT INTO collection_configs (
    collector_name_id, 
    collection_type_id, 
    language_code, 
    collection_name, 
    is_collected
) VALUES 
    -- English
    (get_collector_id('wikipedia_collector'), get_collection_type_id('politician'), 'en', 'Donald Trump Jr.', FALSE),
    (get_collector_id('wikipedia_collector'), get_collection_type_id('politician'), 'en', 'Sir Keir Starmer', FALSE),
    (get_collector_id('wikipedia_collector'), get_collection_type_id('politician'), 'en', 'Emmanuel Macron', FALSE),

    -- French
    (get_collector_id('wikipedia_collector'), get_collection_type_id('politician'), 'fr', "Présidence d'Emmanuel Macron", FALSE)

ON CONFLICT (collector_name_id, collection_type_id, language_code, collection_name) DO NOTHING;

-- Political Topics
INSERT INTO collection_configs (
    collector_name_id, 
    collection_type_id, 
    language_code, 
    collection_name, 
    is_collected
) VALUES 
    -- English
    (get_collector_id('wikipedia_collector'), get_collection_type_id('political_topics'), 'en', '2024 United States presidential election', FALSE),
    (get_collector_id('wikipedia_collector'), get_collection_type_id('political_topics'), 'en', 'United Kingdom general election 2024', FALSE),
    (get_collector_id('wikipedia_collector'), get_collection_type_id('political_topics'), 'en', '2024 French legislative election', FALSE),
    
    -- French
    (get_collector_id('wikipedia_collector'), get_collection_type_id('political_topics'), 'fr', 'Élections législatives françaises de 2024', FALSE)

ON CONFLICT (collector_name_id, collection_type_id, language_code, collection_name) DO NOTHING;