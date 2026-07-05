CREATE TABLE IF NOT EXISTS kafka_messages (
    id SERIAL PRIMARY KEY,
    original_message TEXT,
    transformed_message TEXT,
    processed_at TIMESTAMP,
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS agent_results (
    id SERIAL PRIMARY KEY,
    original_message TEXT,
    agent_analysis TEXT,
    analyzed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);