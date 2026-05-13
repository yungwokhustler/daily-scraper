CREATE TABLE sources (
    id BIGSERIAL PRIMARY KEY,
    channel_id TEXT NOT NULL UNIQUE,
    platform TEXT NOT NULL CHECK (platform IN ('telegram', 'discord')),
    channel_name TEXT NOT NULL
);

CREATE TABLE logs_runs (
    id BIGSERIAL PRIMARY KEY,
    channel_id TEXT NOT NULL,
    platform TEXT NOT NULL,
    pulled INTEGER NOT NULL,
    kept INTEGER NOT NULL,
);

