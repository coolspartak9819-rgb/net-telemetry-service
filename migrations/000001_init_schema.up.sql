CREATE TABLE IF NOT EXISTS device_telemetry_history (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    cpu_usage VARCHAR(20) NOT NULL,
    memory_usage VARCHAR(20) NOT NULL,
    status VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_device_timestamp ON device_telemetry_history(device_id, timestamp DESC);
