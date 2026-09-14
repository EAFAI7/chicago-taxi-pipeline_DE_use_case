CREATE TABLE IF NOT EXISTS taxi_monthly_performance (
    year_month VARCHAR(7) NOT NULL,
    taxi_id VARCHAR(255) NOT NULL,
    total_trips BIGINT,
    total_revenue NUMERIC(18, 2),
    avg_trip_duration_minutes NUMERIC(12, 2),
    avg_trip_distance_km NUMERIC(12, 2)
);