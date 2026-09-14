CREATE TABLE IF NOT EXISTS company_daily_performance (
    trip_date DATE NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    company VARCHAR(255) NOT NULL,
    total_trips BIGINT,
    total_revenue NUMERIC(18, 2),
    avg_trip_duration_minutes NUMERIC(12, 2),
    avg_trip_distance_km NUMERIC(12, 2)
);