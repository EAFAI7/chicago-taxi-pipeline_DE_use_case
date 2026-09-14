CREATE TABLE IF NOT EXISTS daily_average_trip_duration (
    trip_date DATE NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    avg_trip_duration_minutes NUMERIC(12, 2)
);