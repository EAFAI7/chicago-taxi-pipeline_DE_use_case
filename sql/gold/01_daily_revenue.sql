CREATE TABLE IF NOT EXISTS daily_revenue (
    trip_date DATE NOT NULL,
    year_month VARCHAR(7) NOT NULL,
    total_revenue NUMERIC(18, 2),
    total_trips BIGINT
);