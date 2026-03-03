CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    weekday INT
);

CREATE TABLE dim_station (
    station_id SERIAL PRIMARY KEY,
    station_name VARCHAR(255),
    state VARCHAR(100),
    city VARCHAR(100),
    location_lat FLOAT,
    location_long FLOAT
);

CREATE TABLE dim_fuel_type (
    fuel_type_id SERIAL PRIMARY KEY,
    fuel_name VARCHAR(50),
    fuel_description VARCHAR(255)
);

CREATE TABLE fact_fuel_price (
    fuel_id SERIAL PRIMARY KEY,
    date_id INT REFERENCES dim_date(date_id),
    station_id INT REFERENCES dim_station(station_id),
    fuel_type_id INT REFERENCES dim_fuel_type(fuel_type_id),
    price DECIMAL(10,2),
    volume_sold DECIMAL(10,2)
);