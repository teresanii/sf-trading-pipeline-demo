USE ROLE ACCOUNTADMIN;

-- Create database and schemas
CREATE DATABASE IF NOT EXISTS TRADING_ANALYTICS;
USE DATABASE TRADING_ANALYTICS;

CREATE SCHEMA IF NOT EXISTS RAW_DATA;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

USE SCHEMA RAW_DATA;

-- Create stage for CSV files
CREATE STAGE IF NOT EXISTS CSV_STAGE;

-- Create file format for CSV files
CREATE OR REPLACE FILE FORMAT CSV_FORMAT
TYPE = CSV
PARSE_HEADER = TRUE
SKIP_HEADER = 1
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE; -- Important for schema evolution

-- ==============================
-- USER PROFILES TABLE
-- ==============================
CREATE TABLE IF NOT EXISTS USER_PROFILES (
    user_id VARCHAR(50) PRIMARY KEY,
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    registration_date DATE,
    account_type VARCHAR(50),
    tier VARCHAR(50),
    country VARCHAR(100),
    preferred_exchange VARCHAR(50),
    kyc_status VARCHAR(50),
    phone VARCHAR(50),
    date_of_birth DATE,
    address TEXT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _file_name VARCHAR(255)
);

-- ==============================
-- ORDER BOOK DATA 
-- ==============================
CREATE TABLE IF NOT EXISTS ORDER_BOOK (
    id BIGINT AUTOINCREMENT PRIMARY KEY,
    timestamp TIMESTAMP,
    symbol VARCHAR(20),
    side VARCHAR(10), -- BID or ASK
    price FLOAT,
    quantity FLOAT,
    exchange VARCHAR(50),
    level_num INT, -- order book level (1 = best bid/ask)
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _file_name VARCHAR(255)
);

-- ==============================
-- USER TRADES TABLE 
-- ==============================
CREATE TABLE IF NOT EXISTS USER_TRADES (
    id BIGINT AUTOINCREMENT PRIMARY KEY,
    trade_id VARCHAR(50),
    user_id VARCHAR(50),
    symbol VARCHAR(20),
    side VARCHAR(10), -- BUY or SELL
    quantity FLOAT,
    price FLOAT,
    timestamp TIMESTAMP,
    status VARCHAR(20),
    exchange VARCHAR(20),
    order_type VARCHAR(20),
    fees FLOAT,
    settlement_date DATE,
    trade_value FLOAT,
    _loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    _file_name VARCHAR(255)
);

ALTER TABLE USER_TRADES SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE ORDER_BOOK SET ENABLE_SCHEMA_EVOLUTION = TRUE;
ALTER TABLE USER_PROFILES SET ENABLE_SCHEMA_EVOLUTION = TRUE;

-- Show created tables
SHOW TABLES;

-- Now create a warehouse
CREATE OR REPLACE WAREHOUSE trading_analytics_wh
    WAREHOUSE_SIZE = 'xsmall'
    WAREHOUSE_TYPE = 'standard'
    AUTO_SUSPEND = 300
    INITIALLY_SUSPENDED = TRUE
    AUTO_RESUME = TRUE;