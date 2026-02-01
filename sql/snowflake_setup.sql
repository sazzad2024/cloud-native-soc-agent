-- 1. Create Database and Schema
CREATE DATABASE IF NOT EXISTS SOC_DATA;
USE DATABASE SOC_DATA;
USE SCHEMA PUBLIC;

-- 2. Create Warehouse (Compute)
-- Snowflake usually comes with 'COMPUTE_WH' by default, but ensuring we have one.
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH WITH WAREHOUSE_SIZE = 'XSMALL' AUTO_SUSPEND = 60 AUTO_RESUME = TRUE;

-- 3. Create Users Table (Dimension)
CREATE OR REPLACE TABLE DIM_USERS (
    user_id INT PRIMARY KEY,
    email VARCHAR(100),
    department VARCHAR(50),
    risk_score INT
);

-- 4. Create Alerts Table (Fact)
CREATE OR REPLACE TABLE FACT_ALERTS (
    alert_id INT PRIMARY KEY,
    src_ip VARCHAR(50),
    dst_port INT,              -- [NEW] Target Port (e.g., 80, 443, 22)
    protocol INT,              -- [NEW] Protocol ID (6=TCP, 17=UDP)
    attack_cat VARCHAR(50),
    severity VARCHAR(20),
    user_id INT,
    flow_duration INT,         -- [NEW] How long the connection lasted
    total_bytes INT,           -- [NEW] Size of the payload
    timestamp TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 5. Seed Data: User Directory
-- 'Alice' (User 1) - The victim of the brute force
INSERT INTO DIM_USERS (user_id, email, department, risk_score) 
VALUES (1, 'alice@company.com', 'HR', 10);

-- 'Bob' (User 2) - An IT admin
INSERT INTO DIM_USERS (user_id, email, department, risk_score) 
VALUES (2, 'bob@company.com', 'IT', 25);

-- 6. Seed Data: Recent Alerts
-- Simulate the 'Exploits' / 'Brute Force' attack we are investigating
INSERT INTO FACT_ALERTS (alert_id, src_ip, dst_port, protocol, attack_cat, severity, user_id, flow_duration, total_bytes) 
VALUES (1001, '192.168.1.105', 443, 6, 'Exploits', 'High', 1, 12000, 500200);

-- Simulate a 'Reconnaissance' Scan
INSERT INTO FACT_ALERTS (alert_id, src_ip, dst_port, protocol, attack_cat, severity, user_id, flow_duration, total_bytes) 
VALUES (1002, '10.0.0.5', 22, 6, 'Reconnaissance', 'Medium', 2, 500, 1200);

-- Verification
SELECT * FROM DIM_USERS;
SELECT * FROM FACT_ALERTS;
