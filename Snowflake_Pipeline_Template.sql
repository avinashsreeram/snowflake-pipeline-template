-- Snowflake Data Mart Setup Script (Generalized Template)
-- This script prepares database objects from staging to consumption-ready models

--------------------------------------------------------------------
-- 0. Setup Environment (Replace with your actual usernames/roles)
--------------------------------------------------------------------
CREATE USER my_admin_user;
SELECT CURRENT_ROLE();

CREATE DATABASE IF NOT EXISTS plant_maintenance_dm;
USE DATABASE plant_maintenance_dm;

-- Schemas: Organized by layer
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS mart;
CREATE SCHEMA IF NOT EXISTS analytics;
CREATE SCHEMA IF NOT EXISTS shared;


--------------------------------------------------------------------
-- 1. Shared File Format Setup
--------------------------------------------------------------------
CREATE OR REPLACE FILE FORMAT shared.ff_csv_std
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 0
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
TRIM_SPACE = TRUE
NULL_IF = ('NULL', 'NULL')
TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF3'
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;


--------------------------------------------------------------------
-- 2. Create Stage (GUI Uploads used for internal stage)
--------------------------------------------------------------------
USE SCHEMA staging;
CREATE OR REPLACE STAGE staging.workorders_stage
FILE_FORMAT = shared.ff_csv_std;

-- Preview contents manually:
-- SELECT $1, $2 FROM @staging.workorders_stage/sample.csv (FILE_FORMAT => 'shared.ff_csv_std') LIMIT 5;


--------------------------------------------------------------------
-- 3. Raw Table Creation and Data Load
--------------------------------------------------------------------
CREATE OR REPLACE TABLE staging.workorders_raw (
    WorkOrder_ID STRING,
    Order_Num STRING,
    Unit_ID STRING,
    Deadline_DT TIMESTAMP_NTZ,
    Complaint STRING,
    Correction STRING,
    Action_Taken STRING,
    WorkOrder_Cost STRING,
    Meter_Value STRING,
    Status_Code STRING,
    WorkOrder_Type STRING,
    Priority_Code STRING,
    Start_DT TIMESTAMP_NTZ,
    Completion_DT TIMESTAMP_NTZ,
    Technician STRING,
    Root_Cause STRING,
    Supervisor STRING,
    Created_DT TIMESTAMP_NTZ,
    Skill_Area STRING,
    Asset_ID STRING,
    Hours_Worked NUMBER(10,2),
    Completion_Pct NUMBER(5,2),
    Department STRING,
    Subdivision STRING,
    Region STRING,
    Client STRING,
    Reporter STRING,
    Meter1_Type STRING,
    Meter1_Value NUMBER(10,2),
    Meter2_Type STRING,
    Equipment_Group STRING,
    Vendor STRING
);

COPY INTO staging.workorders_raw
FROM @staging.workorders_stage
FILE_FORMAT = (FORMAT_NAME = 'shared.ff_csv_std')
ON_ERROR = 'CONTINUE';


--------------------------------------------------------------------
-- 4. Curated Table with Cleaned + Transformed Fields
--------------------------------------------------------------------
USE SCHEMA mart;
CREATE OR REPLACE TABLE mart.workorders_curated AS
SELECT
    CAST(WorkOrder_ID AS VARCHAR) AS WorkOrder_ID,
    TRIM(Order_Num) AS Order_Num,
    TRIM(Unit_ID) AS Unit_ID,
    CAST(Created_DT AS TIMESTAMP) AS Created_DT,
    CAST(Start_DT AS TIMESTAMP) AS Start_DT,
    CAST(Deadline_DT AS TIMESTAMP) AS Deadline_DT,
    CAST(Completion_DT AS TIMESTAMP) AS Completion_DT,
    INITCAP(TRIM(Complaint)) AS Complaint,
    INITCAP(TRIM(Correction)) AS Correction,
    INITCAP(TRIM(Action_Taken)) AS Action_Taken,
    CAST(WorkOrder_Cost AS DECIMAL(12,2)) AS WorkOrder_Cost,
    CAST(Meter_Value AS DECIMAL(12,2)) AS Meter_Value,
    UPPER(TRIM(WorkOrder_Type)) AS WorkOrder_Type,
    UPPER(TRIM(Priority_Code)) AS Priority_Code,
    INITCAP(TRIM(Root_Cause)) AS Root_Cause,
    INITCAP(TRIM(Technician)) AS Technician,
    INITCAP(TRIM(Supervisor)) AS Supervisor,
    INITCAP(TRIM(Client)) AS Client,
    INITCAP(TRIM(Reporter)) AS Reporter,
    UPPER(TRIM(Skill_Area)) AS Skill_Area,
    TRIM(Asset_ID) AS Asset_ID,
    CAST(Hours_Worked AS DECIMAL(8,2)) AS Hours_Worked,
    CAST(Completion_Pct AS DECIMAL(5,2)) AS Completion_Pct,
    INITCAP(TRIM(Department)) AS Department,
    INITCAP(TRIM(Subdivision)) AS Subdivision,
    INITCAP(TRIM(Region)) AS Region,
    INITCAP(TRIM(Meter1_Type)) AS Meter1_Type,
    CAST(Meter1_Value AS DECIMAL(12,2)) AS Meter1_Value,
    INITCAP(TRIM(Meter2_Type)) AS Meter2_Type,
    INITCAP(TRIM(Equipment_Group)) AS Equipment_Group,
    INITCAP(TRIM(Vendor)) AS Vendor,
    TRIM(Status_Code) AS Status_Code,
    CASE TRIM(Status_Code)
         WHEN '99' THEN 'Completed'
         WHEN '91' THEN 'Cancelled'
         ELSE 'In Progress'
    END AS Status_Label
FROM staging.workorders_raw;


--------------------------------------------------------------------
-- 5. Analytics Layer – KPIs / Views
--------------------------------------------------------------------
USE SCHEMA analytics;

-- KPI 1: Total WorkOrder Spend by Unit
CREATE OR REPLACE VIEW analytics.vw_workorder_spend_by_unit AS
SELECT Unit_ID, SUM(WorkOrder_Cost) AS Total_Spend
FROM mart.workorders_curated
GROUP BY Unit_ID;

-- KPI 2: Avg. Labor Hours by Skill
CREATE OR REPLACE VIEW analytics.vw_avg_hours_by_skill AS
SELECT Skill_Area, AVG(Hours_Worked) AS Avg_Hours
FROM mart.workorders_curated
WHERE Status_Label = 'Completed'
GROUP BY Skill_Area;

-- KPI 3: Technician Workload Summary
CREATE OR REPLACE VIEW analytics.vw_technician_load AS
SELECT
    Technician,
    SUM(CASE WHEN Status_Label = 'In Progress' THEN 1 ELSE 0 END) AS Open_Count,
    SUM(CASE WHEN Status_Label = 'Completed' THEN 1 ELSE 0 END) AS Completed_Count
FROM mart.workorders_curated
WHERE Status_Label != 'Cancelled'
GROUP BY Technician;

-- KPI 4: Distribution by Status
CREATE OR REPLACE VIEW analytics.vw_status_summary AS
SELECT Status_Label, COUNT(*) AS WorkOrder_Count
FROM mart.workorders_curated
GROUP BY Status_Label;


--------------------------------------------------------------------
-- 6. Optional: Data Dictionary View
--------------------------------------------------------------------
USE SCHEMA shared;
CREATE OR REPLACE VIEW shared.vw_data_dictionary AS
SELECT * FROM VALUES
    ('mart.workorders_curated', 'WorkOrder_ID', 'VARCHAR', 'Unique ID'),
    ('mart.workorders_curated', 'Order_Num', 'STRING', 'System order reference'),
    ('mart.workorders_curated', 'Unit_ID', 'STRING', 'Asset/Unit ID'),
    ('mart.workorders_curated', 'Created_DT', 'TIMESTAMP', 'Creation time'),
    ('mart.workorders_curated', 'Start_DT', 'TIMESTAMP', 'Work start time'),
    ('mart.workorders_curated', 'Deadline_DT', 'TIMESTAMP', 'SLA deadline'),
    ('mart.workorders_curated', 'Completion_DT', 'TIMESTAMP', 'Marked completion time'),
    ('mart.workorders_curated', 'Complaint', 'STRING', 'Issue reported'),
    ('mart.workorders_curated', 'Correction', 'STRING', 'What was corrected'),
    ('mart.workorders_curated', 'WorkOrder_Cost', 'DECIMAL', 'Total workorder cost'),
    ('mart.workorders_curated', 'Status_Label', 'STRING', 'Status category: Completed, Cancelled, In Progress')
AS data_dictionary(table_name, column_name, data_type, description);


--------------------------------------------------------------------
-- 7. Fact + Dimension Tables for Power BI Modeling
--------------------------------------------------------------------
-- dim_asset
CREATE OR REPLACE TABLE mart.dim_asset AS
SELECT DISTINCT Asset_ID, Equipment_Group, Vendor, Region, Subdivision, Department
FROM mart.workorders_curated
WHERE Asset_ID IS NOT NULL;

-- dim_employee
CREATE OR REPLACE TABLE mart.dim_employee AS
SELECT DISTINCT Technician, Supervisor, Reporter
FROM mart.workorders_curated
WHERE Technician IS NOT NULL;

-- dim_client
CREATE OR REPLACE TABLE mart.dim_client AS
SELECT DISTINCT Client FROM mart.workorders_curated
WHERE Client IS NOT NULL;

-- dim_status
CREATE OR REPLACE TABLE mart.dim_status AS
SELECT DISTINCT Status_Code, Status_Label FROM mart.workorders_curated
WHERE Status_Code IS NOT NULL;

-- fact_workorders
CREATE OR REPLACE TABLE mart.fact_workorders AS
SELECT
    WorkOrder_ID,
    Order_Num,
    Unit_ID,
    Asset_ID,
    Technician,
    Client,
    Status_Code,
    CAST(Created_DT AS DATE) AS Created_Date,
    CAST(Start_DT AS DATE) AS Start_Date,
    CAST(Deadline_DT AS DATE) AS SLA_Deadline,
    CAST(Completion_DT AS DATE) AS Closed_Date,
    WorkOrder_Type,
    Priority_Code,
    Skill_Area,
    Hours_Worked,
    WorkOrder_Cost,
    Meter1_Value,
    Completion_Pct
FROM mart.workorders_curated;
