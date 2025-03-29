# ❄️ Snowflake Data Pipeline: Work Order Analytics

This project builds a modular data pipeline in **Snowflake** that ingests, transforms, and models work order data for analytics use cases in Power BI.

---

## Architecture

**Layers:**
- `staging`: Raw data directly loaded from CSV
- `mart`: Cleaned and transformed tables for reporting
- `analytics`: Pre-computed KPIs exposed via SQL views
- `shared`: Common reusable objects like file formats and metadata views

---

## Components

### 1. Staging
- Internal stage (`staging.workorders_stage`) created manually via UI
- Raw table: `staging.workorders_raw`

### 2. Curated / Mart
- Table: `mart.workorders_curated`
- Fields cleaned, standardized, typed (timestamps, decimals, etc.)
- Status codes mapped to labels (e.g., `99` = Completed)

### 3. Analytics (Consumption Views)
- `vw_workorder_spend_by_unit`: Total work order spend per asset
- `vw_avg_hours_by_skill`: Average hours per skill category
- `vw_technician_load`: Open vs. closed work by technician
- `vw_status_summary`: Distribution of work order statuses

### 4. Dimensions and Facts (Star Schema)
- `dim_asset`, `dim_employee`, `dim_status`, `dim_client`
- `fact_workorders`: Fact table for Power BI modeling

---

##  Power BI Usage

Connect Power BI to the **analytics schema** and use:
- Pre-built KPIs for dashboards
- Fact/dimension tables for custom slicing/filtering

Suggested relationships:
- `fact_workorders` ⇄ `dim_asset`, `dim_employee`, `dim_status`, `dim_client`

---

## Extras

- `shared.vw_data_dictionary`: Lightweight metadata view for BI and documentation
- All SQL objects created with idempotent `CREATE OR REPLACE`

---

## Setup (If Using This as Template)

1. Upload your CSVs into Snowflake internal stage via UI
2. Run `snowflake_pipeline_template.sql` in Snowflake worksheet
3. Connect Power BI to Snowflake
4. Import views or model custom facts/dimensions as needed

---

## Author

Built by a Avinash passionate about modular pipelines, layered architecture, and clean BI handoffs.

