# Databricks notebook source
# MAGIC %sql
# MAGIC select PAYER_ID,PAYER_NAME, MAX(ELAPRASE_PT_LT_17
# MAGIC ) AS Elaprase_pt, MAX(elaprase_claims_lt_17
# MAGIC ) AS Elaprase_Claims, MAX(elaprase_denial_lt_17) as rate
# MAGIC from cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC GROUP BY PAYER_ID, PAYER_NAME
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC cmpa_insights_internal_schema.tableau_payer360_master

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC cmpa_insights_internal_schema.hco_360
# MAGIC WHERE hco_name = 'Cincinnati Childrens'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT SUM(ELAPRASE_REJECTION_RATE),SUM(elaprase_denial_rate) FROM 
# MAGIC cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC WHERE PAYER_NAME = 'Cigna/ESI'

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT payer_name,total_lives FROM
# MAGIC (SELECT PAYER_NAME,TOTAL_LIVES, RANK() OVER (PARTITION BY PAYER_NAME ORDER BY TOTAL_LIVES desc) AS rnk
# MAGIC FROM cmpa_insights_internal_schema.tableau_payer360_master)
# MAGIC WHERE rnk = 1 
# MAGIC  

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT MAX(TOTAL_LIVES), MAX(ELAPRASE_PT_LT_17), MAX(TOTAL_ELAPRASE_PATIENTS), MAX(elaprase_hco), MAX(elaprase_hcp), median(elaprase_denial_rate), MAX(elaprase_claims_ct), max(elaprase_commercial_pt_ct), max(elaprase_medicare_pt_ct), max(elaprase_medicaid_pt_ct), max(elaprase_other_pt_ct), max(elaprase_age_lt_5_yrs), max(elaprase_age_5_to_10_yrs), max(elaprase_age_11_to_16_yrs),max(elaprase_age_17ngrt_yrs)
# MAGIC FROM cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC WHERE PAYER_NAME = 'All Payers' AND PARENT_NAME = "All Parents"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT avg(elaprase_denial_rate) FROM 
# MAGIC cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC WHERE PAYER_NAME = 'All Payers' and PARENT_NAME = 'All Parents'

# COMMAND ----------

# MAGIC %sql
# MAGIC Select median(elaprase_denial_rate)
# MAGIC FROM cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC WHERE territory_name = "All Territories"
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM
# MAGIC com_edp_prd.cmpa_insights_internal_schema.tableau_payer360_master
# MAGIC WHERE PARENT_NAME = 'CIN'

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     period_month,
# MAGIC     period_start_dt,
# MAGIC     SUM(metric_value) AS patient_count
# MAGIC FROM com_edp_prd.cmpa_insights_internal_schema.vw_rpt_dashboard_trend_data
# MAGIC WHERE
# MAGIC     product_name = 'AVLAYAH'
# MAGIC     AND metric_type = 'PATIENT_COUNT'
# MAGIC     AND time_period = 'MONTHLY'
# MAGIC GROUP BY period_month, period_start_dt
# MAGIC ORDER BY period_start_dt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     period_month,
# MAGIC     period_start_dt,
# MAGIC     SUM(metric_value) AS vial_count
# MAGIC FROM com_edp_prd.cmpa_insights_internal_schema.vw_rpt_dashboard_trend_data
# MAGIC WHERE
# MAGIC     product_name = 'AVLAYAH'
# MAGIC     AND metric_type = 'VIAL_COUNT'
# MAGIC     AND time_period = 'MONTHLY'
# MAGIC GROUP BY period_month, period_start_dt
# MAGIC ORDER BY period_start_dt;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC com_edp_prd.cmpa_insights_internal_schema.patient360_master
# MAGIC WHERE PATIENT_AGE <17 AND primary_hcp_hco_name_2yr = "Children's Hospital of Philadelphia"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC com_edp_prd.cmpa_insights_internal_schema.patient360_master
# MAGIC WHERE PATIENT_AGE >= 17 AND primary_hcp_hco_name_2yr = "Children's Hospital of Philadelphia"

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT *
# MAGIC FROM 
# MAGIC com_edp_prd.cmpa_insights_internal_schema.patient_360_hco_table
# MAGIC WHERE hco_name = "Cincinnati Childrens"
# MAGIC

# COMMAND ----------

workspace_url = spark.conf.get(
    "spark.databricks.workspaceUrl"
)

notebook_path = (
    dbutils.notebook.entry_point
    .getDbutils()
    .notebook()
    .getContext()
    .notebookPath()
    .get()
)

print("Workspace:", workspace_url)
print("Path:", notebook_path)

# COMMAND ----------

# Check existing shares in PRD
spark.sql("SHOW SHARES").show(truncate=False)

# COMMAND ----------

# Check existing recipients
spark.sql("SHOW RECIPIENTS").show(truncate=False)

# COMMAND ----------

# Check metastore details in PRD
spark.sql("SELECT * FROM system.information_schema.metastores").show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Delta Sharing Setup Overview
# =============================================================
# DELTA SHARING SETUP: DEV → PRD (Internal, same metastore)
# =============================================================
# Since com_edp_dev is NOT accessible from this PRD workspace,
# you must run STEP 1 from the DEV workspace, then STEP 2 here.
#
# ─── STEP 1: RUN IN DEV WORKSPACE ───────────────────────────
# (Create a share and grant PRD workspace access)
#
# -- 1a. Create a share in the DEV workspace
# CREATE SHARE IF NOT EXISTS cmpa_dev_to_prd_share;
#
# -- 1b. Add the schema (or individual tables) to the share
# ALTER SHARE cmpa_dev_to_prd_share
#   ADD SCHEMA com_edp_dev.cmpa_insights_internal_schema;
#
# -- 1c. Create a recipient for the PRD workspace (internal = same metastore)
# CREATE RECIPIENT IF NOT EXISTS prd_workspace_recipient;
#
# -- 1d. Grant the recipient access to the share
# GRANT SELECT ON SHARE cmpa_dev_to_prd_share TO RECIPIENT prd_workspace_recipient;
#
# ─── STEP 2: RUN IN THIS PRD WORKSPACE (below) ──────────────
# After Step 1 is done in DEV, run the cells below to create
# a catalog from the share so you can query DEV data from PRD.
# =============================================================

print("See STEP 1 comments above — run those in the DEV workspace first.")
print("Then proceed to the next cell for STEP 2 (consume the share here in PRD).")

# COMMAND ----------

# Run in PRD workspace
dbutils.fs.put(
    "/Volumes/com_edp_dev/cmpa_insights_internal_schema/migration_volume/test.txt",
    "hello from PRD",
    overwrite=True
)
print("✓ PRD can write to DEV Volume!")

# COMMAND ----------

# DBTITLE 1,Step 2 - Consume Share in PRD
# =============================================================
# STEP 2: RUN IN PRD (this workspace) — Consume the share
# =============================================================
# After the DEV admin has created the share and recipient,
# create a foreign catalog here pointing to the shared data.

# 2a. Create a catalog from the share (makes DEV data queryable in PRD)
spark.sql("""
    CREATE CATALOG IF NOT EXISTS com_edp_dev_shared
    USING SHARE `denali`.`cmpa_dev_to_prd_share`
""")
print("✓ Created catalog 'com_edp_dev_shared' from DEV share")

# 2b. Verify you can now query DEV data from PRD
df = spark.sql("SHOW SCHEMAS IN com_edp_dev_shared")
df.show(truncate=False)

# 2c. Query a table (adjust table name as needed)
# spark.sql("SELECT * FROM com_edp_dev_shared.cmpa_insights_internal_schema.<table_name> LIMIT 10").show()
