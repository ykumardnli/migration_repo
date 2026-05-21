# Databricks notebook source
# MAGIC %md
# MAGIC If Steward_Approves the attribute it would be marked as Steward_Approved in table "dcr_steward_review" and those accepted would be merged further in reltio and marked as MERGED in attribute log table, if rejected it would be marked as "Steward_Rejected" in attribute log table
# MAGIC so it can be picked by next process

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from com_edp_dev.com_raw.dcr_steward_review

# COMMAND ----------

# %sql
# --Testing
# Update
#   com_edp_dev.com_raw.dcr_steward_review
# set
#   status = "Steward_Approved"
# where
#   dcr_id in ("DCRL000000002", "DCRL000000004");

# Update
#   com_edp_dev.com_raw.dcr_steward_review
# set
#   status = "Steward_Rejected"
# where
#   dcr_id in ("DCRL000000005", "DCRL000000006");

# COMMAND ----------

'''
Merge logic to send steward approved would come here

Once done those attributes would be marked as MERGED in main table

'''

# COMMAND ----------

df_steward_approved = spark.table("com_edp_dev.com_raw.dcr_steward_review").filter("status = 'Steward_Approved'")
df_steward_approved.createOrReplaceTempView("df_steward_approved")

spark.sql("""
    UPDATE com_edp_dev.com_raw.dcr_attribute_log
    SET STATUS = 'MERGED'
    WHERE dcr_id IN (SELECT dcr_id FROM df_steward_approved)
""")

# ----------------------------------------------------------------------------------------------------------------
# Rejected Ones will be marked as Steward_Rejected

df_steward_rejected = spark.table("com_edp_dev.com_raw.dcr_steward_review").filter("status = 'Steward_Rejected'")
df_steward_rejected.createOrReplaceTempView("df_steward_rejected")

spark.sql("""
    UPDATE com_edp_dev.com_raw.dcr_attribute_log
    SET STATUS = 'Steward_Rejected'
    WHERE dcr_id IN (SELECT dcr_id FROM df_steward_rejected)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --Testing
# MAGIC select * from com_edp_dev.com_raw.dcr_attribute_log
# MAGIC where dcr_id in ("DCRL000000002", "DCRL000000004", "DCRL000000005", "DCRL000000006");
