# Databricks notebook source
from pyspark.sql.functions import col

df_Auto_Approve = spark.table(
    "com_edp_dev.com_raw.dcr_attribute_log"
)
df_Auto_Approve = df_Auto_Approve.filter(col("status") == "Auto_Approve")

display(df_Auto_Approve)

# COMMAND ----------

# need to add logic to further add this rows into reltio/vod and send via API



# COMMAND ----------

# Mark those attributes as Merged in attribute logs table once updated in Reltio/VOD

df_Auto_Approve.createOrReplaceTempView("df_auto_approve_view")

spark.sql("""
    UPDATE com_edp_dev.com_raw.dcr_attribute_log
    SET STATUS = 'MERGED'
    WHERE dcr_id IN (SELECT dcr_id FROM df_auto_approve_view)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC --select * from com_edp_dev.com_raw.dcr_attribute_log;

