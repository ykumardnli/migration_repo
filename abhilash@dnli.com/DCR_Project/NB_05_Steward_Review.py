# Databricks notebook source
from pyspark.sql.functions import col

df_steward_review = spark.table("com_edp_dev.com_raw.dcr_attribute_log")
df_steward_review = df_steward_review.filter(col("status") == "Steward_Review")
display(df_steward_review)

# COMMAND ----------

# df_steward_review.write.mode("overwrite").saveAsTable("com_edp_dev.com_raw.dcr_steward_review")

# COMMAND ----------

df_steward_review.createOrReplaceTempView("df_steward_review_view")

spark.sql("""
    UPDATE com_edp_dev.com_raw.dcr_attribute_log
    SET STATUS = 'PENDING_STEWARD'
    WHERE dcr_id IN (SELECT dcr_id FROM df_steward_review_view)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from com_edp_dev.com_raw.dcr_attribute_log;

