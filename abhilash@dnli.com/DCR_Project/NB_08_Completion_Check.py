# Databricks notebook source
# Below logic checks the multiple attributes and their statuses in attribute logs table and marks control tower table accordingly for that dcr

# COMMAND ----------

from pyspark.sql import functions as F

# ── Load Tables ──────────────────────────────────────────────────────────────
control_tower = spark.table("com_edp_dev.com_raw.dcr_control_tower")
attribute_log = spark.table("com_edp_dev.com_raw.dcr_attribute_log")

# ── Aggregate attribute_log per source_crm_id ────────────────────────────────
agg = attribute_log.groupBy("source_crm_id").agg(
    F.count("*").alias("total"),
    F.sum(F.when(F.col("status") == "MERGED",           1).otherwise(0)).alias("merged"),
    F.sum(F.when(F.col("status") == "Steward_Rejected", 1).otherwise(0)).alias("rejected"),
)

# ── Derive new status ─────────────────────────────────────────────────────────
agg = agg.withColumn("new_status",
    F.when(F.col("merged")   == F.col("total"),                    "CLOSED")
     .when(F.col("rejected") == F.col("total"),                    "REJECTED")
     .when(F.col("merged") + F.col("rejected") == F.col("total"),  "PARTIALLY_CLOSED")
     .otherwise(None)
)

# ── DEBUG: Check derived statuses before applying ────────────────────────────
print(">>> Derived statuses from attribute_log:")
agg.filter(F.col("new_status").isNotNull()).show(truncate=False)

# ── DEBUG: Check join result ──────────────────────────────────────────────────
joined = control_tower.join(
    agg.select("source_crm_id", "new_status"), on="source_crm_id", how="left"
)
print(">>> Joined result (rows where new_status is not null):")
joined.filter(F.col("new_status").isNotNull()) \
      .select("source_crm_id", "status", "new_status") \
      .show(truncate=False)

# ── Apply new status ──────────────────────────────────────────────────────────
updated = joined \
    .withColumn("status", F.coalesce(F.col("new_status"), F.col("status"))) \
    .drop("new_status")

# ── Write back (comment this out until debug looks correct!) ─────────────────
updated.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable("com_edp_dev.com_raw.dcr_control_tower")

print(">>> Done! Control tower updated.")

