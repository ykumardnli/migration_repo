# Databricks notebook source
# Loading all tables in df's for further processing

df_dcr = spark.table("com_edp_dev.com_raw.dcr_control_tower")
df_dcrl = spark.table("com_edp_dev.com_raw.dcr_attribute_log")
df_dcr_routing = spark.table("com_edp_dev.com_raw.dcr_routing_rules")

# COMMAND ----------

# df = spark.read.option("header", True).csv("/Volumes/com_edp_dev/com_raw/data/dcr_temp_storage/dcr_routing_rules.csv")
# #display(df)
# df.write.saveAsTable("com_edp_dev.com_raw.dcr_routing_rules")

# COMMAND ----------

# %sql
# select * from com_edp_dev.com_raw.dcr_routing_rules;
# --drop table com_edp_dev.com_raw.dcr_routing_rules;

# COMMAND ----------

from pyspark.sql.functions import col

# --------------------------------------------------------
# LOGIC:
# 1. Get only required columns from df_dcr
# 2. Join with df_dcrl to bring entity_type
# 3. Join with routing table based on entity_type + attribute_name
# --------------------------------------------------------

# Step 1: Select required columns from DCR
df_sel_dcr = df_dcr.select("source_crm_id", "entity_type").alias("dcr")

# Step 2: Alias DCRL
df_dcrl_alias = df_dcrl.alias("dcrl")

# Step 3: Join DCRL + DCR
df_joined = df_dcrl_alias.join(
    df_sel_dcr, col("dcrl.source_crm_id") == col("dcr.source_crm_id"), "left"
).select(
    "dcrl.*", col("dcr.entity_type")  # all columns from DCRL  # bring entity_type
)

# Step 4: Alias routing table
df_routing_alias = df_dcr_routing.alias("rt")

# Step 5: Join with routing table (correct syntax)
df_master_joined = df_joined.alias("j").join(
    df_routing_alias,
    (col("j.entity_type") == col("rt.dcr_entity_type"))
    & (col("j.attribute_name") == col("rt.dcrl_field_api_name")),
    "left",
)

df_master_joined = df_master_joined.withColumn(
    "status", col("routing_classification")
).select(
    "dcr_id",
    "source_crm_id",
    "attribute_name",
    "old_value",
    "new_value",
    "status",
    "steward_comment",
    "updated_at",
)
# Ingest back the new details in dcr line table
df_master_joined.write.mode("overwrite").saveAsTable(
    "com_edp_dev.com_raw.dcr_attribute_log"
)

display(spark.table("com_edp_dev.com_raw.dcr_attribute_log").filter(col("status").isNull()))
