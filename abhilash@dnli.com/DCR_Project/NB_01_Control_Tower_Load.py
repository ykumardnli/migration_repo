# Databricks notebook source
# MAGIC %md
# MAGIC Below code loads delta from com_edp_prd.com_raw.vcrm_data_change_request__v --> com_edp_prd.com_raw.vcrm_data_change_request_delta

# COMMAND ----------

from pyspark.sql.functions import col, max as max_

# Read full source table
src_df = spark.table('com_edp_dev.com_raw.vcrm_data_change_request__v')
#display(src_df)

# Target table where incremental data gets stored
tgt_table = 'com_edp_dev.com_raw.vcrm_data_change_request_delta'

# Check if target table exists (first run vs incremental run)
if spark.catalog.tableExists(tgt_table):
    
    # Read existing target data
    tgt_df = spark.table(tgt_table)
    
    # Get the latest loaded timestamp (watermark)
    max_date = tgt_df.select(max_("modified_date__v")).collect()[0][0]
    
    if max_date is None:
        # Target is empty → load everything
        print("Target table is empty. Performing full load...")
        new_df = src_df
    else:
        # Pick only records newer than last loaded timestamp
        print(f"Last loaded timestamp in target: {max_date}")
        new_df = src_df.filter(col("modified_date__v") > max_date)

else:
    # Target doesn't exist → first time load
    print("Target table does not exist. Performing full load...")
    new_df = src_df

# Count how many new records are ready to ingest
new_record_count = new_df.count()
print(f"New records found: {new_record_count}")

if new_record_count > 0:
    # Append only new records, existing data stays untouched
    new_df.write.mode("append").saveAsTable(tgt_table)
    print(f"Successfully ingested {new_record_count} new records into '{tgt_table}'")
else:
    # Nothing to load, target is already up to date
    print("No new records to ingest. Target table is already up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC Below code loads delta from com_edp_prd.com_intgr.data_change_request_line --> com_edp_prd.com_intgr.data_change_request_line_delta

# COMMAND ----------

from pyspark.sql.functions import col, max as max_

# Read full source table
src_df = spark.table('com_edp_dev.com_intgr.data_change_request_line')
#display(src_df)

# Target table where incremental data gets stored
tgt_table = 'com_edp_dev.com_raw.data_change_request_line_delta'

# Check if target table exists (first run vs incremental run)
if spark.catalog.tableExists(tgt_table):
    
    # Read existing target data
    tgt_df = spark.table(tgt_table)
    
    # Get the latest loaded timestamp (watermark)
    max_date = tgt_df.select(max_("modified_date__v")).collect()[0][0]
    
    if max_date is None:
        # Target is empty → load everything
        print("Target table is empty. Performing full load...")
        new_df = src_df
    else:
        # Pick only records newer than last loaded timestamp
        print(f"Last loaded timestamp in target: {max_date}")
        new_df = src_df.filter(col("modified_date__v") > max_date)

else:
    # Target doesn't exist → first time load
    print("Target table does not exist. Performing full load...")
    new_df = src_df

# Count how many new records are ready to ingest
new_record_count = new_df.count()
print(f"New records found: {new_record_count}")

if new_record_count > 0:
    # Append only new records, existing data stays untouched
    new_df.write.mode("append").saveAsTable(tgt_table)
    print(f"Successfully ingested {new_record_count} new records into '{tgt_table}'")
else:
    # Nothing to load, target is already up to date
    print("No new records to ingest. Target table is already up to date.")

# COMMAND ----------

# MAGIC %md
# MAGIC # Below logic loads data into Control Tower Table
# MAGIC i.e "com_edp_dev.com_raw.dcr_control_tower"
# MAGIC
# MAGIC Yet to add reltio pull logic

# COMMAND ----------

# --------------------------------------------------------
# PURPOSE:
# Build a clean "Control Tower" table from raw DCR data.
# Only latest record per DCR is kept, invalid rows dropped,
# and entity type codes are mapped to readable business values.
# --------------------------------------------------------

from pyspark.sql.functions import col, when, row_number
from pyspark.sql.window import Window

# Read from delta table (incremental source) instead of raw source table
df_dcr = spark.table("com_edp_dev.com_raw.vcrm_data_change_request_delta")

# Count source records for validation at end
src_count = df_dcr.count()
print(f"Source records read from delta table: {src_count}")

# -----------------------------------
# Step 1: Drop invalid records
# Records without modified_date__v are incomplete → exclude them
# -----------------------------------
df_dcr = df_dcr.filter(col("modified_date__v").isNotNull())

# -----------------------------------
# Step 2: Keep only the latest version of each DCR
# A DCR can have multiple updates → we only want the most recent one
# Partition by DCR id, order by modified date descending, pick rank 1
# -----------------------------------
window_dcr = Window.partitionBy("id").orderBy(col("modified_date__v").desc())

df_dcr_latest = (
    df_dcr.withColumn("rn", row_number().over(window_dcr))
    .filter(col("rn") == 1)   # rank 1 = latest record per DCR
    .drop("rn")                # cleanup ranking column
)

# -----------------------------------
# Step 3: Select and rename only the columns needed for control table
# Dropping unnecessary fields, aliasing to business-friendly names
# -----------------------------------
df_control = df_dcr_latest.select(
    col("name__v").alias("dcr_id"),                                     # Unique DCR identifier
    col("id").alias("source_crm_id"),                                   # CRM internal record ID
    col("object_type__v").alias("entity_type"),                         # HCP / HCO / ADDRESS (raw code)
    col("type__v").alias("dcr_type"),                                   # Type of change request
    col("network_session_id__v").alias("veeva_record_id"),              # Veeva network session reference
    col("data_change_request_status__v").alias("status"),               # Current status of DCR
    col("created_date__v").alias("submitted_at"),                       # When DCR was first raised
    col("modified_date__v").alias("last_updated_at"),                   # When DCR was last modified
    col("modified_by__v").cast("string").alias("updated_by"),           # Who last updated the DCR
    col("parent_data_change_request__v").alias("parent_dcr_id")         # Parent DCR reference (if any)
)

# -----------------------------------
# Step 4: Decode entity type codes to readable business labels
# CRM stores entity types as internal OOT codes → map to HCP / HCO / ADDRESS
# Note: No parent lookup done — ADDRESS is kept as-is without resolution
# -----------------------------------
df_control = df_control.withColumn(
    "entity_type",
    when(col("entity_type") == "OOT00000000V455", "HCP")       # Healthcare Professional
    .when(col("entity_type") == "OOT00000000V456", "HCO")      # Healthcare Organization
    .when(col("entity_type") == "OOT00000000V457", "ADDRESS")  # Physical address record
    .otherwise(col("entity_type"))                              # Unknown codes kept as-is
)

# -----------------------------------
# Step 5: Remove parent_dcr_id
# Not required in control table — no parent-child resolution in scope
# -----------------------------------
df_control = df_control.drop("parent_dcr_id")

# -----------------------------------
# Step 6: Write final clean data to control tower table
# Overwrite ensures table always reflects latest processed state
# -----------------------------------
df_control.write.mode("overwrite").saveAsTable("com_edp_dev.com_raw.dcr_control_tower")

# -----------------------------------
# Step 7: Validate — compare source vs target record counts
# -----------------------------------
tgt_count = spark.table("com_edp_dev.com_raw.dcr_control_tower").count()

print(f"Records in source (delta table) : {src_count}")
print(f"Records loaded in target        : {tgt_count}")
print(f"Records dropped (invalid/dedup) : {src_count - tgt_count}")
print("Control table loaded successfully")

display(spark.table("com_edp_dev.com_raw.dcr_control_tower"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Below logic loads data into Log Table
# MAGIC i.e "com_edp_dev.com_raw.dcr_attribute_log"

# COMMAND ----------

# --------------------------------------------------------
# PURPOSE:
# Build a clean "Attribute Log" table from DCR Line data.
# Only latest record per line id is kept, invalid rows dropped,
# and data is loaded from the delta table (incremental source).
# --------------------------------------------------------

from pyspark.sql.functions import col, row_number, lower, when, regexp_replace, concat, lit
from pyspark.sql.window import Window

# -----------------------------------
# Step 0: Read from delta table (incremental source) instead of raw source table
# -----------------------------------
df_line = spark.table("com_edp_dev.com_raw.data_change_request_line_delta")

# Count source records for validation at end
src_count = df_line.count()
print(f"Source records read from delta table: {src_count}")

# -----------------------------------
# Step 1: Drop invalid records
# Records without modified_date__v are incomplete → exclude them
# -----------------------------------
df_line = df_line.filter(col("modified_date__v").isNotNull())

# -----------------------------------
# Step 2: Keep only the latest version of each line record
# A line can have multiple updates → we only want the most recent one
# Partition by line id, order by modified date descending, pick rank 1
# -----------------------------------
window_line = Window.partitionBy("id").orderBy(col("modified_date__v").desc())

df_line_latest = (
    df_line.withColumn("rn", row_number().over(window_line))
    .filter(col("rn") == 1)   # rank 1 = latest record per line
    .drop("rn")                # cleanup ranking column
)

# -----------------------------------
# Step 3: Select and rename only the columns needed for attribute log
# Dropping unnecessary fields, aliasing to business-friendly names
# -----------------------------------
df_attr = df_line_latest.select(
    col("name__v").alias("dcr_id"),                                 # Unique DCR line identifier
    col("data_change_request__v").alias("source_crm_id"),           # Parent DCR reference
    col("field_api_name__v").alias("attribute_name"),               # Field that was changed
    col("old_localized_value__v").alias("old_value"),               # Value before the change
    col("new_localized_value__v").alias("new_value"),               # Value after the change
    col("status__v").alias("status"),                               # Current status of this line
    col("resolution_note__v").alias("steward_comment"),             # Data steward's review note
    col("modified_date__v").alias("updated_at"),                    # When this line was last updated
)

# -----------------------------------
# Step 4: Write final clean data to attribute log table
# Overwrite ensures table always reflects latest processed state
# -----------------------------------
df_attr.write.mode("overwrite").saveAsTable("com_edp_dev.com_raw.dcr_attribute_log")

# -----------------------------------
# Step 5: Validate — compare source vs target record counts
# -----------------------------------
tgt_count = spark.table("com_edp_dev.com_raw.dcr_attribute_log").count()

print(f"Records in source (delta table) : {src_count}")
print(f"Records loaded in target        : {tgt_count}")
print(f"Records dropped (invalid/dedup) : {src_count - tgt_count}")
print("Attribute log table loaded successfully")

display(spark.table("com_edp_dev.com_raw.dcr_attribute_log"))

