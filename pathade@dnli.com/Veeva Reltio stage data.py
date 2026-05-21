# Databricks notebook source
# MAGIC %sql
# MAGIC select * from vod_reltio_stage.customer_hco;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from vod_reltio_stage.customer_hcp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vod_reltio_stage.customer_hcp_hco_affiliation;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vod_reltio_stage.customer_location;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vod_reltio_stage.customer_organization_affiliation
# MAGIC limit 15;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vod_reltio_stage.customer_organization_hierarchy
# MAGIC limit 15;
