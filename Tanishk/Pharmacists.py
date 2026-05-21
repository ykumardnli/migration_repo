from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime

run_ts = datetime.now().strftime("%Y%m%d_%H%M%S")

TARGET_TABLE = f"com_edp_prd.cmpa_insights_internal_schema.pharmacist_hcp_reporting_parent_sid_{run_ts}"
TEMP_HCP_BASE = f"com_edp_prd.cmpa_insights_internal_schema.tmp_pharmacist_hcp_base_sid_{run_ts}"

vod_references = spark.table("com_edp_prd.com_raw.vod_references").select(
    "reference_type", "name", "code"
)

vod_hcp = spark.table("com_edp_prd.com_raw.vod_hcp").select(
    "npi_num__v", "first_name__v", "last_name__v", "specialty_1__v", "vid__v"
)

vod_parenthco = spark.table("com_edp_prd.com_raw.vod_parenthco").select(
    "entity_vid__v",
    "parent_hco_vid__v",
    "hierarchy_type__v",
    "parent_hco_status__v",
    "relationship_type__v",
    "modified_date__v",
    "status_update_time__v"
)

vod_hco = spark.table("com_edp_prd.com_raw.vod_hco").select(
    "vid__v", "npi_num__v", "corporate_name__v"
)

kom_providers = spark.table("com_edp_prd.com_raw.kom_providers").select(
    "npi",
    "provider_type",
    "hco_primary_npi",
    "organization_name",
    "provider_address",
    "provider_zip"
)

vod_address = spark.table("com_edp_prd.com_raw.vod_address").select(
    "entity_vid__v",
    "entity_type__v",
    "record_state__v",
    "address_status__v",
    "address_verification_status__v",
    "address_line_1__v",
    "postal_code_cda__v",
    "modified_date__v"
)

zip_map = spark.table(
    "com_edp_prd.cmpa_insights_internal_schema.zip_to_territory_mapping"
).select("zipcode", "city", "state")

specialty_ref = (
    vod_references
    .filter(
        (F.col("reference_type") == "Specialty") &
        (
            F.lower(F.col("name")).like("%clinical pharmacology%") |
            F.lower(F.col("name")).like("%pharmacology%") |
            F.lower(F.col("name")).like("%pharmacy specialty%") |
            F.lower(F.col("name")).like("%pharmaceutical medicine%")
        )
    )
    .select("code")
    .dropDuplicates()
)

hcp_base = (
    vod_hcp.alias("h")
    .join(
        F.broadcast(specialty_ref).alias("s"),
        F.col("h.specialty_1__v") == F.col("s.code"),
        "inner"
    )
    .filter(F.col("h.npi_num__v").isNotNull())
    .select(
        F.col("h.npi_num__v").alias("hcp_npi"),
        F.col("h.first_name__v"),
        F.col("h.last_name__v"),
        F.col("h.specialty_1__v").alias("hcp_specialty"),
        F.col("h.vid__v").alias("hcp_vid")
    )
    .dropDuplicates(["hcp_npi", "hcp_vid"])
)

hcp_base.write.mode("overwrite").format("delta").saveAsTable(TEMP_HCP_BASE)
hcp_base = spark.table(TEMP_HCP_BASE)

vod_parenthco_filtered = (
    vod_parenthco
    .filter(
        (F.col("hierarchy_type__v") == "HCP_HCO") &
        (F.col("parent_hco_status__v") == "A") &
        (F.col("relationship_type__v") == "7356")
    )
    .select("entity_vid__v", "parent_hco_vid__v", "modified_date__v", "status_update_time__v")
)

vod_hco_slim = vod_hco.select(
    F.col("vid__v").alias("hco_vid"),
    F.col("npi_num__v").alias("vod_hco_npi"),
    F.col("corporate_name__v").alias("vod_hco_name")
)

kom_individual = (
    kom_providers
    .filter(F.col("provider_type") == "INDIVIDUAL")
    .select(
        F.col("npi").alias("hcp_npi"),
        F.col("hco_primary_npi").alias("komodo_hco_npi")
    )
)

kom_org = (
    kom_providers
    .filter(F.col("provider_type") == "ORGANIZATION")
    .select(
        F.col("npi").alias("org_npi"),
        F.col("organization_name").alias("komodo_hco_name"),
        F.col("provider_address"),
        F.col("provider_zip")
    )
)

vod_address_filtered = (
    vod_address
    .filter(
        (F.col("entity_type__v") == "HCO") &
        (F.col("record_state__v") == "VALID") &
        (F.col("address_status__v").isin("A", "DS")) &
        (~F.col("address_verification_status__v").isin("NS", "U"))
    )
    .select("entity_vid__v", "address_line_1__v", "postal_code_cda__v", "modified_date__v")
)

vod_ranked = (
    hcp_base.alias("a")
    .join(
        vod_parenthco_filtered.alias("b"),
        F.col("a.hcp_vid") == F.col("b.entity_vid__v"),
        "left"
    )
    .join(
        vod_hco_slim.alias("c"),
        F.col("b.parent_hco_vid__v") == F.col("c.hco_vid"),
        "left"
    )
    .select(
        F.col("a.hcp_npi"),
        F.col("c.vod_hco_npi"),
        F.col("c.vod_hco_name"),
        F.col("b.modified_date__v"),
        F.col("b.status_update_time__v")
    )
)

vod_window = Window.partitionBy("hcp_npi").orderBy(
    F.col("modified_date__v").desc_nulls_last(),
    F.col("status_update_time__v").desc_nulls_last()
)

vod_final = (
    vod_ranked
    .withColumn("rn", F.row_number().over(vod_window))
    .filter(F.col("rn") == 1)
    .select("hcp_npi", "vod_hco_npi", "vod_hco_name")
)

komodo = (
    hcp_base.alias("a")
    .join(kom_individual.alias("b"), "hcp_npi", "left")
    .join(
        kom_org.select("org_npi", "komodo_hco_name").alias("c"),
        F.col("b.komodo_hco_npi") == F.col("c.org_npi"),
        "left"
    )
    .select(
        F.col("a.hcp_npi"),
        F.col("b.komodo_hco_npi"),
        F.col("c.komodo_hco_name")
    )
)

base_output = (
    hcp_base.alias("a")
    .join(vod_final.alias("v"), "hcp_npi", "left")
    .join(komodo.alias("k"), "hcp_npi", "left")
    .select(
        F.col("hcp_npi"),
        F.col("first_name__v"),
        F.col("last_name__v"),
        F.col("hcp_specialty"),
        F.coalesce(F.col("v.vod_hco_npi"), F.col("k.komodo_hco_npi"), F.lit("-")).alias("reporting_hco_npi"),
        F.coalesce(F.col("v.vod_hco_name"), F.col("k.komodo_hco_name"), F.lit("-")).alias("reporting_hco_name"),
        F.when(F.col("v.vod_hco_npi").isNotNull(), F.lit("VOD"))
         .when(F.col("k.komodo_hco_npi").isNotNull(), F.lit("Komodo"))
         .otherwise(F.lit("None"))
         .alias("affiliation_source")
    )
)

hco_vod_join = vod_hco.select(
    F.col("vid__v").alias("hco_vid"),
    F.col("npi_num__v").alias("hco_npi")
)

hco_addr_ranked = (
    hco_vod_join.alias("h")
    .join(
        vod_address_filtered.alias("a"),
        F.col("h.hco_vid") == F.col("a.entity_vid__v"),
        "inner"
    )
    .select(
        F.col("h.hco_npi"),
        F.col("a.address_line_1__v"),
        F.col("a.postal_code_cda__v"),
        F.col("a.modified_date__v")
    )
)

addr_window = Window.partitionBy("hco_npi").orderBy(
    F.col("modified_date__v").desc_nulls_last()
)

hco_vod_address = (
    hco_addr_ranked
    .withColumn("rn", F.row_number().over(addr_window))
    .filter(F.col("rn") == 1)
    .select("hco_npi", "address_line_1__v", "postal_code_cda__v")
)

final_output = (
    base_output.alias("b")
    .join(
        hco_vod_address.alias("v"),
        F.col("b.reporting_hco_npi") == F.col("v.hco_npi"),
        "left"
    )
    .join(
        kom_org.alias("kp"),
        F.col("b.reporting_hco_npi") == F.col("kp.org_npi"),
        "left"
    )
    .withColumn(
        "reporting_parent_address",
        F.when(F.col("v.postal_code_cda__v").isNotNull(), F.col("v.address_line_1__v"))
         .otherwise(F.col("kp.provider_address"))
    )
    .withColumn(
        "reporting_parent_zip",
        F.coalesce(F.col("v.postal_code_cda__v"), F.col("kp.provider_zip"))
    )
    .join(
        F.broadcast(zip_map).alias("z"),
        F.col("reporting_parent_zip") == F.col("z.zipcode"),
        "left"
    )
    .select(
        F.col("b.hcp_npi"),
        F.col("b.first_name__v"),
        F.col("b.last_name__v"),
        F.col("b.hcp_specialty"),
        F.col("b.reporting_hco_npi"),
        F.col("b.reporting_hco_name"),
        F.col("b.affiliation_source"),
        F.col("reporting_parent_address"),
        F.col("reporting_parent_zip"),
        F.col("z.city").alias("reporting_parent_city"),
        F.col("z.state").alias("reporting_parent_state")
    )
)

final_output.write.mode("overwrite").format("delta").saveAsTable(TARGET_TABLE)

spark.sql(f"DROP TABLE IF EXISTS {TEMP_HCP_BASE}")

print(f"Finished building {TARGET_TABLE}")

## Display the table.
spark.table(TARGET_TABLE).show(20, False)