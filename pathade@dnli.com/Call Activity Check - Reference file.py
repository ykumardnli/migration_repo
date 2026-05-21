# Databricks notebook source
# MAGIC %sql
# MAGIC select b.ndc11, count(distinct a.patient_id) from cmpa_insights_internal_schema.patient360_master a
# MAGIC left join com_intgr.claims_pharmacy_events b
# MAGIC on a.patient_id = b.PATIENT_ID
# MAGIC where b.ndc11 = 8497600101 
# MAGIC GROUP BY b.ndc11

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW runtime_parameters AS
# MAGIC
# MAGIC SELECT
# MAGIC     (SELECT MAX(service_date) FROM com_edp_prd.com_raw.kom_medical_events) AS max_medical_date,
# MAGIC
# MAGIC     (SELECT MAX(fill_date) FROM com_edp_prd.com_raw.kom_pharmacy_events) AS max_pharmacy_date,
# MAGIC
# MAGIC     LAST_DAY(
# MAGIC         ADD_MONTHS(
# MAGIC             LEAST(
# MAGIC                 (SELECT MAX(service_date) FROM com_edp_prd.com_raw.kom_medical_events),
# MAGIC                 (SELECT MAX(fill_date) FROM com_edp_prd.com_raw.kom_pharmacy_events)
# MAGIC             ), -1
# MAGIC         )
# MAGIC     ) AS end_date,
# MAGIC
# MAGIC     CURRENT_DATE() AS run_date;
# MAGIC
# MAGIC     SELECT * FROM runtime_parameters;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from cmpa_insights_internal_schema.patient360_master;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- patient360_master (Final patient-level master table)
# MAGIC --
# MAGIC -- What this step does:
# MAGIC --   Materializes a single, “wide” patient-level table by combining three upstream
# MAGIC --   datasets into one row per patient:
# MAGIC --     A) patient360_base              -> core demographics + claim-derived milestones + HCP features
# MAGIC --     B) most_recently_treated_hcp    -> most recent treating HCP in refresh window + visit context metrics
# MAGIC --     C) primary_hcp                  -> primary HCP assignment + HCO/territory/region enrichment
# MAGIC --
# MAGIC -- Output:
# MAGIC --   com_edp_prd.cmpa_insights_internal_schema.patient360_master
# MAGIC --
# MAGIC -- Join strategy:
# MAGIC --   - Start from patient360_base (a) as the backbone (all patients retained)
# MAGIC --   - LEFT JOIN most_recently_treated_hcp (b) on patient_id to add recent-treatment attribution fields
# MAGIC --   - LEFT JOIN primary_hcp (c) on patient_id to add primary HCP and territory enrichment fields
# MAGIC --   - SELECT DISTINCT used to dedupe in case joins introduce multiplicity (e.g., enrichment tables
# MAGIC --     or upstream views have >1 row per patient)
# MAGIC --
# MAGIC -- Column handling:
# MAGIC --   - b.* EXCEPT(patient_id) and c.* EXCEPT(patient_id) avoid duplicating patient_id columns
# MAGIC --     from the joined datasets.
# MAGIC --   - Window suffixes (_2yr/_3yr/_5yr) indicate derivation windows from upstream logic.
# MAGIC --
# MAGIC -- Parameters:
# MAGIC --   None directly here (all parameterized logic occurs upstream), but this depends on upstream
# MAGIC --   tables/views that may be parameterized by ${end_date}.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TABLE com_edp_prd.cmpa_insights_internal_schema.patient360_master AS
# MAGIC SELECT DISTINCT
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Patient identity + demographics (from patient360_base)
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.PATIENT_ID,
# MAGIC     a.PATIENT_YOB,
# MAGIC     a.PATIENT_AGE,
# MAGIC     a.PATIENT_GENDER,
# MAGIC     a.patient_state,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Historical milestones (patient-level; timeframe-agnostic where defined upstream)
# MAGIC     --   - incidence_date: earliest observed Dx date across history (as defined in base)
# MAGIC     --   - first_incidence_treatment_date: earliest observed treatment date across history (as defined in base)
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.incidence_date,
# MAGIC     a.first_incidence_treatment_date,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Latest claim (patient-level)
# MAGIC     --   - latest_claim_date may fall back to a patient-level date if NPI attribution is missing upstream
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.latest_claim_date,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Latest claim attributed HCP + visit context (from patient360_base)
# MAGIC     --   Note: these fields are populated only if the latest claim had an attributable NPI upstream
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.latest_claim_hcp_npi,
# MAGIC     a.latest_claim_hcp_name,
# MAGIC     a.latest_claim_hcp_specialty,
# MAGIC     a.latest_claim_hcp_visit_count,
# MAGIC
# MAGIC     -- Latest claim attributed HCO (via reference enrichment in base)
# MAGIC     a.latest_claim_hcp_hco_npi,
# MAGIC     a.latest_claim_hcp_hco_name,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Latest treatment (patient-level) + derived treatment type
# MAGIC     --   - latest_treatment_date may fall back to a patient-level date if NPI attribution is missing upstream
# MAGIC     --   - latest_mpsii_tx_type typically derived within the refresh window upstream
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.latest_treatment_date,
# MAGIC     a.latest_mpsii_tx_type,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- First Tx after Dx + timelines (from patient360_base)
# MAGIC     --   - first_tx_after_diagnosis is computed upstream (all-time tx constrained to >= incidence_date)
# MAGIC     --   - timelines are derived from incidence_date / first_tx_after_diagnosis / latest_treatment_date
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.first_tx_after_diagnosis,
# MAGIC     a.time_dx_to_first_tx_in_months,
# MAGIC     a.treatment_period_months,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Treatment activity proxy (from patient360_base)
# MAGIC     --   - elaprase_fills: distinct tx dates in refresh window as defined upstream
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.elaprase_fills,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Latest treatment attributed HCP + visit context (from patient360_base)
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.latest_treatment_hcp_npi,
# MAGIC     a.latest_treatment_hcp_name,
# MAGIC     a.latest_treatment_hcp_specialty,
# MAGIC     a.latest_treatment_hcp_visit_count,
# MAGIC
# MAGIC     -- Latest treatment attributed HCO (via reference enrichment in base)
# MAGIC     a.latest_treatment_hcp_hco_npi,
# MAGIC     a.latest_treatment_hcp_hco_name,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- First Dx / Tx attributed HCP metrics (5y-ish universe; from patient360_base)
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.first_dx_hcp_5yr,
# MAGIC     a.first_dx_all_visit_count_5yr,
# MAGIC     a.first_dx_last_visit_5yr,
# MAGIC     a.first_tx_hcp_5yr,
# MAGIC     a.first_tx_all_visit_count_5yr,
# MAGIC     a.first_tx_treatment_visit_count_5yr,
# MAGIC     a.first_tx_last_visit_5yr,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Top-5 most-seen HCPs (ranked by 3y; includes 5y counts + last-visit; from patient360_base)
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.most_seen_hcp1_3yr_ranked,
# MAGIC     a.most_seen_hcp1_visit_count_5yr,
# MAGIC     a.most_seen_hcp1_last_visit_5yr,
# MAGIC     a.most_seen_hcp2_3yr_ranked,
# MAGIC     a.most_seen_hcp2_visit_count_5yr,
# MAGIC     a.most_seen_hcp2_last_visit_5yr,
# MAGIC     a.most_seen_hcp3_3yr_ranked,
# MAGIC     a.most_seen_hcp3_visit_count_5yr,
# MAGIC     a.most_seen_hcp3_last_visit_5yr,
# MAGIC     a.most_seen_hcp4_3yr_ranked,
# MAGIC     a.most_seen_hcp4_visit_count_5yr,
# MAGIC     a.most_seen_hcp4_last_visit_5yr,
# MAGIC     a.most_seen_hcp5_3yr_ranked,
# MAGIC     a.most_seen_hcp5_visit_count_5yr,
# MAGIC     a.most_seen_hcp5_last_visit_5yr,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Add-on enrichment block #1: most_recently_treated_hcp
# MAGIC     --   Pulls in:
# MAGIC     --     - most_recently_treated_hcp_2yr and its attributes (name/specialty/HCO/territory/region)
# MAGIC     --     - visit context metrics computed over the broader claims universe in that view
# MAGIC     --   EXCEPT(patient_id) avoids duplicating the patient id column.
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     b.* EXCEPT (patient_id),
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Add-on enrichment block #2: primary_hcp
# MAGIC     --   Pulls in:
# MAGIC     --     - primary HCP attribution + name/HCO/territory/region fields
# MAGIC     --   EXCEPT(patient_id) avoids duplicating the patient id column.
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     c.* EXCEPT (patient_id),
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC -- Add-on enrichment block #3: TIVI HCP
# MAGIC -- -------------------------------------------------------------------------
# MAGIC
# MAGIC     d.tivi_first_tx_hcp_5yr,
# MAGIC     d.tivi_first_tx_visit_count_5yr,
# MAGIC
# MAGIC     d.tivi_latest_tx_hcp,
# MAGIC     d.tivi_latest_tx_visit_count_5yr,
# MAGIC
# MAGIC     d.tivi_most_seen_hcp1,
# MAGIC     d.tivi_most_seen_hcp2,
# MAGIC     d.tivi_most_seen_hcp3,
# MAGIC     d.tivi_most_seen_hcp4,
# MAGIC     d.tivi_most_seen_hcp5,
# MAGIC     -- =========================
# MAGIC     -- TIVI: LATEST HCP ENRICHMENT
# MAGIC     -- =========================
# MAGIC     CONCAT(pdtivi_latest_tx.FIRST_NAME, ' ', pdtivi_latest_tx.LAST_NAME) AS tivi_latest_tx_hcp_name,
# MAGIC     pdtivi_latest_tx.PRIMARY_SPECIALTY AS tivi_latest_tx_hcp_specialty,
# MAGIC     ref_tivi_latest_tx.hco_npi         AS tivi_latest_tx_hcp_hco_npi,
# MAGIC     ref_tivi_latest_tx.hco_name        AS tivi_latest_tx_hcp_hco_name,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- TIVI: FIRST HCP ENRICHMENT
# MAGIC     -- =========================
# MAGIC     CONCAT(pdtivi_first_tx.FIRST_NAME, ' ', pdtivi_first_tx.LAST_NAME) AS tivi_first_tx_hcp_name,
# MAGIC     pdtivi_first_tx.PRIMARY_SPECIALTY  AS tivi_first_tx_hcp_specialty,
# MAGIC     ref_tivi_first_tx.hco_npi          AS tivi_first_tx_hcp_hco_npi,
# MAGIC     ref_tivi_first_tx.hco_name         AS tivi_first_tx_hcp_hco_name,
# MAGIC
# MAGIC     -- =========================
# MAGIC -- TIVI MOST SEEN HCP 1
# MAGIC -- =========================
# MAGIC CONCAT(pdtivi_m1.FIRST_NAME, ' ', pdtivi_m1.LAST_NAME) AS tivi_most_seen_hcp1_name,
# MAGIC pdtivi_m1.PRIMARY_SPECIALTY AS tivi_most_seen_hcp1_specialty,
# MAGIC ref_tivi_m1.hco_name AS tivi_most_seen_hcp1_hco_name,
# MAGIC
# MAGIC -- =========================
# MAGIC -- TIVI MOST SEEN HCP 2
# MAGIC -- =========================
# MAGIC CONCAT(pdtivi_m2.FIRST_NAME, ' ', pdtivi_m2.LAST_NAME) AS tivi_most_seen_hcp2_name,
# MAGIC pdtivi_m2.PRIMARY_SPECIALTY AS tivi_most_seen_hcp2_specialty,
# MAGIC ref_tivi_m2.hco_name AS tivi_most_seen_hcp2_hco_name
# MAGIC
# MAGIC FROM com_edp_prd.cmpa_insights_internal_schema.patient360_base AS a
# MAGIC
# MAGIC -- Left join retains all patients from patient360_base even if no match in most_recently_treated_hcp
# MAGIC LEFT JOIN most_recently_treated_hcp AS b
# MAGIC     ON a.PATIENT_ID = b.patient_id
# MAGIC
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.primary_hcp AS c
# MAGIC     ON a.PATIENT_ID = c.patient_id
# MAGIC
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.patient360_tivi_hcp_summary AS d
# MAGIC     ON a.PATIENT_ID = d.patient_id
# MAGIC LEFT JOIN com_raw.kom_providers pdtivi_latest_tx
# MAGIC     ON d.tivi_latest_tx_hcp = pdtivi_latest_tx.npi
# MAGIC    AND pdtivi_latest_tx.provider_type = 'INDIVIDUAL'
# MAGIC
# MAGIC LEFT JOIN com_raw.kom_providers pdtivi_first_tx
# MAGIC     ON d.tivi_first_tx_hcp_5yr = pdtivi_first_tx.npi
# MAGIC    AND pdtivi_first_tx.provider_type = 'INDIVIDUAL'
# MAGIC
# MAGIC LEFT JOIN cmpa_insights_internal_schema.reference_file_pooja_1703 ref_tivi_latest_tx
# MAGIC     ON d.tivi_latest_tx_hcp = ref_tivi_latest_tx.hcp_npi
# MAGIC
# MAGIC LEFT JOIN cmpa_insights_internal_schema.reference_file_pooja_1703 ref_tivi_first_tx
# MAGIC     ON d.tivi_first_tx_hcp_5yr = ref_tivi_first_tx.hcp_npi
# MAGIC -- =========================
# MAGIC -- TIVI MOST SEEN HCP 1
# MAGIC -- =========================
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.reference_file_pooja_1703 ref_tivi_m1
# MAGIC     ON d.tivi_most_seen_hcp1 = ref_tivi_m1.hcp_npi
# MAGIC
# MAGIC LEFT JOIN com_raw.kom_providers pdtivi_m1
# MAGIC     ON d.tivi_most_seen_hcp1 = pdtivi_m1.npi
# MAGIC    AND pdtivi_m1.provider_type = 'INDIVIDUAL'
# MAGIC
# MAGIC -- =========================
# MAGIC -- TIVI MOST SEEN HCP 2
# MAGIC -- =========================
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.reference_file_pooja_1703 ref_tivi_m2
# MAGIC     ON d.tivi_most_seen_hcp2 = ref_tivi_m2.hcp_npi
# MAGIC
# MAGIC LEFT JOIN com_raw.kom_providers pdtivi_m2
# MAGIC     ON d.tivi_most_seen_hcp2 = pdtivi_m2.npi
# MAGIC    AND pdtivi_m2.provider_type = 'INDIVIDUAL'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE com_edp_prd.cmpa_insights_internal_schema.patient360_tivi_hcp_summary AS
# MAGIC
# MAGIC WITH
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 1. ELIGIBLE PATIENTS (REUSE BASE TABLE)
# MAGIC -- ============================================
# MAGIC
# MAGIC eligible_patients AS (
# MAGIC     SELECT DISTINCT patient_id
# MAGIC     FROM com_edp_prd.cmpa_insights_internal_schema.patient360_base
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 2. PROVIDER FILTER (SAME AS BASE)
# MAGIC -- ============================================
# MAGIC
# MAGIC cohort_3_learnings AS (
# MAGIC   SELECT DISTINCT npi
# MAGIC   FROM com_raw.kom_providers
# MAGIC   WHERE provider_type = 'INDIVIDUAL'
# MAGIC     AND (
# MAGIC       PRIMARY_SPECIALTY NOT IN (
# MAGIC         'Anesthesiologist Assistant','Anesthesiology','Dentist','Dietitian, Registered',
# MAGIC         'Emergency Medical Technician, Basic','Emergency Medicine','General Acute Care Hospital',
# MAGIC         'Nurse Anesthetist, Certified Registered','Obstetrics & Gynecology','Pathology',
# MAGIC         'Radiology','Urology'
# MAGIC       )
# MAGIC       OR SECONDARY_SPECIALTY IN (
# MAGIC         'Child & Adolescent Psychiatry','Psychiatry','Adolescent Medicine','Developmental - Behavioral Pediatrics',
# MAGIC         'Neonatal-Perinatal Medicine','Nutrition, Pediatric','Oncology, Pediatrics','Pediatric Cardiology',
# MAGIC         'Pediatric Critical Care Medicine','Pediatric Dermatology','Pediatric Emergency Medicine',
# MAGIC         'Pediatric Endocrinology','Pediatric Gastroenterology','Pediatric Hematology-Oncology',
# MAGIC         'Pediatric Infectious Diseases','Pediatric Nephrology','Pediatric Ophthalmology and Strabismus Specialist',
# MAGIC         'Pediatric Orthopaedic Surgery','Pediatric Otolaryngology','Pediatric Pulmonology','Pediatric Radiology',
# MAGIC         'Pediatric Rehabilitation Medicine','Pediatric Rheumatology','Pediatric Surgery','Pediatrics',
# MAGIC         'Clinical Biochemical Genetics','Clinical Genetics (M.D.)','Clinical Molecular Genetics',
# MAGIC         'Ph.D. Medical Genetics','Neurodevelopmental Disabilities','Neurology',
# MAGIC         'Neurology with Special Qualifications in Child Neurology','Neuroradiology'
# MAGIC       )
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 3. TIVI TX CLAIMS (5YR WINDOW)
# MAGIC -- ============================================
# MAGIC
# MAGIC tivi_tx_claims_5yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC           SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 = '50383066730'
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC
# MAGIC       UNION
# MAGIC
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           PRESCRIBER_NPI AS NPI,
# MAGIC           FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 = '50383066730'
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     )
# MAGIC     WHERE PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       AND (npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL)
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 4. 3YR + 2YR WINDOWS
# MAGIC -- ============================================
# MAGIC
# MAGIC tivi_tx_claims_3yr AS (
# MAGIC     SELECT *
# MAGIC     FROM tivi_tx_claims_5yr
# MAGIC     WHERE FILL_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC tivi_tx_claims_2yr AS (
# MAGIC     SELECT *
# MAGIC     FROM tivi_tx_claims_5yr
# MAGIC     WHERE FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 5. FIRST TIVI HCP (5YR)
# MAGIC -- ============================================
# MAGIC
# MAGIC tivi_first_tx_hcp_ranked AS (
# MAGIC     SELECT PATIENT_ID, NPI, MIN(FILL_DATE) AS first_date,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY MIN(FILL_DATE), NPI) rn
# MAGIC     FROM tivi_tx_claims_5yr
# MAGIC     WHERE NPI IS NOT NULL
# MAGIC     GROUP BY PATIENT_ID, NPI
# MAGIC ),
# MAGIC
# MAGIC tivi_first_tx_hcp AS (
# MAGIC     SELECT PATIENT_ID, NPI AS tivi_first_tx_hcp_5yr, first_date
# MAGIC     FROM tivi_first_tx_hcp_ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC tivi_first_tx_stats AS (
# MAGIC     SELECT f.PATIENT_ID, f.tivi_first_tx_hcp_5yr,
# MAGIC            COUNT(DISTINCT t.FILL_DATE) AS visit_count_5yr,
# MAGIC            MAX(t.FILL_DATE) AS last_visit_5yr
# MAGIC     FROM tivi_first_tx_hcp f
# MAGIC     LEFT JOIN tivi_tx_claims_5yr t
# MAGIC       ON f.PATIENT_ID = t.PATIENT_ID AND f.tivi_first_tx_hcp_5yr = t.NPI
# MAGIC     GROUP BY 1,2
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 6. MOST RECENT TIVI HCP (2YR)
# MAGIC -- ============================================
# MAGIC
# MAGIC tivi_latest_tx_hcp_ranked AS (
# MAGIC     SELECT PATIENT_ID, NPI, FILL_DATE,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY FILL_DATE DESC, NPI) rn
# MAGIC     FROM tivi_tx_claims_2yr
# MAGIC ),
# MAGIC
# MAGIC tivi_latest_tx_hcp AS (
# MAGIC     SELECT PATIENT_ID, NPI AS tivi_latest_tx_hcp, FILL_DATE
# MAGIC     FROM tivi_latest_tx_hcp_ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC tivi_latest_tx_stats AS (
# MAGIC     SELECT l.PATIENT_ID, l.tivi_latest_tx_hcp,
# MAGIC            COUNT(DISTINCT t.FILL_DATE) AS visit_count_5yr
# MAGIC     FROM tivi_latest_tx_hcp l
# MAGIC     LEFT JOIN tivi_tx_claims_5yr t
# MAGIC       ON l.PATIENT_ID = t.PATIENT_ID AND l.tivi_latest_tx_hcp = t.NPI
# MAGIC     GROUP BY 1,2
# MAGIC ),
# MAGIC
# MAGIC -- ============================================
# MAGIC -- 7. MOST SEEN (3YR)
# MAGIC -- ============================================
# MAGIC
# MAGIC tivi_most_seen AS (
# MAGIC     SELECT PATIENT_ID, NPI,
# MAGIC            COUNT(DISTINCT FILL_DATE) AS visit_count_3yr,
# MAGIC            MAX(FILL_DATE) AS last_visit_3yr,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY PATIENT_ID
# MAGIC              ORDER BY COUNT(DISTINCT FILL_DATE) DESC,
# MAGIC                       MAX(FILL_DATE) DESC,
# MAGIC                       NPI
# MAGIC            ) rank
# MAGIC     FROM tivi_tx_claims_3yr
# MAGIC     WHERE NPI IS NOT NULL
# MAGIC     GROUP BY PATIENT_ID, NPI
# MAGIC ),
# MAGIC
# MAGIC tivi_most_seen_pivot AS (
# MAGIC     SELECT
# MAGIC         PATIENT_ID,
# MAGIC         MAX(CASE WHEN rank=1 THEN NPI END) AS tivi_most_seen_hcp1,
# MAGIC         MAX(CASE WHEN rank=2 THEN NPI END) AS tivi_most_seen_hcp2,
# MAGIC         MAX(CASE WHEN rank=3 THEN NPI END) AS tivi_most_seen_hcp3,
# MAGIC         MAX(CASE WHEN rank=4 THEN NPI END) AS tivi_most_seen_hcp4,
# MAGIC         MAX(CASE WHEN rank=5 THEN NPI END) AS tivi_most_seen_hcp5
# MAGIC     FROM tivi_most_seen
# MAGIC     GROUP BY PATIENT_ID
# MAGIC )
# MAGIC
# MAGIC -- ============================================
# MAGIC -- FINAL SELECT
# MAGIC -- ============================================
# MAGIC
# MAGIC SELECT
# MAGIC     ep.PATIENT_ID,
# MAGIC
# MAGIC     ft.tivi_first_tx_hcp_5yr,
# MAGIC     fs.visit_count_5yr AS tivi_first_tx_visit_count_5yr,
# MAGIC     fs.last_visit_5yr AS tivi_last_visit_5yr,
# MAGIC
# MAGIC     lt.tivi_latest_tx_hcp,
# MAGIC     ls.visit_count_5yr AS tivi_latest_tx_visit_count_5yr,
# MAGIC
# MAGIC     mp.* EXCEPT (patient_id)
# MAGIC
# MAGIC FROM eligible_patients ep
# MAGIC
# MAGIC LEFT JOIN tivi_first_tx_hcp ft ON ep.PATIENT_ID = ft.PATIENT_ID
# MAGIC LEFT JOIN tivi_first_tx_stats fs ON ep.PATIENT_ID = fs.PATIENT_ID
# MAGIC LEFT JOIN tivi_latest_tx_hcp lt ON ep.PATIENT_ID = lt.PATIENT_ID
# MAGIC LEFT JOIN tivi_latest_tx_stats ls ON ep.PATIENT_ID = ls.PATIENT_ID
# MAGIC LEFT JOIN tivi_most_seen_pivot mp ON ep.PATIENT_ID = mp.PATIENT_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE com_edp_prd.cmpa_insights_internal_schema.patient360_master AS
# MAGIC
# MAGIC SELECT DISTINCT
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- PATIENT CORE
# MAGIC     -- =====================================================
# MAGIC     a.PATIENT_ID,
# MAGIC     a.PATIENT_YOB,
# MAGIC     a.PATIENT_AGE,
# MAGIC     a.PATIENT_GENDER,
# MAGIC     a.patient_state,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- MILESTONES
# MAGIC     -- =====================================================
# MAGIC     a.incidence_date,
# MAGIC     a.first_incidence_treatment_date,
# MAGIC     a.latest_claim_date,
# MAGIC     a.latest_treatment_date,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- ELAPRASE - FIRST DX / TX (5YR)
# MAGIC     -- =====================================================
# MAGIC     a.first_dx_hcp_5yr,
# MAGIC     a.first_dx_all_visit_count_5yr,
# MAGIC     a.first_dx_last_visit_5yr,
# MAGIC
# MAGIC     a.first_tx_hcp_5yr,
# MAGIC     a.first_tx_all_visit_count_5yr,
# MAGIC     a.first_tx_last_visit_5yr,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- ELAPRASE - MOST SEEN HCPs
# MAGIC     -- =====================================================
# MAGIC     a.most_seen_hcp1_3yr_ranked,
# MAGIC     a.most_seen_hcp1_visit_count_5yr,
# MAGIC     a.most_seen_hcp1_last_visit_5yr,
# MAGIC
# MAGIC     a.most_seen_hcp2_3yr_ranked,
# MAGIC     a.most_seen_hcp2_visit_count_5yr,
# MAGIC     a.most_seen_hcp2_last_visit_5yr,
# MAGIC
# MAGIC     a.most_seen_hcp3_3yr_ranked,
# MAGIC     a.most_seen_hcp3_visit_count_5yr,
# MAGIC     a.most_seen_hcp3_last_visit_5yr,
# MAGIC
# MAGIC     a.most_seen_hcp4_3yr_ranked,
# MAGIC     a.most_seen_hcp4_visit_count_5yr,
# MAGIC     a.most_seen_hcp4_last_visit_5yr,
# MAGIC
# MAGIC     a.most_seen_hcp5_3yr_ranked,
# MAGIC     a.most_seen_hcp5_visit_count_5yr,
# MAGIC     a.most_seen_hcp5_last_visit_5yr,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- ELAPRASE - LATEST CLAIM HCP
# MAGIC     -- =====================================================
# MAGIC     a.latest_claim_hcp_npi,
# MAGIC     a.latest_claim_hcp_name,
# MAGIC     a.latest_claim_hcp_specialty,
# MAGIC     a.latest_claim_hcp_visit_count,
# MAGIC     a.latest_claim_hcp_hco_npi,
# MAGIC     a.latest_claim_hcp_hco_name,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- ELAPRASE - LATEST TREATMENT HCP
# MAGIC     -- =====================================================
# MAGIC     a.latest_treatment_hcp_npi,
# MAGIC     a.latest_treatment_hcp_name,
# MAGIC     a.latest_treatment_hcp_specialty,
# MAGIC     a.latest_treatment_hcp_visit_count,
# MAGIC     a.latest_treatment_hcp_hco_npi,
# MAGIC     a.latest_treatment_hcp_hco_name,
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- MOST RECENTLY TREATED HCP (2YR)
# MAGIC     -- =====================================================
# MAGIC     b.* EXCEPT (patient_id),
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- PRIMARY HCP
# MAGIC     -- =====================================================
# MAGIC     c.* EXCEPT (patient_id),
# MAGIC
# MAGIC     -- =====================================================
# MAGIC     -- =====================================================
# MAGIC     -- 🔵 TIVI CARE TEAM BLOCK
# MAGIC     -- =====================================================
# MAGIC     -- =====================================================
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- FIRST TIVI HCP (5YR)
# MAGIC     -- =========================
# MAGIC     d.tivi_first_tx_hcp_5yr,
# MAGIC     d.tivi_first_tx_visit_count_5yr,
# MAGIC
# MAGIC     CONCAT(pdtivi_first_tx.FIRST_NAME, ' ', pdtivi_first_tx.LAST_NAME) AS tivi_first_tx_hcp_name_5yr,
# MAGIC     pdtivi_first_tx.PRIMARY_SPECIALTY AS tivi_first_tx_hcp_specialty_5yr,
# MAGIC     ref_tivi_first_tx.hco_npi AS tivi_first_tx_hcp_hco_npi_5yr,
# MAGIC     ref_tivi_first_tx.hco_name AS tivi_first_tx_hcp_hco_name_5yr,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- LATEST TIVI HCP
# MAGIC     -- =========================
# MAGIC     d.tivi_latest_tx_hcp,
# MAGIC     d.tivi_latest_tx_visit_count_5yr,
# MAGIC
# MAGIC     CONCAT(pdtivi_latest_tx.FIRST_NAME, ' ', pdtivi_latest_tx.LAST_NAME) AS tivi_latest_tx_hcp_name,
# MAGIC     pdtivi_latest_tx.PRIMARY_SPECIALTY AS tivi_latest_tx_hcp_specialty,
# MAGIC     ref_tivi_latest_tx.hco_npi AS tivi_latest_tx_hcp_hco_npi,
# MAGIC     ref_tivi_latest_tx.hco_name AS tivi_latest_tx_hcp_hco_name,
# MAGIC
# MAGIC     -- =========================
# MAGIC     -- MOST SEEN TIVI HCPs
# MAGIC     -- =========================
# MAGIC     d.tivi_most_seen_hcp1,
# MAGIC     d.tivi_most_seen_hcp2,
# MAGIC     d.tivi_most_seen_hcp3,
# MAGIC     d.tivi_most_seen_hcp4,
# MAGIC     d.tivi_most_seen_hcp5
# MAGIC
# MAGIC FROM com_edp_prd.cmpa_insights_internal_schema.patient360_base a
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- ELAPRASE ENRICHMENTS
# MAGIC -- =====================================================
# MAGIC LEFT JOIN most_recently_treated_hcp b
# MAGIC     ON a.patient_id = b.patient_id
# MAGIC
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.primary_hcp c
# MAGIC     ON a.patient_id = c.patient_id
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- TIVI SUMMARY (PRE-AGGREGATED — SAFE JOIN)
# MAGIC -- =====================================================
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.patient360_tivi_hcp_summary d
# MAGIC     ON a.patient_id = d.patient_id
# MAGIC
# MAGIC -- =====================================================
# MAGIC -- TIVI PROVIDER ENRICHMENT
# MAGIC -- =====================================================
# MAGIC
# MAGIC -- FIRST TIVI
# MAGIC LEFT JOIN com_edp_prd.com_raw.kom_providers pdtivi_first_tx
# MAGIC     ON d.tivi_first_tx_hcp_5yr = pdtivi_first_tx.npi
# MAGIC    AND pdtivi_first_tx.provider_type = 'INDIVIDUAL'
# MAGIC
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.reference_file ref_tivi_first_tx
# MAGIC     ON d.tivi_first_tx_hcp_5yr = ref_tivi_first_tx.hcp_npi
# MAGIC
# MAGIC -- LATEST TIVI
# MAGIC LEFT JOIN com_edp_prd.com_raw.kom_providers pdtivi_latest_tx
# MAGIC     ON d.tivi_latest_tx_hcp = pdtivi_latest_tx.npi
# MAGIC    AND pdtivi_latest_tx.provider_type = 'INDIVIDUAL'
# MAGIC
# MAGIC LEFT JOIN com_edp_prd.cmpa_insights_internal_schema.reference_file ref_tivi_latest_tx
# MAGIC     ON d.tivi_latest_tx_hcp = ref_tivi_latest_tx.hcp_npi;
