# Databricks notebook source
# MAGIC %sql
# MAGIC select max(ingestion_date) from com_raw.kom_medical_events;

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
# MAGIC -- =============================================================================
# MAGIC -- Patient HCP Visit Summary View - Top 5 based on 3-Year activity
# MAGIC -- Purpose:
# MAGIC --   Build a patient-level summary for MPS II (E761/E763) patients including:
# MAGIC --   - eligibility logic (Dx criteria + evidence of treatment)
# MAGIC --   - HCP attribution (first Dx, first Tx, latest claim, latest Tx, most-seen top 5)
# MAGIC --   - derived treatment timing metrics (dx->tx months, tx period months)
# MAGIC --   - Tivi fill counts (windowed)
# MAGIC --
# MAGIC -- Key change implemented:
# MAGIC --   first_tx_after_diagnosis is ALL-TIME tx (no date filters) but must be >= incidence_date,
# MAGIC --   using all_tx_claims_alltime.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW patient_hcp_visit_summary AS
# MAGIC WITH
# MAGIC /* ============================================================================
# MAGIC    1) ELIGIBILITY COHORT BUILD
# MAGIC    Goal: Identify eligible MPS II patients using:
# MAGIC      A) "Specified" Dx (E761) with >=2 distinct Dx dates + ANY qualifying treatment evidence
# MAGIC      B) "Incremental Unspecified" Dx (E763) with >=2 distinct Dx dates + Tivi-only evidence
# MAGIC         and NOT already included in (A)
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC -- Pull all "Specified" diagnosis events (E761) within 5-year-ish window for Dx counting.
# MAGIC MPSII_Diagnoses_Specified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E761%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E761'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Keep patients with >=2 distinct Dx dates for "Specified".
# MAGIC Patients_2Dx_Specified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Specified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- Pull all "Unspecified" diagnosis events (E763) within the same window for Dx counting.
# MAGIC MPSII_Diagnoses_Unspecified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E763%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E763'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Keep patients with >=2 distinct Dx dates for "Unspecified".
# MAGIC Patients_2Dx_Unspecified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Unspecified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- Treatment evidence universe (broad): Tivi NDCs OR relevant infusion/procedure codes.
# MAGIC -- Used to ensure "Specified" cohort has some treatment evidence in the more recent window.
# MAGIC MPSII_Treatment_All AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC         --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- Treatment evidence (narrow): Tivi only (NDCs + J1743).
# MAGIC -- Used for incremental inclusion of "Unspecified" cohort.
# MAGIC MPSII_Treatment_Tivi_Only AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE = 'J1743'
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- Eligible "Specified" = >=2 Dx dates AND any treatment evidence.
# MAGIC Patients_2Dx_Specified_With_Treatment AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Specified p
# MAGIC     INNER JOIN MPSII_Treatment_All t USING (PATIENT_ID)
# MAGIC ),
# MAGIC
# MAGIC -- Eligible "Incremental Unspecified" = >=2 Dx dates AND Tivi-only evidence,
# MAGIC -- excluding anyone already in the specified+treatment set.
# MAGIC Patients_Incremental_Unspecified AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Unspecified p
# MAGIC     INNER JOIN MPSII_Treatment_Tivi_Only t USING (PATIENT_ID)
# MAGIC     WHERE p.PATIENT_ID NOT IN (SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment)
# MAGIC ),
# MAGIC
# MAGIC -- Final eligible patient list.
# MAGIC eligible_patients AS (
# MAGIC     SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment
# MAGIC     UNION
# MAGIC     SELECT PATIENT_ID FROM Patients_Incremental_Unspecified
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    2) PROVIDER FILTER ("COHORT 3 LEARNINGS")
# MAGIC    Goal: constrain HCPs to relevant specialties and exclude noise specialties.
# MAGIC    Used to filter Dx/Tx claim NPIs (but still allow NULL NPI claims through).
# MAGIC    ========================================================================== */
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
# MAGIC /* ============================================================================
# MAGIC    3) CLAIMS UNIVERSES (DX + TX) WITH NPI ATTRIBUTION
# MAGIC    - 5Y-ish window (2020-08-01 -> end_date) used for "stats" and "latest"
# MAGIC    - 3Y-ish window (2022-08-01 -> end_date) used for ranking "most-seen"
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC -- All diagnosis claims (E761/E763) in the 5Y window, with NPI attribution.
# MAGIC -- Medical uses rendering/referring; pharmacy uses prescriber.
# MAGIC all_dx_claims_5yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT PATIENT_ID, COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE (DIAGNOSIS_CODES LIKE '%E761%' OR DIAGNOSIS_CODES LIKE '%E763%')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT PATIENT_ID, PRESCRIBER_NPI AS NPI, FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE DIAGNOSIS_CODE IN ('E761','E763')
# MAGIC         AND TRANSACTION_STATUS = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     -- Filter to "allowed" NPIs, but keep NULL NPI rows so patient-level dates won't be lost.
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- All treatment claims in the 5Y window, with a unified TX_CODE field:
# MAGIC --   - Tivi NDCs from medical/pharmacy
# MAGIC --   - Infusion/procedure codes from medical (TX_CODE = PROCEDURE_CODE)
# MAGIC all_tx_claims_5yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC           SERVICE_DATE AS FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           PRESCRIBER_NPI AS NPI,
# MAGIC           FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       -- UNION
# MAGIC
# MAGIC       -- SELECT DISTINCT
# MAGIC       --     PATIENT_ID,
# MAGIC       --     RENDERING_NPI AS NPI,
# MAGIC       --     SERVICE_DATE AS FILL_DATE,
# MAGIC       --     PROCEDURE_CODE AS TX_CODE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- NEW: ALL-TIME treatment universe (no date restriction) using same tx definition as above.
# MAGIC -- Used ONLY to compute "first_tx_after_diagnosis" without restricting to the 5Y window.
# MAGIC all_tx_claims_alltime AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC           SERVICE_DATE AS FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           PRESCRIBER_NPI AS NPI,
# MAGIC           FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       -- UNION
# MAGIC
# MAGIC       -- SELECT DISTINCT
# MAGIC       --     PATIENT_ID,
# MAGIC       --     RENDERING_NPI AS NPI,
# MAGIC       --     SERVICE_DATE AS FILL_DATE,
# MAGIC       --     PROCEDURE_CODE AS TX_CODE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- Normalize Dx + Tx into a single 5Y claim stream (TX_CODE NULL for Dx rows).
# MAGIC -- This enables unified "visit count" and "latest claim" logic.
# MAGIC all_claims_5yr AS (
# MAGIC     SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         NPI,
# MAGIC         FILL_DATE,
# MAGIC         CAST(NULL AS STRING) AS TX_CODE
# MAGIC     FROM all_dx_claims_5yr
# MAGIC     UNION
# MAGIC     SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         NPI,
# MAGIC         FILL_DATE,
# MAGIC         TX_CODE
# MAGIC     FROM all_tx_claims_5yr
# MAGIC ),
# MAGIC
# MAGIC -- Combined Dx + Tx claims in the 3Y window for "most-seen HCP" ranking.
# MAGIC all_claims_3yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       -- Dx (medical/pharmacy)
# MAGIC       SELECT DISTINCT PATIENT_ID, COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE (DIAGNOSIS_CODES LIKE '%E761%' OR DIAGNOSIS_CODES LIKE '%E763%')
# MAGIC         AND SERVICE_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT PATIENT_ID, PRESCRIBER_NPI AS NPI, FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE DIAGNOSIS_CODE IN ('E761','E763')
# MAGIC         AND TRANSACTION_STATUS = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC       -- Tx (medical/pharmacy/proc)
# MAGIC       SELECT DISTINCT PATIENT_ID, COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT PATIENT_ID, PRESCRIBER_NPI AS NPI, FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       -- UNION
# MAGIC       -- SELECT DISTINCT PATIENT_ID, RENDERING_NPI AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2022-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    4) FIRST DX / FIRST TX HCP ATTRIBUTION (5Y WINDOW)
# MAGIC    - "first_dx_hcp": earliest Dx claim NPI per patient (ties broken by NPI)
# MAGIC    - "first_tx_hcp": earliest Tx claim NPI per patient (ties broken by NPI)
# MAGIC    - plus 5Y visit counts + last-visit dates for those attributed HCPs
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC first_dx_hcp_ranked AS (
# MAGIC     SELECT PATIENT_ID, NPI, MIN(FILL_DATE) AS first_dx_date,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY MIN(FILL_DATE) ASC, NPI) AS rn
# MAGIC     FROM all_dx_claims_5yr
# MAGIC     WHERE NPI IS NOT NULL
# MAGIC     GROUP BY PATIENT_ID, NPI
# MAGIC ),
# MAGIC first_dx_hcp AS (
# MAGIC     SELECT PATIENT_ID, NPI AS first_dx_hcp, first_dx_date
# MAGIC     FROM first_dx_hcp_ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- Basic provider dimension for name/specialty lookup.
# MAGIC provider_dim AS (
# MAGIC     SELECT
# MAGIC         npi,
# MAGIC         CONCAT(FIRST_NAME, ' ', LAST_NAME) AS provider_name,
# MAGIC         primary_specialty
# MAGIC     FROM com_raw.kom_providers
# MAGIC     WHERE provider_type = 'INDIVIDUAL'
# MAGIC ),
# MAGIC
# MAGIC -- For the first Dx-attributed HCP: count all claim dates (Dx+Tx) in 5Y and get last visit.
# MAGIC first_dx_hcp_5yr_stats AS (
# MAGIC     SELECT fdh.PATIENT_ID, fdh.first_dx_hcp,
# MAGIC            COUNT(DISTINCT ac.FILL_DATE) AS first_dx_all_visit_count_5yr,
# MAGIC            MAX(ac.FILL_DATE) AS first_dx_last_visit_5yr
# MAGIC     FROM first_dx_hcp fdh
# MAGIC     LEFT JOIN all_claims_5yr ac
# MAGIC       ON fdh.PATIENT_ID = ac.PATIENT_ID AND fdh.first_dx_hcp = ac.NPI
# MAGIC     GROUP BY fdh.PATIENT_ID, fdh.first_dx_hcp
# MAGIC ),
# MAGIC
# MAGIC first_tx_hcp_ranked AS (
# MAGIC     SELECT PATIENT_ID, NPI, MIN(FILL_DATE) AS first_tx_date,
# MAGIC            ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY MIN(FILL_DATE) ASC, NPI) AS rn
# MAGIC     FROM all_tx_claims_5yr
# MAGIC     WHERE NPI IS NOT NULL
# MAGIC     GROUP BY PATIENT_ID, NPI
# MAGIC ),
# MAGIC first_tx_hcp AS (
# MAGIC     SELECT PATIENT_ID, NPI AS first_tx_hcp, first_tx_date
# MAGIC     FROM first_tx_hcp_ranked
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC -- For the first Tx-attributed HCP: count all claim dates (Dx+Tx) in 5Y and get last visit.
# MAGIC first_tx_hcp_5yr_stats AS (
# MAGIC     SELECT fth.PATIENT_ID, fth.first_tx_hcp,
# MAGIC            COUNT(DISTINCT ac.FILL_DATE) AS first_tx_all_visit_count_5yr,
# MAGIC            MAX(ac.FILL_DATE) AS first_tx_last_visit_5yr
# MAGIC     FROM first_tx_hcp fth
# MAGIC     LEFT JOIN all_claims_5yr ac
# MAGIC       ON fth.PATIENT_ID = ac.PATIENT_ID AND fth.first_tx_hcp = ac.NPI
# MAGIC     GROUP BY fth.PATIENT_ID, fth.first_tx_hcp
# MAGIC ),
# MAGIC
# MAGIC -- For the first Tx-attributed HCP: count treatment claim dates only in 5Y.
# MAGIC first_tx_hcp_5yr_tx_only AS (
# MAGIC     SELECT fth.PATIENT_ID, fth.first_tx_hcp,
# MAGIC            COUNT(DISTINCT tx.FILL_DATE) AS first_tx_treatment_visit_count_5yr
# MAGIC     FROM first_tx_hcp fth
# MAGIC     LEFT JOIN all_tx_claims_5yr tx
# MAGIC       ON fth.PATIENT_ID = tx.PATIENT_ID AND fth.first_tx_hcp = tx.NPI
# MAGIC     GROUP BY fth.PATIENT_ID, fth.first_tx_hcp
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    5) MOST-SEEN HCP RANKING (TOP 5) USING 3Y ACTIVITY
# MAGIC    - rank by #distinct visit dates in 3Y, then by recency, then by NPI
# MAGIC    - attach 5Y counts and 5Y last visit for those same HCPs
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC most_seen_3yr_ranking AS (
# MAGIC     SELECT PATIENT_ID,
# MAGIC            NPI,
# MAGIC            COUNT(DISTINCT FILL_DATE) AS visit_count_3yr,
# MAGIC            MAX(FILL_DATE) AS last_visit_3yr,
# MAGIC            ROW_NUMBER() OVER (
# MAGIC              PARTITION BY PATIENT_ID
# MAGIC              ORDER BY COUNT(DISTINCT FILL_DATE) DESC,
# MAGIC                       MAX(FILL_DATE) DESC,
# MAGIC                       NPI ASC
# MAGIC            ) AS rank
# MAGIC     FROM all_claims_3yr
# MAGIC     WHERE NPI IS NOT NULL
# MAGIC     GROUP BY PATIENT_ID, NPI
# MAGIC ),
# MAGIC
# MAGIC most_seen_combined_stats AS (
# MAGIC     SELECT
# MAGIC         ms3.PATIENT_ID,
# MAGIC         ms3.NPI,
# MAGIC         ms3.rank,
# MAGIC         ms3.visit_count_3yr,
# MAGIC         ms3.last_visit_3yr,
# MAGIC         COUNT(DISTINCT ac5.FILL_DATE) AS visit_count_5yr,
# MAGIC         MAX(ac5.FILL_DATE)          AS last_visit_5yr
# MAGIC     FROM most_seen_3yr_ranking ms3
# MAGIC     LEFT JOIN all_claims_5yr ac5
# MAGIC       ON ms3.PATIENT_ID = ac5.PATIENT_ID AND ms3.NPI = ac5.NPI
# MAGIC     WHERE ms3.rank <= 5
# MAGIC     GROUP BY ms3.PATIENT_ID, ms3.NPI, ms3.rank, ms3.visit_count_3yr, ms3.last_visit_3yr
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    6) HISTORICAL (ALL-TIME) FIRST DX / FIRST TX DATES (PATIENT LEVEL)
# MAGIC    Goal: get true first Dx date and true first Tx date without windowing.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC historical_first_dx AS (
# MAGIC     SELECT PATIENT_ID, MIN(FILL_DATE) AS incidence_date
# MAGIC     FROM (
# MAGIC         -- Dx specified + unspecified from both medical and pharmacy, no date filters
# MAGIC         SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE DIAGNOSIS_CODES LIKE '%E761%'
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE DIAGNOSIS_CODE = 'E761'
# MAGIC           AND TRANSACTION_STATUS = 'PAID'
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE DIAGNOSIS_CODES LIKE '%E763%'
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE DIAGNOSIS_CODE = 'E763'
# MAGIC           AND TRANSACTION_STATUS = 'PAID'
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     ) all_dx
# MAGIC     GROUP BY PATIENT_ID
# MAGIC ),
# MAGIC
# MAGIC historical_first_tx AS (
# MAGIC     SELECT PATIENT_ID, MIN(FILL_DATE) AS first_incidence_treatment_date
# MAGIC     FROM (
# MAGIC         -- Tx NDCs and procedures, no date filters
# MAGIC         SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC         --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC         --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     ) all_tx
# MAGIC     GROUP BY PATIENT_ID
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    7) LATEST HCP ATTRIBUTION (5Y WINDOW)
# MAGIC    - latest claim HCP (across Dx+Tx): most recent claim date with an NPI
# MAGIC    - latest tx HCP (tx only): most recent tx claim date with an NPI
# MAGIC    Also compute visit counts for those attributed HCPs within the 5Y window.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC all_claims_5yr_specialty_removed AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       -- Dx (medical/pharmacy)
# MAGIC       SELECT DISTINCT PATIENT_ID, COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE (DIAGNOSIS_CODES LIKE '%E761%' OR DIAGNOSIS_CODES LIKE '%E763%')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT PATIENT_ID, PRESCRIBER_NPI AS NPI, FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE DIAGNOSIS_CODE IN ('E761','E763')
# MAGIC         AND TRANSACTION_STATUS = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC       -- Tx (medical/pharmacy/proc)
# MAGIC       SELECT DISTINCT PATIENT_ID, COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT PATIENT_ID, PRESCRIBER_NPI AS NPI, FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       -- UNION
# MAGIC       -- SELECT DISTINCT PATIENT_ID, RENDERING_NPI AS NPI, SERVICE_DATE AS FILL_DATE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     -- WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ), 
# MAGIC
# MAGIC latest_claim_hcp_ranked as (
# MAGIC   select patient_id, npi, fill_date, row_number() over(partition by patient_id order by fill_date desc, npi asc) as rn
# MAGIC   from all_claims_5yr
# MAGIC ),
# MAGIC
# MAGIC latest_claim_hcp as (
# MAGIC   select patient_id, npi as latest_claim_hcp_npi, fill_date as latest_claim_date from latest_claim_hcp_ranked where rn = 1
# MAGIC ),
# MAGIC
# MAGIC latest_claim_hcp_visit_count_5yr as (
# MAGIC   select a.patient_id, a.latest_claim_hcp_npi, count(distinct fill_date) as latest_claim_hcp_visit_count_5yr
# MAGIC   from latest_claim_hcp as a
# MAGIC   left join all_claims_5yr as b on a.patient_id = b.patient_id and a.latest_claim_hcp_npi = b.npi
# MAGIC   group by 1,2
# MAGIC ),
# MAGIC
# MAGIC latest_claim_hcp_final as (
# MAGIC   select a.patient_id, a.latest_claim_hcp_npi, a.latest_claim_date, b.latest_claim_hcp_visit_count_5yr
# MAGIC   from latest_claim_hcp as a
# MAGIC   left join latest_claim_hcp_visit_count_5yr as b on a.patient_id = b.patient_id
# MAGIC ),
# MAGIC
# MAGIC -- latest_claim_hcp_ranked AS (
# MAGIC --     SELECT
# MAGIC --         PATIENT_ID,
# MAGIC --         NPI,
# MAGIC --         FILL_DATE,
# MAGIC --         ROW_NUMBER() OVER (
# MAGIC --             PARTITION BY PATIENT_ID
# MAGIC --             ORDER BY FILL_DATE DESC, NPI ASC
# MAGIC --         ) AS rn
# MAGIC --     FROM all_claims_5yr
# MAGIC --     WHERE NPI IS NOT NULL
# MAGIC -- ),
# MAGIC -- latest_claim_hcp AS (
# MAGIC --     SELECT
# MAGIC --         PATIENT_ID,
# MAGIC --         NPI AS latest_claim_hcp_npi,
# MAGIC --         FILL_DATE AS latest_claim_date
# MAGIC --     FROM latest_claim_hcp_ranked
# MAGIC --     WHERE rn = 1
# MAGIC -- ),
# MAGIC -- latest_claim_hcp_visit_count_5yr AS (
# MAGIC --     SELECT
# MAGIC --         lch.PATIENT_ID,
# MAGIC --         lch.latest_claim_hcp_npi,
# MAGIC --         COUNT(DISTINCT ac.FILL_DATE) AS latest_claim_hcp_visit_count_5yr
# MAGIC --     FROM latest_claim_hcp lch
# MAGIC --     LEFT JOIN all_claims_5yr ac
# MAGIC --       ON lch.PATIENT_ID = ac.PATIENT_ID
# MAGIC --      AND lch.latest_claim_hcp_npi = ac.NPI
# MAGIC --     GROUP BY lch.PATIENT_ID, lch.latest_claim_hcp_npi
# MAGIC -- ),
# MAGIC
# MAGIC all_tx_claims_5yr_specialty_removed AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC           SERVICE_DATE AS FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC
# MAGIC       SELECT DISTINCT
# MAGIC           PATIENT_ID,
# MAGIC           PRESCRIBER_NPI AS NPI,
# MAGIC           FILL_DATE,
# MAGIC           NDC11 AS TX_CODE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       -- UNION
# MAGIC
# MAGIC       -- SELECT DISTINCT
# MAGIC       --     PATIENT_ID,
# MAGIC       --     RENDERING_NPI AS NPI,
# MAGIC       --     SERVICE_DATE AS FILL_DATE,
# MAGIC       --     PROCEDURE_CODE AS TX_CODE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     -- WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC most_recent_tx_hcp_ranked as (
# MAGIC   select patient_id, npi, fill_date, row_number() over(partition by patient_id order by fill_date desc, npi asc) as rn 
# MAGIC   from all_tx_claims_5yr
# MAGIC ),
# MAGIC
# MAGIC most_recent_tx_hcp as (
# MAGIC   select patient_id, npi as latest_treatment_hcp_npi, fill_date as latest_treatment_date
# MAGIC   from most_recent_tx_hcp_ranked where rn = 1
# MAGIC ),
# MAGIC
# MAGIC latest_treatment_hcp_visit_count as (
# MAGIC   select a.patient_id, a.latest_treatment_hcp_npi, count(distinct fill_date) as latest_treatment_hcp_visit_count_5yr
# MAGIC   from most_recent_tx_hcp as a 
# MAGIC   left join all_tx_claims_5yr as b on a.patient_id = b.patient_id and a.latest_treatment_hcp_npi = b.npi
# MAGIC   group by 1, 2
# MAGIC ),
# MAGIC
# MAGIC most_recent_tx_hcp_final as (
# MAGIC   select a.patient_id, a.latest_treatment_hcp_npi, a.latest_treatment_date, b.latest_treatment_hcp_visit_count_5yr
# MAGIC   from most_recent_tx_hcp as a
# MAGIC   left join latest_treatment_hcp_visit_count as b on a.patient_id = b.patient_id
# MAGIC ),
# MAGIC
# MAGIC -- most_recent_tx_hcp_ranked AS (
# MAGIC --     SELECT
# MAGIC --         PATIENT_ID,
# MAGIC --         NPI,
# MAGIC --         FILL_DATE,
# MAGIC --         ROW_NUMBER() OVER (
# MAGIC --             PARTITION BY PATIENT_ID
# MAGIC --             ORDER BY FILL_DATE DESC, NPI ASC
# MAGIC --         ) AS rn
# MAGIC --     FROM all_tx_claims_5yr
# MAGIC --     WHERE NPI IS NOT NULL
# MAGIC -- ),
# MAGIC -- most_recent_tx_hcp AS (
# MAGIC --     SELECT
# MAGIC --         PATIENT_ID,
# MAGIC --         NPI AS latest_treatment_hcp_npi,
# MAGIC --         FILL_DATE AS latest_treatment_date
# MAGIC --     FROM most_recent_tx_hcp_ranked
# MAGIC --     WHERE rn = 1
# MAGIC -- ),
# MAGIC -- latest_treatment_hcp_visit_count AS (
# MAGIC --     SELECT
# MAGIC --         mrt.PATIENT_ID,
# MAGIC --         mrt.latest_treatment_hcp_npi,
# MAGIC --         COUNT(DISTINCT ac.FILL_DATE) AS latest_treatment_hcp_visit_count_5yr
# MAGIC --     FROM most_recent_tx_hcp mrt
# MAGIC --     LEFT JOIN all_claims_5yr ac
# MAGIC --       ON mrt.PATIENT_ID = ac.PATIENT_ID
# MAGIC --      AND mrt.latest_treatment_hcp_npi = ac.NPI
# MAGIC --     GROUP BY mrt.PATIENT_ID, mrt.latest_treatment_hcp_npi
# MAGIC -- ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    8) PATIENT-LEVEL "LATEST DATE" FALLBACKS (IGNORE NPI)
# MAGIC    Why: if claims exist but all have NULL NPI, HCP-attributed latest_* CTEs go NULL.
# MAGIC         These patient-level dates ensure latest_claim_date/latest_treatment_date are populated.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC -- latest_claim_date_patient AS (
# MAGIC --   SELECT
# MAGIC --     PATIENT_ID,
# MAGIC --     MAX(FILL_DATE) AS latest_claim_date_any
# MAGIC --   FROM all_claims_5yr
# MAGIC --   GROUP BY PATIENT_ID
# MAGIC -- ),
# MAGIC -- latest_treatment_date_patient AS (
# MAGIC --   SELECT
# MAGIC --     PATIENT_ID,
# MAGIC --     MAX(FILL_DATE) AS latest_treatment_date_any
# MAGIC --   FROM all_tx_claims_5yr
# MAGIC --   GROUP BY PATIENT_ID
# MAGIC -- ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    9) LATEST TX TYPE (WINDOWED TO RECENT TREATMENT PERIOD)
# MAGIC    Goal: classify the latest tx within 2023-08-01..end_date as "Tivi" vs other proc.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC latest_mpsii_treatment_type AS (
# MAGIC   SELECT
# MAGIC     patient_id,
# MAGIC     CASE
# MAGIC       WHEN tx_code IN ('8497600101') THEN 'Tivi - Avalayah'
# MAGIC     END AS latest_mpsii_tx_type
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       patient_id,
# MAGIC       fill_date,
# MAGIC       tx_code,
# MAGIC       ROW_NUMBER() OVER (
# MAGIC         PARTITION BY patient_id
# MAGIC         ORDER BY fill_date DESC, tx_code ASC
# MAGIC       ) AS rn
# MAGIC     FROM all_tx_claims_5yr
# MAGIC     WHERE tx_code IS NOT NULL
# MAGIC       AND fill_date BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC   )
# MAGIC   WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    10) FIRST TX AFTER DIAGNOSIS (ALL-TIME TX, BUT MUST BE AFTER DX)
# MAGIC    Goal: compute earliest treatment date after incidence_date using all_tx_claims_alltime.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC first_tx_after_diagnosis AS (
# MAGIC   SELECT
# MAGIC     tx.patient_id,
# MAGIC     MIN(tx.fill_date) AS first_tx_after_diagnosis
# MAGIC   FROM all_tx_claims_alltime tx
# MAGIC   INNER JOIN historical_first_dx dx
# MAGIC     ON tx.patient_id = dx.patient_id
# MAGIC   WHERE tx.fill_date >= dx.incidence_date
# MAGIC   GROUP BY tx.patient_id
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    11) Tivi FILL COUNTS (WINDOWED)
# MAGIC    Goal: count distinct treatment dates for Tivi-coded tx between 2023-08-01..end_date.
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC Tivi_fills AS (
# MAGIC   SELECT
# MAGIC     patient_id,
# MAGIC     COUNT(DISTINCT fill_date) AS Tivi_fills
# MAGIC   FROM all_tx_claims_5yr
# MAGIC   WHERE fill_date BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     AND tx_code IN ('8497600101')
# MAGIC   GROUP BY patient_id
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    12) PATIENT DIMENSIONS
# MAGIC    - demographics: pick a single record per patient
# MAGIC    - geography: pick "best current" state using validity logic
# MAGIC    ========================================================================== */
# MAGIC
# MAGIC patient_demographics AS (
# MAGIC     SELECT *
# MAGIC     FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID, PATIENT_YOB, PATIENT_GENDER,
# MAGIC                ROW_NUMBER() OVER (PARTITION BY PATIENT_ID ORDER BY PATIENT_YOB ASC) AS rn
# MAGIC         FROM com_edp_prd.com_raw.kom_patient_demographics
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC patient_geography AS (
# MAGIC     SELECT patient_id, patient_state
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             PATIENT_ID,
# MAGIC             patient_state,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY PATIENT_ID
# MAGIC                 ORDER BY
# MAGIC                     CASE WHEN VALID_TO_DATE > CURRENT_DATE() THEN 1 ELSE 2 END,
# MAGIC                     VALID_TO_DATE DESC
# MAGIC             ) AS rn
# MAGIC         FROM COM_EDP_PRD.COM_RAW.KOM_PATIENT_GEOGRAPHY
# MAGIC     )
# MAGIC     WHERE rn = 1
# MAGIC ),
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    13) PIVOT TOP-5 MOST-SEEN HCPs INTO WIDE FORMAT
# MAGIC    Goal: turn rows (patient_id, rank=1..5) into columns to avoid repeated joins.
# MAGIC    ========================================================================== */
# MAGIC most_seen_pivot AS (
# MAGIC     SELECT
# MAGIC         PATIENT_ID,
# MAGIC
# MAGIC         MAX(CASE WHEN rank = 1 THEN NPI END)              AS most_seen_hcp1_3yr_ranked,
# MAGIC         MAX(CASE WHEN rank = 1 THEN visit_count_5yr END)  AS most_seen_hcp1_visit_count_5yr,
# MAGIC         MAX(CASE WHEN rank = 1 THEN last_visit_5yr END)   AS most_seen_hcp1_last_visit_5yr,
# MAGIC
# MAGIC         MAX(CASE WHEN rank = 2 THEN NPI END)              AS most_seen_hcp2_3yr_ranked,
# MAGIC         MAX(CASE WHEN rank = 2 THEN visit_count_5yr END)  AS most_seen_hcp2_visit_count_5yr,
# MAGIC         MAX(CASE WHEN rank = 2 THEN last_visit_5yr END)   AS most_seen_hcp2_last_visit_5yr,
# MAGIC
# MAGIC         MAX(CASE WHEN rank = 3 THEN NPI END)              AS most_seen_hcp3_3yr_ranked,
# MAGIC         MAX(CASE WHEN rank = 3 THEN visit_count_5yr END)  AS most_seen_hcp3_visit_count_5yr,
# MAGIC         MAX(CASE WHEN rank = 3 THEN last_visit_5yr END)   AS most_seen_hcp3_last_visit_5yr,
# MAGIC
# MAGIC         MAX(CASE WHEN rank = 4 THEN NPI END)              AS most_seen_hcp4_3yr_ranked,
# MAGIC         MAX(CASE WHEN rank = 4 THEN visit_count_5yr END)  AS most_seen_hcp4_visit_count_5yr,
# MAGIC         MAX(CASE WHEN rank = 4 THEN last_visit_5yr END)   AS most_seen_hcp4_last_visit_5yr,
# MAGIC
# MAGIC         MAX(CASE WHEN rank = 5 THEN NPI END)              AS most_seen_hcp5_3yr_ranked,
# MAGIC         MAX(CASE WHEN rank = 5 THEN visit_count_5yr END)  AS most_seen_hcp5_visit_count_5yr,
# MAGIC         MAX(CASE WHEN rank = 5 THEN last_visit_5yr END)   AS most_seen_hcp5_last_visit_5yr
# MAGIC
# MAGIC     FROM most_seen_combined_stats
# MAGIC     GROUP BY PATIENT_ID
# MAGIC )
# MAGIC
# MAGIC /* ============================================================================
# MAGIC    FINAL SELECT
# MAGIC    Produces one row per eligible patient with:
# MAGIC    - demographics + geography
# MAGIC    - incidence dates + latest dates (with patient-level fallbacks)
# MAGIC    - attributed HCPs (latest claim, latest tx, first dx, first tx, top 5 most-seen)
# MAGIC    - provider + HCO enrichment (reference_file_pooja_1703)
# MAGIC    - derived metrics and Tivi counts
# MAGIC    ========================================================================== */
# MAGIC SELECT
# MAGIC     ep.PATIENT_ID,
# MAGIC     pd.PATIENT_YOB,
# MAGIC     YEAR(CURRENT_DATE) - YEAR(pd.PATIENT_YOB) AS PATIENT_AGE,
# MAGIC     pd.PATIENT_GENDER,
# MAGIC     pg.patient_state,
# MAGIC
# MAGIC     -- Historical First Dates (ALL-TIME)
# MAGIC     hfdx.incidence_date,
# MAGIC     hftx.first_incidence_treatment_date,
# MAGIC
# MAGIC     -- Latest claim date: use HCP-attributed latest if available else patient-level fallback
# MAGIC     -- COALESCE(lch.latest_claim_date, lcd.latest_claim_date_any) AS latest_claim_date,
# MAGIC     lch.latest_claim_date AS latest_claim_date,
# MAGIC
# MAGIC     -- Latest claim HCP attribution (only when NPI exists on that latest claim)
# MAGIC     lch.latest_claim_hcp_npi,
# MAGIC     pdlch.provider_name     AS latest_claim_hcp_name,
# MAGIC     pdlch.primary_specialty AS latest_claim_hcp_specialty,
# MAGIC     COALESCE(lch.latest_claim_hcp_visit_count_5yr, 0) AS latest_claim_hcp_visit_count,
# MAGIC
# MAGIC     -- Map latest-claim HCP -> HCO via reference crosswalk
# MAGIC     -- ref1.hco_npi  AS latest_claim_hcp_hco_npi,
# MAGIC     ref1.hco_name AS latest_claim_hcp_hco_name,
# MAGIC
# MAGIC     -- Latest treatment date: use HCP-attributed latest if available else patient-level fallback
# MAGIC     mrt.latest_treatment_date AS latest_treatment_date,
# MAGIC
# MAGIC     -- Latest tx type (windowed to 2023-08-01..end_date)
# MAGIC     lmt.latest_mpsii_tx_type,
# MAGIC
# MAGIC     -- First treatment after diagnosis (ALL-TIME tx, constrained to >= incidence_date)
# MAGIC     fta.first_tx_after_diagnosis,
# MAGIC
# MAGIC     -- Derived timing: dx -> first tx (months)
# MAGIC     ROUND(MONTHS_BETWEEN(fta.first_tx_after_diagnosis, hfdx.incidence_date), 0)
# MAGIC       AS time_dx_to_first_tx_in_months,
# MAGIC
# MAGIC     -- Derived timing: tx period (months) = first tx after dx -> latest tx (patient-level)
# MAGIC     ROUND(MONTHS_BETWEEN(mrt.latest_treatment_date, fta.first_tx_after_diagnosis), 0) AS treatment_period_months,
# MAGIC
# MAGIC     -- Tivi fills (windowed, Tivi-only codes)
# MAGIC     COALESCE(ef.Tivi_fills, 0) AS Tivi_fills,
# MAGIC
# MAGIC     -- Latest treatment HCP attribution (only when NPI exists on that latest tx claim)
# MAGIC     mrt.latest_treatment_hcp_npi,
# MAGIC     pdtch.provider_name     AS latest_treatment_hcp_name,
# MAGIC     pdtch.primary_specialty AS latest_treatment_hcp_specialty,
# MAGIC     COALESCE(mrt.latest_treatment_hcp_visit_count_5yr, 0) AS latest_treatment_hcp_visit_count,
# MAGIC
# MAGIC     -- Map latest-tx HCP -> HCO via reference crosswalk
# MAGIC     -- ref2.hco_npi  AS latest_treatment_hcp_hco_npi,
# MAGIC     ref2.hco_name AS latest_treatment_hcp_hco_name,
# MAGIC
# MAGIC     -- First Dx HCP (5Y stats)
# MAGIC     fdh.first_dx_hcp AS first_dx_hcp_5yr,
# MAGIC     COALESCE(fdhs.first_dx_all_visit_count_5yr, 0) AS first_dx_all_visit_count_5yr,
# MAGIC     fdhs.first_dx_last_visit_5yr AS first_dx_last_visit_5yr,
# MAGIC
# MAGIC     -- First Tx HCP (5Y stats)
# MAGIC     fth.first_tx_hcp AS first_tx_hcp_5yr,
# MAGIC     COALESCE(fths.first_tx_all_visit_count_5yr, 0) AS first_tx_all_visit_count_5yr,
# MAGIC     COALESCE(fthtx.first_tx_treatment_visit_count_5yr, 0) AS first_tx_treatment_visit_count_5yr,
# MAGIC     fths.first_tx_last_visit_5yr AS first_tx_last_visit_5yr,
# MAGIC
# MAGIC     -- Top 5 most-seen HCPs (ranked by 3Y, with 5Y stats)
# MAGIC     msp.most_seen_hcp1_3yr_ranked,
# MAGIC     COALESCE(msp.most_seen_hcp1_visit_count_5yr, 0) AS most_seen_hcp1_visit_count_5yr,
# MAGIC     msp.most_seen_hcp1_last_visit_5yr,
# MAGIC
# MAGIC     msp.most_seen_hcp2_3yr_ranked,
# MAGIC     COALESCE(msp.most_seen_hcp2_visit_count_5yr, 0) AS most_seen_hcp2_visit_count_5yr,
# MAGIC     msp.most_seen_hcp2_last_visit_5yr,
# MAGIC
# MAGIC     msp.most_seen_hcp3_3yr_ranked,
# MAGIC     COALESCE(msp.most_seen_hcp3_visit_count_5yr, 0) AS most_seen_hcp3_visit_count_5yr,
# MAGIC     msp.most_seen_hcp3_last_visit_5yr,
# MAGIC
# MAGIC     msp.most_seen_hcp4_3yr_ranked,
# MAGIC     COALESCE(msp.most_seen_hcp4_visit_count_5yr, 0) AS most_seen_hcp4_visit_count_5yr,
# MAGIC     msp.most_seen_hcp4_last_visit_5yr,
# MAGIC
# MAGIC     msp.most_seen_hcp5_3yr_ranked,
# MAGIC     COALESCE(msp.most_seen_hcp5_visit_count_5yr, 0) AS most_seen_hcp5_visit_count_5yr,
# MAGIC     msp.most_seen_hcp5_last_visit_5yr
# MAGIC
# MAGIC FROM eligible_patients ep
# MAGIC LEFT JOIN patient_demographics pd
# MAGIC     ON ep.PATIENT_ID = pd.PATIENT_ID
# MAGIC LEFT JOIN patient_geography pg
# MAGIC     ON ep.PATIENT_ID = pg.PATIENT_ID
# MAGIC
# MAGIC -- Patient-level ALL-TIME incidence dates
# MAGIC LEFT JOIN historical_first_dx hfdx
# MAGIC     ON ep.PATIENT_ID = hfdx.PATIENT_ID
# MAGIC LEFT JOIN historical_first_tx hftx
# MAGIC     ON ep.PATIENT_ID = hftx.PATIENT_ID
# MAGIC
# MAGIC -- First tx after dx (ALL-TIME)
# MAGIC LEFT JOIN first_tx_after_diagnosis fta
# MAGIC     ON ep.PATIENT_ID = fta.PATIENT_ID
# MAGIC
# MAGIC -- Tivi fills (windowed)
# MAGIC LEFT JOIN Tivi_fills ef
# MAGIC     ON ep.PATIENT_ID = ef.PATIENT_ID
# MAGIC
# MAGIC -- Patient-level latest date fallbacks (ignore NPI)
# MAGIC -- LEFT JOIN latest_claim_date_patient lcd
# MAGIC --     ON ep.PATIENT_ID = lcd.PATIENT_ID
# MAGIC -- LEFT JOIN latest_treatment_date_patient ltd
# MAGIC --     ON ep.PATIENT_ID = ltd.PATIENT_ID
# MAGIC
# MAGIC -- Latest tx type (windowed)
# MAGIC LEFT JOIN latest_mpsii_treatment_type lmt
# MAGIC     ON ep.PATIENT_ID = lmt.PATIENT_ID
# MAGIC
# MAGIC -- Latest claim HCP + enrichment (provider + HCO)
# MAGIC LEFT JOIN latest_claim_hcp_final lch
# MAGIC     ON ep.PATIENT_ID = lch.PATIENT_ID
# MAGIC -- LEFT JOIN latest_claim_hcp_visit_count_5yr lchvc
# MAGIC --     ON ep.PATIENT_ID = lchvc.PATIENT_ID
# MAGIC --    AND lch.latest_claim_hcp_npi = lchvc.latest_claim_hcp_npi
# MAGIC LEFT JOIN provider_dim pdlch
# MAGIC     ON lch.latest_claim_hcp_npi = pdlch.npi
# MAGIC LEFT JOIN cmpa_insights_internal_schema.reference_file_pooja_1703 ref1
# MAGIC     ON lch.latest_claim_hcp_npi = ref1.hcp_npi
# MAGIC
# MAGIC -- Latest tx HCP + enrichment (provider + HCO)
# MAGIC LEFT JOIN most_recent_tx_hcp_final mrt
# MAGIC     ON ep.PATIENT_ID = mrt.PATIENT_ID
# MAGIC -- LEFT JOIN latest_treatment_hcp_visit_count lthvc
# MAGIC --     ON ep.PATIENT_ID = lthvc.PATIENT_ID
# MAGIC --    AND mrt.latest_treatment_hcp_npi = lthvc.latest_treatment_hcp_npi
# MAGIC LEFT JOIN provider_dim pdtch
# MAGIC     ON mrt.latest_treatment_hcp_npi = pdtch.npi
# MAGIC LEFT JOIN cmpa_insights_internal_schema.reference_file_pooja_1703 ref2
# MAGIC     ON mrt.latest_treatment_hcp_npi = ref2.hcp_npi
# MAGIC
# MAGIC -- First Dx/Tx HCP attribution + stats
# MAGIC LEFT JOIN first_dx_hcp fdh
# MAGIC     ON ep.PATIENT_ID = fdh.PATIENT_ID
# MAGIC LEFT JOIN first_dx_hcp_5yr_stats fdhs
# MAGIC     ON ep.PATIENT_ID = fdhs.PATIENT_ID
# MAGIC LEFT JOIN first_tx_hcp fth
# MAGIC     ON ep.PATIENT_ID = fth.PATIENT_ID
# MAGIC LEFT JOIN first_tx_hcp_5yr_stats fths
# MAGIC     ON ep.PATIENT_ID = fths.PATIENT_ID
# MAGIC LEFT JOIN first_tx_hcp_5yr_tx_only fthtx
# MAGIC     ON ep.PATIENT_ID = fthtx.PATIENT_ID
# MAGIC
# MAGIC -- Pivoted top-5 most-seen HCPs
# MAGIC LEFT JOIN most_seen_pivot msp
# MAGIC     ON ep.PATIENT_ID = msp.PATIENT_ID
# MAGIC
# MAGIC ORDER BY ep.PATIENT_ID;
# MAGIC
# MAGIC -- Materialize the temp view into the persistent base table.
# MAGIC CREATE OR Replace TEMPORARY VIEW patient360_base AS
# MAGIC SELECT DISTINCT * FROM patient_hcp_visit_summary;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- mpsii_tx_claims (Temp View)
# MAGIC --
# MAGIC -- What this view is:
# MAGIC --   A curated “treatment claims” (Tx) universe for an MPS II eligible patient cohort.
# MAGIC --   It outputs Tx events (medical NDC, pharmacy NDC, and procedure administrations)
# MAGIC --   with an attributed HCP NPI when available, within the refresh window.
# MAGIC --
# MAGIC -- Output grain:
# MAGIC --   One row per (patient_id, npi-attribution, fill_date) treatment event.
# MAGIC --   Note: NPI can be NULL (kept intentionally).
# MAGIC --
# MAGIC -- Key inputs:
# MAGIC --   - com_edp_prd.com_raw.kom_medical_events
# MAGIC --   - com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC --   - com_raw.kom_providers  (for provider filtering)
# MAGIC --
# MAGIC -- Key logic:
# MAGIC --   1) Build eligible_patients using Dx evidence (E761/E763) + treatment evidence.
# MAGIC --   2) Define provider inclusion list (cohort_3_learnings) based on specialties.
# MAGIC --   3) Pull treatment events in refresh window and filter to included providers (or NULL NPI).
# MAGIC --
# MAGIC -- Parameters:
# MAGIC --   ${end_date} should be supplied by the runtime (Databricks widget/job parameter).
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW mpsii_tx_claims AS
# MAGIC WITH
# MAGIC -- ============================================================================
# MAGIC -- 1) Dx evidence for cohort building (Specified vs Unspecified)
# MAGIC --    These CTEs create Dx event streams used ONLY to count distinct Dx dates.
# MAGIC --    Window used here: 2020-08-01 → ${end_date}
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Specified Dx events:
# MAGIC --   Pull E761 diagnosis occurrences from:
# MAGIC --     (a) medical_events where DIAGNOSIS_CODES contains E761, using SERVICE_DATE
# MAGIC --     (b) pharmacy_events where DIAGNOSIS_CODE = E761 and transaction is PAID, using FILL_DATE
# MAGIC MPSII_Diagnoses_Specified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E761%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E761'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Specified Dx patients:
# MAGIC --   Keep patients with at least 2 distinct Dx dates (>=2 distinct fill_date values).
# MAGIC Patients_2Dx_Specified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Specified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- Unspecified Dx events:
# MAGIC --   Pull E763 diagnosis occurrences from medical + paid pharmacy, same as above.
# MAGIC MPSII_Diagnoses_Unspecified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E763%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E763'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Unspecified Dx patients:
# MAGIC --   Keep patients with at least 2 distinct Dx dates.
# MAGIC Patients_2Dx_Unspecified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Unspecified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 2) Treatment evidence for cohort building (refresh window only)
# MAGIC --    These CTEs do NOT output the final Tx universe; they’re used to confirm
# MAGIC --    that a patient has qualifying treatment evidence in the refresh window.
# MAGIC --    Window used here: 2023-08-01 → ${end_date}
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Treatment evidence (broad):
# MAGIC --   Patient qualifies if they have ANY of:
# MAGIC --     - Tivi NDCs in medical (NDC11) within refresh window
# MAGIC --     - Tivi NDCs in pharmacy within refresh window with PAID result
# MAGIC --     - Any procedure/admin codes in the provided procedure list within refresh window
# MAGIC MPSII_Treatment_All AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC         --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- Treatment evidence (Tivi-only):
# MAGIC --   Narrower evidence set for incremental unspecified cohort:
# MAGIC --     - Tivi NDCs (medical/pharmacy) in refresh window
# MAGIC --     - J1743 procedure in refresh window
# MAGIC MPSII_Treatment_Tivi_Only AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE = 'J1743'
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 3) Build eligible patient cohort
# MAGIC --    - Specified: >=2 E761 Dx dates AND ANY treatment evidence in refresh window
# MAGIC --    - Incremental unspecified: >=2 E763 Dx dates AND Tivi-only evidence
# MAGIC --      AND not already in specified+treatment cohort
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Specified cohort:
# MAGIC --   Patients who meet the “>=2 specified Dx dates” requirement
# MAGIC --   AND have at least one qualifying treatment event in refresh window.
# MAGIC Patients_2Dx_Specified_With_Treatment AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Specified p
# MAGIC     INNER JOIN MPSII_Treatment_All t USING (PATIENT_ID)
# MAGIC ),
# MAGIC
# MAGIC -- Incremental unspecified cohort:
# MAGIC --   Patients who meet the “>=2 unspecified Dx dates” requirement
# MAGIC --   AND have Tivi-only evidence in refresh window
# MAGIC --   AND are not in the specified+treatment cohort (avoids double-counting).
# MAGIC Patients_Incremental_Unspecified AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Unspecified p
# MAGIC     INNER JOIN MPSII_Treatment_Tivi_Only t USING (PATIENT_ID)
# MAGIC     WHERE p.PATIENT_ID NOT IN (SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment)
# MAGIC ),
# MAGIC
# MAGIC -- Final eligible cohort:
# MAGIC --   Union of specified+treatment cohort and incremental unspecified cohort.
# MAGIC eligible_patients AS (
# MAGIC     SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment
# MAGIC     UNION
# MAGIC     SELECT PATIENT_ID FROM Patients_Incremental_Unspecified
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 4) Provider inclusion filter (cohort_3_learnings)
# MAGIC --    Goal: Restrict attributed NPIs to INDIVIDUAL providers that meet specialty rules.
# MAGIC --    This filter will be applied AFTER pulling treatment events.
# MAGIC --    Note: rows with NULL NPI are retained to preserve treatment dates even without attribution.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC cohort_3_learnings AS (
# MAGIC   SELECT DISTINCT npi
# MAGIC   FROM com_raw.kom_providers
# MAGIC   WHERE provider_type = 'INDIVIDUAL'
# MAGIC     AND (
# MAGIC       -- Exclude a set of primary specialties considered out-of-scope/noise
# MAGIC       PRIMARY_SPECIALTY NOT IN (
# MAGIC         'Anesthesiologist Assistant',
# MAGIC         'Anesthesiology',
# MAGIC         'Dentist',
# MAGIC         'Dietitian, Registered',
# MAGIC         'Emergency Medical Technician, Basic',
# MAGIC         'Emergency Medicine',
# MAGIC         'General Acute Care Hospital',
# MAGIC         'Nurse Anesthetist, Certified Registered',
# MAGIC         'Obstetrics & Gynecology',
# MAGIC         'Pathology',
# MAGIC         'Radiology',
# MAGIC         'Urology'
# MAGIC       )
# MAGIC       -- OR explicitly include certain secondary specialties that are relevant
# MAGIC       OR SECONDARY_SPECIALTY IN (
# MAGIC         'Child & Adolescent Psychiatry',
# MAGIC         'Psychiatry',
# MAGIC         'Adolescent Medicine',
# MAGIC         'Developmental - Behavioral Pediatrics',
# MAGIC         'Neonatal-Perinatal Medicine',
# MAGIC         'Nutrition, Pediatric',
# MAGIC         'Oncology, Pediatrics',
# MAGIC         'Pediatric Cardiology',
# MAGIC         'Pediatric Critical Care Medicine',
# MAGIC         'Pediatric Dermatology',
# MAGIC         'Pediatric Emergency Medicine',
# MAGIC         'Pediatric Endocrinology',
# MAGIC         'Pediatric Gastroenterology',
# MAGIC         'Pediatric Hematology-Oncology',
# MAGIC         'Pediatric Infectious Diseases',
# MAGIC         'Pediatric Nephrology',
# MAGIC         'Pediatric Ophthalmology and Strabismus Specialist',
# MAGIC         'Pediatric Orthopaedic Surgery',
# MAGIC         'Pediatric Otolaryngology',
# MAGIC         'Pediatric Pulmonology',
# MAGIC         'Pediatric Radiology',
# MAGIC         'Pediatric Rehabilitation Medicine',
# MAGIC         'Pediatric Rheumatology',
# MAGIC         'Pediatric Surgery',
# MAGIC         'Pediatrics',
# MAGIC         'Clinical Biochemical Genetics',
# MAGIC         'Clinical Genetics (M.D.)',
# MAGIC         'Clinical Molecular Genetics',
# MAGIC         'Ph.D. Medical Genetics',
# MAGIC         'Neurodevelopmental Disabilities',
# MAGIC         'Neurology',
# MAGIC         'Neurology with Special Qualifications in Child Neurology',
# MAGIC         'Neuroradiology'
# MAGIC       )
# MAGIC     )
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 5) Treatment claims universe returned by the view (refresh/“2y” window)
# MAGIC --    Pull Tx events for eligible patients during 2023-08-01 → ${end_date}.
# MAGIC --    Sources and attribution rules:
# MAGIC --      A) Medical NDC events: NPI = COALESCE(rendering_npi, referring_npi), date = SERVICE_DATE
# MAGIC --      B) Pharmacy NDC events: NPI = prescriber_npi, date = FILL_DATE, PAID only
# MAGIC --      C) Procedure events:    NPI = rendering_npi, date = SERVICE_DATE
# MAGIC --    Then apply provider filter:
# MAGIC --      - keep if NPI in cohort_3_learnings OR NPI is NULL
# MAGIC -- ============================================================================
# MAGIC
# MAGIC all_tx_claims_2yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       -- Medical: Tivi NDC events with attributed HCP (rendering/referring)
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC         SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       UNION
# MAGIC
# MAGIC       -- Pharmacy: Tivi NDC fills with attributed prescriber (paid only)
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         PRESCRIBER_NPI AS NPI,
# MAGIC         FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC       -- UNION
# MAGIC
# MAGIC       -- -- Medical: procedure/admin events with attributed renderer
# MAGIC       -- SELECT DISTINCT
# MAGIC       --   PATIENT_ID,
# MAGIC       --   RENDERING_NPI AS NPI,
# MAGIC       --   SERVICE_DATE AS FILL_DATE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     -- Provider specialty filter: keep included providers OR keep NULL NPI rows
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC )
# MAGIC
# MAGIC -- Final output: all treatment events in refresh window for the eligible cohort
# MAGIC SELECT * FROM all_tx_claims_2yr;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- most_recently_treated_hcp (Temp View)
# MAGIC --
# MAGIC -- What this view is:
# MAGIC --   Patient-level attribution of the “most recently treating HCP” within the
# MAGIC --   REFRESH treatment window (2023-08-01 → ${end_date}), plus HCP enrichment and
# MAGIC --   visit context metrics computed over a broader (5y-ish) claims universe.
# MAGIC --
# MAGIC -- Output grain:
# MAGIC --   One row per patient (only patients with an attributable treatment NPI in the
# MAGIC --   refresh window will appear, because latest_treating_hcp filters npi IS NOT NULL).
# MAGIC --
# MAGIC -- Key concepts:
# MAGIC --   - Selection window ("most recently treated"): tx_claims (refresh window)
# MAGIC --   - Context window ("visits / last seen"): all_claims (currently 2020-08-01 → ${end_date})
# MAGIC --   - Provider filter (cohort_3_learnings): keeps included INDIVIDUAL NPIs; retains NULL NPI rows
# MAGIC --     in claims universes, but final attribution requires non-null NPI.
# MAGIC --
# MAGIC -- Parameters:
# MAGIC --   ${end_date} must be provided by the runtime.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW most_recently_treated_hcp AS
# MAGIC WITH
# MAGIC -- ============================================================================
# MAGIC -- 1) Treatment claims: refresh window source
# MAGIC --    This CTE simply points to the already-built tx universe from mpsii_tx_claims:
# MAGIC --      - eligible cohort already applied
# MAGIC --      - window already applied (2023-08-01 → ${end_date})
# MAGIC --      - provider filter already applied (allowed NPIs + NULL NPIs)
# MAGIC -- ============================================================================
# MAGIC tx_claims AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM mpsii_tx_claims
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 2) Re-derive eligible_patients cohort (duplicated here)
# MAGIC --    This block repeats the cohort logic so that the subsequent 5y Dx/Tx universes
# MAGIC --    (all_dx_claims_5yr / all_tx_claims_5yr) can be built inside this view.
# MAGIC --    NOTE: This duplication is intentional in the current script; no logic is changed.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Specified Dx event stream (E761) within 2020-08-01 → ${end_date}
# MAGIC MPSII_Diagnoses_Specified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E761%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E761'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Specified Dx patients: require ≥2 distinct Dx dates
# MAGIC Patients_2Dx_Specified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Specified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- Unspecified Dx event stream (E763) within 2020-08-01 → ${end_date}
# MAGIC MPSII_Diagnoses_Unspecified AS (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E763%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E763'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC ),
# MAGIC
# MAGIC -- Unspecified Dx patients: require ≥2 distinct Dx dates
# MAGIC Patients_2Dx_Unspecified AS (
# MAGIC     SELECT PATIENT_ID
# MAGIC     FROM MPSII_Diagnoses_Unspecified
# MAGIC     GROUP BY PATIENT_ID
# MAGIC     HAVING COUNT(DISTINCT FILL_DATE) >= 2
# MAGIC ),
# MAGIC
# MAGIC -- Qualifying treatment evidence (broad) in refresh window, used to ensure “active treatment”
# MAGIC MPSII_Treatment_All AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC         --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- Tivi-only evidence in refresh window (used only for incremental unspecified cohort)
# MAGIC MPSII_Treatment_Tivi_Only AS (
# MAGIC     SELECT DISTINCT PATIENT_ID FROM (
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         UNION ALL
# MAGIC         SELECT DISTINCT PATIENT_ID
# MAGIC         FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC         WHERE NDC11 IN ('8497600101')
# MAGIC           AND TRANSACTION_RESULT = 'PAID'
# MAGIC           AND FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         -- UNION ALL
# MAGIC         -- SELECT DISTINCT PATIENT_ID
# MAGIC         -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC         -- WHERE PROCEDURE_CODE = 'J1743'
# MAGIC         --   AND SERVICE_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     ) t
# MAGIC ),
# MAGIC
# MAGIC -- Specified eligible: ≥2 specified Dx dates AND any qualifying treatment in refresh window
# MAGIC Patients_2Dx_Specified_With_Treatment AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Specified p
# MAGIC     INNER JOIN MPSII_Treatment_All t USING (PATIENT_ID)
# MAGIC ),
# MAGIC
# MAGIC -- Incremental unspecified eligible: ≥2 unspecified Dx dates AND Tivi-only tx in refresh window,
# MAGIC -- excluding already eligible specified+treatment patients
# MAGIC Patients_Incremental_Unspecified AS (
# MAGIC     SELECT DISTINCT p.PATIENT_ID
# MAGIC     FROM Patients_2Dx_Unspecified p
# MAGIC     INNER JOIN MPSII_Treatment_Tivi_Only t USING (PATIENT_ID)
# MAGIC     WHERE p.PATIENT_ID NOT IN (SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment)
# MAGIC ),
# MAGIC
# MAGIC -- Final eligible cohort used downstream for 5y Dx/Tx universes
# MAGIC eligible_patients AS (
# MAGIC     SELECT PATIENT_ID FROM Patients_2Dx_Specified_With_Treatment
# MAGIC     UNION
# MAGIC     SELECT PATIENT_ID FROM Patients_Incremental_Unspecified
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 3) Provider filter (cohort_3_learnings)
# MAGIC --    Defines an allowed set of INDIVIDUAL NPIs based on specialty inclusion rules.
# MAGIC --    Applied to all_dx_claims_5yr / all_tx_claims_5yr, with NULL NPIs retained.
# MAGIC -- ============================================================================
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
# MAGIC -- ============================================================================
# MAGIC -- 4) Build 5y-ish claims universes for visit counts / last-visit context
# MAGIC --    These are bounded by 2020-08-01 → ${end_date} and filtered to eligible_patients.
# MAGIC --    Provider filter is applied (allowed NPIs + NULL NPIs retained).
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Dx claims in 5y-ish window (E761/E763)
# MAGIC all_dx_claims_5yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC         SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE (DIAGNOSIS_CODES LIKE '%E761%' OR DIAGNOSIS_CODES LIKE '%E763%')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         PRESCRIBER_NPI AS NPI,
# MAGIC         FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE DIAGNOSIS_CODE IN ('E761','E763')
# MAGIC         AND TRANSACTION_STATUS = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- Tx claims in 5y-ish window (Tivi NDCs + procedure list)
# MAGIC all_tx_claims_5yr AS (
# MAGIC     SELECT DISTINCT *
# MAGIC     FROM (
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC         SERVICE_DATE AS FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       UNION
# MAGIC       SELECT DISTINCT
# MAGIC         PATIENT_ID,
# MAGIC         PRESCRIBER_NPI AS NPI,
# MAGIC         FILL_DATE
# MAGIC       FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC       WHERE NDC11 IN ('8497600101')
# MAGIC         AND TRANSACTION_RESULT = 'PAID'
# MAGIC         AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC         AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC       -- UNION
# MAGIC       -- SELECT DISTINCT
# MAGIC       --   PATIENT_ID,
# MAGIC       --   RENDERING_NPI AS NPI,
# MAGIC       --   SERVICE_DATE AS FILL_DATE
# MAGIC       -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC       -- WHERE PROCEDURE_CODE IN ('99601','99602','96365','96366','J1743','S9357','S9379',
# MAGIC       --                          '38206','38230','38232','38240','38241','38242','38243','38250')
# MAGIC       --   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC       --   AND PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC     )
# MAGIC     WHERE npi IN (SELECT npi FROM cohort_3_learnings) OR npi IS NULL
# MAGIC ),
# MAGIC
# MAGIC -- Combined claims universe used only for:
# MAGIC --   - counting distinct visit dates per patient↔HCP
# MAGIC --   - computing last observed visit date per patient↔HCP
# MAGIC all_claims AS (
# MAGIC     SELECT * FROM all_dx_claims_5yr
# MAGIC     UNION
# MAGIC     SELECT * FROM all_tx_claims_5yr
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 5) Select the most recently treating HCP (refresh window)
# MAGIC --    Uses tx_claims (2023-08-01 → ${end_date}) to pick 1 NPI per patient:
# MAGIC --      - Prefer rows with non-null NPI
# MAGIC --      - Then pick the latest fill_date
# MAGIC --      - Tie-break by npi DESC (deterministic tie-breaker)
# MAGIC --    Final output from this CTE requires npi IS NOT NULL (attributable HCP).
# MAGIC -- ============================================================================
# MAGIC latest_treating_hcp AS (
# MAGIC     SELECT patient_id, npi
# MAGIC     FROM (
# MAGIC         SELECT
# MAGIC             patient_id,
# MAGIC             npi,
# MAGIC             fill_date,
# MAGIC             ROW_NUMBER() OVER (
# MAGIC                 PARTITION BY patient_id
# MAGIC                 ORDER BY
# MAGIC                     CASE WHEN npi IS NOT NULL THEN 1 ELSE 2 END,  -- prioritize attributed claims
# MAGIC                     fill_date DESC,                               -- most recent date wins
# MAGIC                     npi DESC                                      -- deterministic tie-break
# MAGIC             ) AS rn
# MAGIC         FROM tx_claims
# MAGIC     ) t
# MAGIC     WHERE rn = 1
# MAGIC       AND npi IS NOT NULL
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 6) Compute visit metrics for the selected patient↔HCP pair (5y-ish universe)
# MAGIC --    These metrics are NOT limited to the refresh window; they use all_claims.
# MAGIC -- ============================================================================
# MAGIC
# MAGIC -- Count of distinct visit dates for each patient↔HCP across all_claims
# MAGIC visit_counts AS (
# MAGIC     SELECT
# MAGIC         patient_id,
# MAGIC         npi,
# MAGIC         COUNT(DISTINCT fill_date) AS visit_counts
# MAGIC     FROM all_claims
# MAGIC     WHERE npi IS NOT NULL
# MAGIC     GROUP BY patient_id, npi
# MAGIC ),
# MAGIC
# MAGIC -- Last observed visit date for each selected patient↔HCP across all_claims
# MAGIC last_visit_date AS (
# MAGIC     SELECT
# MAGIC         lth.patient_id,
# MAGIC         lth.npi,
# MAGIC         MAX(ac.fill_date) AS last_visit_date
# MAGIC     FROM latest_treating_hcp lth
# MAGIC     LEFT JOIN all_claims ac
# MAGIC       ON lth.patient_id = ac.patient_id
# MAGIC      AND lth.npi = ac.npi
# MAGIC     GROUP BY lth.patient_id, lth.npi
# MAGIC ),
# MAGIC
# MAGIC -- Attach visit metrics to the most recently treated HCP per patient
# MAGIC latest_treating_hcp_with_visits AS (
# MAGIC     SELECT
# MAGIC         a.patient_id,
# MAGIC         a.npi AS most_recently_treated_hcp,
# MAGIC         b.visit_counts AS no_of_visits,
# MAGIC         c.last_visit_date AS last_visit_date_5yr
# MAGIC     FROM latest_treating_hcp AS a
# MAGIC     LEFT JOIN visit_counts AS b
# MAGIC         ON a.patient_id = b.patient_id
# MAGIC        AND a.npi        = b.npi
# MAGIC     LEFT JOIN last_visit_date AS c
# MAGIC         ON a.patient_id = c.patient_id
# MAGIC        AND a.npi        = c.npi
# MAGIC ),
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- 7) Enrichment: provider name/specialty + HCO + territory/region mapping
# MAGIC --    - provider info from kom_providers (INDIVIDUAL)
# MAGIC --    - HCO / territory / region crosswalk from reference_file_pooja_1703
# MAGIC --      with additional territory_id/region_id derived via zip_to_territory_mapping
# MAGIC -- ============================================================================
# MAGIC hcp_with_other_info AS (
# MAGIC     SELECT
# MAGIC         a.*,
# MAGIC         CONCAT(b.FIRST_NAME, ' ', b.LAST_NAME) AS hcp_name,
# MAGIC         b.PRIMARY_SPECIALTY AS hcp_specialty,
# MAGIC         -- c.hco_npi,
# MAGIC         c.hco_name,
# MAGIC         c.territory_id,
# MAGIC         c.territory,
# MAGIC         c.region_id,
# MAGIC         c.region
# MAGIC     FROM latest_treating_hcp_with_visits AS a
# MAGIC
# MAGIC     -- Provider name and specialty enrichment (limit to INDIVIDUAL provider rows)
# MAGIC     LEFT JOIN com_edp_prd.com_raw.kom_providers AS b
# MAGIC         ON a.most_recently_treated_hcp = b.NPI
# MAGIC        AND b.PROVIDER_TYPE = 'INDIVIDUAL'
# MAGIC
# MAGIC     -- Crosswalk HCP -> HCO and attach territory/region metadata.
# MAGIC     -- Inner derived tables map territory_name/region_name to numeric IDs.
# MAGIC     LEFT JOIN (
# MAGIC       SELECT
# MAGIC         * EXCEPT (hcp_primary_specialty),
# MAGIC         hcp_primary_specialty AS hcp_specialty
# MAGIC       FROM (
# MAGIC         SELECT
# MAGIC           a.*,
# MAGIC           b.territory_id,
# MAGIC           c.region_id
# MAGIC         FROM cmpa_insights_internal_schema.reference_file_pooja_1703 AS a
# MAGIC         LEFT JOIN (
# MAGIC           SELECT DISTINCT territory_id, territory_name
# MAGIC           FROM cmpa_insights_internal_schema.zip_to_territory_mapping
# MAGIC         ) AS b
# MAGIC           ON a.territory = b.territory_name
# MAGIC         LEFT JOIN (
# MAGIC           SELECT DISTINCT region_id, region_name
# MAGIC           FROM cmpa_insights_internal_schema.zip_to_territory_mapping
# MAGIC         ) AS c
# MAGIC           ON a.region = c.region_name
# MAGIC       )
# MAGIC     ) AS c
# MAGIC         ON a.most_recently_treated_hcp = c.hcp_npi
# MAGIC )
# MAGIC
# MAGIC -- ============================================================================
# MAGIC -- FINAL SELECT
# MAGIC --   Renames fields to make explicit:
# MAGIC --     - selection window: “_2yr” (refresh window)
# MAGIC --     - context metrics: “_5yr” (computed from all_claims 2020-08-01 → ${end_date})
# MAGIC -- ============================================================================
# MAGIC SELECT
# MAGIC     patient_id,
# MAGIC     most_recently_treated_hcp AS most_recently_treated_hcp_2yr,
# MAGIC     hcp_name AS most_recently_treated_hcp_name_2yr,
# MAGIC
# MAGIC     -- Visit metrics are computed across all_claims (currently 2020-08-01 → ${end_date})
# MAGIC     no_of_visits AS most_recently_treated_hcp_2yr_no_of_visits_5yr,
# MAGIC     last_visit_date_5yr AS most_recent_tx_hcp_2yr_last_visit_5yr,
# MAGIC
# MAGIC     hcp_specialty AS most_recently_treated_hcp_specialty_2yr,
# MAGIC     -- hco_npi AS most_recently_treated_hcp_hco_npi,
# MAGIC     hco_name AS most_recently_treated_hcp_hco_name,
# MAGIC     territory_id AS most_recently_treated_hcp_territory_id_2yr,
# MAGIC     territory AS most_recently_treated_hcp_territory_2yr,
# MAGIC     region_id AS most_recently_treated_hcp_region_id_2yr,
# MAGIC     region AS most_recently_treated_hcp_region_2yr
# MAGIC FROM hcp_with_other_info;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- Primary HCP Assignment (Dx + Tx Claims)
# MAGIC --
# MAGIC -- Goal
# MAGIC --   Assign ONE “primary HCP” (NPI) per eligible MPS II patient by ranking HCPs using:
# MAGIC --     Tier 1) Specialty priority
# MAGIC --     Tier 2) Total distinct visit dates (Dx + Tx combined)
# MAGIC --     Tier 3) Most recent visit date
# MAGIC --     Tier 4) NPI tiebreaker
# MAGIC --
# MAGIC -- Date windows used in this script
# MAGIC --   • Dx claim extraction (“5y” universe): 2020-08-01 → ${end_date}
# MAGIC --   • Tx claim extraction (“5y” universe): 2020-08-01 → ${end_date}
# MAGIC --   • Tx eligibility window (“2y/refresh”): 2023-08-01 → ${end_date}
# MAGIC --
# MAGIC -- NPI attribution (aligned to GTM file)
# MAGIC --   • Medical NDC claims:      COALESCE(RENDERING_NPI, REFERRING_NPI)
# MAGIC --   • Medical procedure claims:RENDERING_NPI
# MAGIC --   • Pharmacy claims:         PRESCRIBER_NPI
# MAGIC --
# MAGIC -- Eligibility overview (eligible_patients)
# MAGIC --   • Specified cohort:
# MAGIC --       - ≥2 distinct E761 Dx dates (medical or paid pharmacy) in Dx window
# MAGIC --       - AND any Tx in the 2y/refresh window
# MAGIC --   • Incremental unspecified cohort:
# MAGIC --       - ≥2 distinct E763 Dx dates (medical or paid pharmacy) in Dx window
# MAGIC --       - AND Tivi-coded Tx in the 2y/refresh window (NDCs or J1743)
# MAGIC --       - Excludes patients already in specified cohort
# MAGIC -- =============================================================================
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 1: DIAGNOSIS CLAIMS (Dx universe for visit counting)
# MAGIC --   • Captures E761/E763 diagnosis evidence from medical + paid pharmacy events
# MAGIC --   • Window: 2020-08-01 → ${end_date}
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW all_dx_claims AS
# MAGIC
# MAGIC -- Medical events Dx (NPI = COALESCE(rendering, referring))
# MAGIC SELECT DISTINCT
# MAGIC     PATIENT_ID,
# MAGIC     COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC     SERVICE_DATE AS FILL_DATE,
# MAGIC     'DX' AS CLAIM_TYPE
# MAGIC FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC WHERE (DIAGNOSIS_CODES LIKE '%E761%' OR DIAGNOSIS_CODES LIKE '%E763%')
# MAGIC   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- Pharmacy events Dx (NPI = prescriber_npi; paid only)
# MAGIC SELECT DISTINCT
# MAGIC     PATIENT_ID,
# MAGIC     PRESCRIBER_NPI AS NPI,
# MAGIC     FILL_DATE,
# MAGIC     'DX' AS CLAIM_TYPE
# MAGIC FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC WHERE DIAGNOSIS_CODE IN ('E761', 'E763')
# MAGIC   AND TRANSACTION_STATUS = 'PAID'
# MAGIC   AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters);
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 2: TREATMENT CLAIMS (Tx universe for visit counting)
# MAGIC --   • Captures Tivi-coded treatment from medical NDC, medical procedures, and paid pharmacy NDC
# MAGIC --   • Window: 2020-08-01 → ${end_date}
# MAGIC --   • Includes CODE field to retain NDC/procedure provenance
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW all_tx_claims AS
# MAGIC
# MAGIC -- Medical events Tx via NDC (NPI = COALESCE(rendering, referring))
# MAGIC SELECT DISTINCT
# MAGIC     PATIENT_ID,
# MAGIC     COALESCE(RENDERING_NPI, REFERRING_NPI) AS NPI,
# MAGIC     SERVICE_DATE AS FILL_DATE,
# MAGIC     'TX' AS CLAIM_TYPE,
# MAGIC     NDC11 AS CODE
# MAGIC FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC WHERE NDC11 IN ('8497600101')
# MAGIC   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC
# MAGIC -- UNION
# MAGIC
# MAGIC -- -- Medical events Tx via procedures (NPI = rendering_npi only)
# MAGIC -- SELECT DISTINCT
# MAGIC --     PATIENT_ID,
# MAGIC --     RENDERING_NPI AS NPI,
# MAGIC --     SERVICE_DATE AS FILL_DATE,
# MAGIC --     'TX' AS CLAIM_TYPE,
# MAGIC --     PROCEDURE_CODE AS CODE
# MAGIC -- FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC -- WHERE PROCEDURE_CODE IN ('99601', '99602', '96365', '96366', 'J1743',
# MAGIC --                          'S9357', 'S9379', '38206', '38230', '38232',
# MAGIC --                          '38240', '38241', '38242', '38243', '38250')
# MAGIC --   AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC
# MAGIC UNION
# MAGIC
# MAGIC -- Pharmacy events Tx via NDC (NPI = prescriber_npi; paid only)
# MAGIC SELECT DISTINCT
# MAGIC     PATIENT_ID,
# MAGIC     PRESCRIBER_NPI AS NPI,
# MAGIC     FILL_DATE,
# MAGIC     'TX' AS CLAIM_TYPE,
# MAGIC     NDC11 AS CODE
# MAGIC FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC WHERE NDC11 IN ('8497600101')
# MAGIC   AND TRANSACTION_RESULT = 'PAID'
# MAGIC   AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters);
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 3: Tx CLAIMS IN ELIGIBILITY WINDOW (2y/refresh)
# MAGIC --   • Subset of all_tx_claims restricted to 2023-08-01 → ${end_date}
# MAGIC --   • Used ONLY to determine cohort eligibility (not for visit counting tiers)
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW tx_claims_2yr AS
# MAGIC SELECT DISTINCT PATIENT_ID, NPI, FILL_DATE, CLAIM_TYPE, CODE
# MAGIC FROM all_tx_claims
# MAGIC WHERE FILL_DATE BETWEEN '2023-08-01' AND (SELECT end_date FROM runtime_parameters);
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 4: PATIENT ELIGIBILITY
# MAGIC --   Builds eligible_patients using Dx evidence (≥2 dates) + Tx evidence in 2y window.
# MAGIC -- =============================================================================
# MAGIC
# MAGIC -- 4A) Specified Dx requirement: ≥2 distinct E761 Dx dates in Dx window
# MAGIC CREATE OR REPLACE TEMPORARY VIEW e761_patients_2dx AS
# MAGIC SELECT PATIENT_ID
# MAGIC FROM (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E761%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E761'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC )
# MAGIC GROUP BY PATIENT_ID
# MAGIC HAVING COUNT(DISTINCT FILL_DATE) >= 2;
# MAGIC
# MAGIC -- 4B) Specified cohort: specified Dx + any Tx in 2y/refresh window
# MAGIC CREATE OR REPLACE TEMPORARY VIEW specified_patients AS
# MAGIC SELECT DISTINCT e.PATIENT_ID
# MAGIC FROM e761_patients_2dx e
# MAGIC INNER JOIN tx_claims_2yr t ON e.PATIENT_ID = t.PATIENT_ID;
# MAGIC
# MAGIC -- 4C) Incremental Dx requirement: ≥2 distinct E763 Dx dates in Dx window
# MAGIC CREATE OR REPLACE TEMPORARY VIEW e763_patients_2dx AS
# MAGIC SELECT PATIENT_ID
# MAGIC FROM (
# MAGIC     SELECT DISTINCT PATIENT_ID, SERVICE_DATE AS FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_medical_events
# MAGIC     WHERE DIAGNOSIS_CODES LIKE '%E763%'
# MAGIC       AND SERVICE_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC     UNION
# MAGIC     SELECT DISTINCT PATIENT_ID, FILL_DATE
# MAGIC     FROM com_edp_prd.com_raw.kom_pharmacy_events
# MAGIC     WHERE DIAGNOSIS_CODE = 'E763'
# MAGIC       AND TRANSACTION_STATUS = 'PAID'
# MAGIC       AND FILL_DATE BETWEEN '2020-08-01' AND (SELECT end_date FROM runtime_parameters)
# MAGIC )
# MAGIC GROUP BY PATIENT_ID
# MAGIC HAVING COUNT(DISTINCT FILL_DATE) >= 2;
# MAGIC
# MAGIC -- 4D) Tivi-coded Tx requirement for incremental eligibility (2y/refresh window)
# MAGIC CREATE OR REPLACE TEMPORARY VIEW Tivi_tx_2yr AS
# MAGIC SELECT DISTINCT PATIENT_ID
# MAGIC FROM tx_claims_2yr
# MAGIC WHERE CODE IN ('8497600101');
# MAGIC
# MAGIC -- 4E) Incremental cohort: incremental Dx + Tivi-coded tx in 2y window + exclude specified
# MAGIC CREATE OR REPLACE TEMPORARY VIEW incremental_patients AS
# MAGIC SELECT DISTINCT e.PATIENT_ID
# MAGIC FROM e763_patients_2dx e
# MAGIC INNER JOIN Tivi_tx_2yr t ON e.PATIENT_ID = t.PATIENT_ID
# MAGIC WHERE e.PATIENT_ID NOT IN (SELECT PATIENT_ID FROM specified_patients);
# MAGIC
# MAGIC -- 4F) Final eligible cohort
# MAGIC CREATE OR REPLACE TEMPORARY VIEW eligible_patients AS
# MAGIC SELECT PATIENT_ID FROM specified_patients
# MAGIC UNION
# MAGIC SELECT PATIENT_ID FROM incremental_patients;
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- Provider inclusion list (INDIVIDUAL NPIs only)
# MAGIC --   • Used to restrict HCPs considered for primary assignment
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW cohort_3_learnings AS
# MAGIC SELECT DISTINCT npi
# MAGIC FROM com_raw.kom_providers
# MAGIC WHERE provider_type = 'INDIVIDUAL'
# MAGIC   AND (
# MAGIC     PRIMARY_SPECIALTY NOT IN (
# MAGIC       'Anesthesiologist Assistant','Anesthesiology','Dentist','Dietitian, Registered',
# MAGIC       'Emergency Medical Technician, Basic','Emergency Medicine','General Acute Care Hospital',
# MAGIC       'Nurse Anesthetist, Certified Registered','Obstetrics & Gynecology','Pathology',
# MAGIC       'Radiology','Urology'
# MAGIC     )
# MAGIC     OR SECONDARY_SPECIALTY IN (
# MAGIC       'Child & Adolescent Psychiatry','Psychiatry','Adolescent Medicine','Developmental - Behavioral Pediatrics',
# MAGIC       'Neonatal-Perinatal Medicine','Nutrition, Pediatric','Oncology, Pediatrics','Pediatric Cardiology',
# MAGIC       'Pediatric Critical Care Medicine','Pediatric Dermatology','Pediatric Emergency Medicine',
# MAGIC       'Pediatric Endocrinology','Pediatric Gastroenterology','Pediatric Hematology-Oncology',
# MAGIC       'Pediatric Infectious Diseases','Pediatric Nephrology','Pediatric Ophthalmology and Strabismus Specialist',
# MAGIC       'Pediatric Orthopaedic Surgery','Pediatric Otolaryngology','Pediatric Pulmonology','Pediatric Radiology',
# MAGIC       'Pediatric Rehabilitation Medicine','Pediatric Rheumatology','Pediatric Surgery','Pediatrics',
# MAGIC       'Clinical Biochemical Genetics','Clinical Genetics (M.D.)','Clinical Molecular Genetics',
# MAGIC       'Ph.D. Medical Genetics','Neurodevelopmental Disabilities','Neurology',
# MAGIC       'Neurology with Special Qualifications in Child Neurology','Neuroradiology'
# MAGIC     )
# MAGIC   );
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 5: COMBINED Dx + Tx CLAIMS FOR ELIGIBLE PATIENTS
# MAGIC --   • Combines Dx + Tx events (visit dates) for eligible patients only
# MAGIC --   • Filters to included INDIVIDUAL NPIs (cohort_3_learnings)
# MAGIC --   • NOTE: As written, this excludes NULL NPI rows (because of the IN filter)
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW all_patient_claims AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC   -- Dx claims
# MAGIC   SELECT PATIENT_ID, NPI, FILL_DATE, CLAIM_TYPE
# MAGIC   FROM all_dx_claims
# MAGIC   WHERE PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC
# MAGIC   UNION
# MAGIC
# MAGIC   -- Tx claims
# MAGIC   SELECT PATIENT_ID, NPI, FILL_DATE, CLAIM_TYPE
# MAGIC   FROM all_tx_claims
# MAGIC   WHERE PATIENT_ID IN (SELECT PATIENT_ID FROM eligible_patients)
# MAGIC )
# MAGIC WHERE npi IN (SELECT DISTINCT npi FROM cohort_3_learnings);
# MAGIC
# MAGIC
# MAGIC -- =============================================================================
# MAGIC -- STEP 6: PRIMARY HCP ASSIGNMENT (4-tier ranking)
# MAGIC --   Tier 1: Specialty priority bucket (lower = better)
# MAGIC --   Tier 2: Total distinct visit dates (Dx + Tx)
# MAGIC --   Tier 3: Most recent visit date
# MAGIC --   Tier 4: NPI tiebreaker
# MAGIC -- =============================================================================
# MAGIC CREATE OR REPLACE TEMPORARY VIEW primary_hcp AS
# MAGIC WITH hcp_metrics AS (
# MAGIC     SELECT
# MAGIC         a.PATIENT_ID,
# MAGIC         a.NPI,
# MAGIC
# MAGIC         -- Specialty bucket (reporting label)
# MAGIC         CASE
# MAGIC             WHEN p.primary_specialty LIKE '%Genetic%'
# MAGIC               OR p.secondary_specialty LIKE '%Genetic%'
# MAGIC                 THEN 'Geneticist'
# MAGIC             WHEN p.primary_specialty LIKE '%Psychiatry & Neurology%'
# MAGIC               OR p.secondary_specialty LIKE '%Neurodevelopmental Disabilities%'
# MAGIC               OR p.primary_specialty LIKE '%Neurological Surgery%'
# MAGIC                 THEN 'Psychiatry & Neurology'
# MAGIC             WHEN p.primary_specialty LIKE '%Pediatrics%'
# MAGIC                 THEN 'Pediatrician'
# MAGIC             WHEN p.primary_specialty LIKE '%Internal Medicine%'
# MAGIC               OR p.secondary_specialty LIKE '%Internal Medicine%'
# MAGIC               OR p.primary_specialty LIKE '%Family Medicine%'
# MAGIC               OR p.secondary_specialty LIKE '%Family Medicine%'
# MAGIC                 THEN 'PCP'
# MAGIC             WHEN p.primary_specialty LIKE '%Nurse Practitioner%'
# MAGIC               OR p.primary_specialty LIKE '%Physician Assistant%'
# MAGIC                 THEN 'NPPA'
# MAGIC             WHEN a.NPI IS NULL
# MAGIC                 THEN 'NA'
# MAGIC             ELSE 'Others'
# MAGIC         END AS SPECIALTY,
# MAGIC
# MAGIC         -- Tier 1: specialty priority (lower is better)
# MAGIC         CASE
# MAGIC             WHEN p.primary_specialty LIKE '%Genetic%'
# MAGIC               OR p.secondary_specialty LIKE '%Genetic%'
# MAGIC                 THEN 1
# MAGIC             WHEN p.primary_specialty LIKE '%Psychiatry & Neurology%'
# MAGIC               OR p.secondary_specialty LIKE '%Neurodevelopmental Disabilities%'
# MAGIC               OR p.primary_specialty LIKE '%Neurological Surgery%'
# MAGIC                 THEN 2
# MAGIC             WHEN p.primary_specialty LIKE '%Pediatrics%'
# MAGIC                 THEN 3
# MAGIC             WHEN p.primary_specialty LIKE '%Internal Medicine%'
# MAGIC               OR p.secondary_specialty LIKE '%Internal Medicine%'
# MAGIC               OR p.primary_specialty LIKE '%Family Medicine%'
# MAGIC               OR p.secondary_specialty LIKE '%Family Medicine%'
# MAGIC                 THEN 4
# MAGIC             WHEN p.primary_specialty LIKE '%Nurse Practitioner%'
# MAGIC               OR p.primary_specialty LIKE '%Physician Assistant%'
# MAGIC                 THEN 5
# MAGIC             WHEN a.NPI IS NULL
# MAGIC                 THEN 7
# MAGIC             ELSE 6
# MAGIC         END AS SPECIALTY_PRIORITY,
# MAGIC
# MAGIC         -- Tier 2: visit counts (Dx + Tx combined)
# MAGIC         COUNT(DISTINCT a.FILL_DATE) AS NO_OF_VISITS,
# MAGIC
# MAGIC         -- Reference-only breakdowns (not used in ranking)
# MAGIC         COUNT(DISTINCT CASE WHEN a.CLAIM_TYPE = 'DX' THEN a.FILL_DATE END) AS DX_VISITS,
# MAGIC         COUNT(DISTINCT CASE WHEN a.CLAIM_TYPE = 'TX' THEN a.FILL_DATE END) AS TX_VISITS,
# MAGIC
# MAGIC         -- Tier 3: most recent visit date
# MAGIC         MAX(a.FILL_DATE) AS MOST_RECENT_VISIT
# MAGIC
# MAGIC     FROM all_patient_claims a
# MAGIC     LEFT JOIN com_edp_prd.com_raw.kom_providers p
# MAGIC         ON a.NPI = p.NPI
# MAGIC     GROUP BY
# MAGIC         a.PATIENT_ID,
# MAGIC         a.NPI,
# MAGIC         p.primary_specialty,
# MAGIC         p.secondary_specialty
# MAGIC ),
# MAGIC ranked_hcps AS (
# MAGIC     SELECT
# MAGIC         *,
# MAGIC         RANK() OVER (
# MAGIC             PARTITION BY PATIENT_ID
# MAGIC             ORDER BY
# MAGIC                 SPECIALTY_PRIORITY ASC,
# MAGIC                 NO_OF_VISITS DESC,
# MAGIC                 MOST_RECENT_VISIT DESC,
# MAGIC                 NPI ASC
# MAGIC         ) AS HCP_RANK
# MAGIC     FROM hcp_metrics
# MAGIC )
# MAGIC SELECT
# MAGIC     PATIENT_ID,
# MAGIC     NPI AS PRIMARY_HCP_NPI,
# MAGIC     SPECIALTY AS PRIMARY_HCP_SPECIALTY,
# MAGIC     SPECIALTY_PRIORITY,
# MAGIC     NO_OF_VISITS,
# MAGIC     DX_VISITS,
# MAGIC     TX_VISITS,
# MAGIC     MOST_RECENT_VISIT,
# MAGIC     HCP_RANK
# MAGIC FROM ranked_hcps
# MAGIC WHERE HCP_RANK = 1;
# MAGIC
# MAGIC -- Optional materialization:
# MAGIC -- CREATE OR REPLACE TABLE com_edp_prd.cmpa_insights_internal_schema.primary_hcp AS
# MAGIC -- SELECT * FROM primary_hcp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- =============================================================================
# MAGIC -- STEP 7: Materialize Primary HCP table with HCP identity + HCO/territory metadata
# MAGIC --
# MAGIC -- What this step does:
# MAGIC --   Takes the existing primary_hcp assignment (already computed upstream)
# MAGIC --   and persists it as a physical table, while enriching it with:
# MAGIC --     - HCP full name (from kom_providers)
# MAGIC --     - HCO affiliation + territory/region attributes (from reference_file_pooja_1703)
# MAGIC --     - territory_id / region_id (derived via zip_to_territory_mapping lookups)
# MAGIC --
# MAGIC -- What this step does NOT do:
# MAGIC --   - It does not re-rank or change the primary HCP selection logic.
# MAGIC --   - It does not filter the cohort; it simply enriches and stores primary_hcp rows.
# MAGIC --
# MAGIC -- Output:
# MAGIC --   com_edp_prd.cmpa_insights_internal_schema.primary_hcp
# MAGIC -- =============================================================================
# MAGIC
# MAGIC CREATE OR REPLACE TEMPORARY VIEW primary_hcp_base AS
# MAGIC SELECT
# MAGIC     ph.*,  -- retain all columns produced by the upstream primary_hcp view/table
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- HCP display name enrichment
# MAGIC     --   Concatenate first + last name from provider dimension; COALESCE protects
# MAGIC     --   against nulls so string concat doesn't produce NULL.
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     COALESCE(p.FIRST_NAME, '') || ' ' || COALESCE(p.LAST_NAME, '') AS primary_hcp_name_2yr,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- HCO affiliation enrichment (via crosswalk)
# MAGIC     --   Map HCP NPI -> HCO NPI / HCO name using reference_file_pooja_1703.
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- ref.HCO_NPI  AS primary_hcp_hco_npi_2yr,
# MAGIC     ref.HCO_NAME AS primary_hcp_hco_name_2yr,
# MAGIC
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     -- Territory / region enrichment
# MAGIC     --   Pull both the human-readable names and numeric IDs (territory_id, region_id).
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     ref.territory_id AS primary_hcp_territory_id_2yr,
# MAGIC     ref.TERRITORY    AS primary_hcp_territory_2yr,
# MAGIC     ref.region_id    AS primary_hcp_region_id_2yr,
# MAGIC     ref.region       AS primary_hcp_region_2yr
# MAGIC
# MAGIC FROM primary_hcp ph
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Join #1: Provider dimension (name enrichment)
# MAGIC --   Join on the attributed primary HCP NPI to retrieve FIRST_NAME / LAST_NAME.
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC LEFT JOIN com_edp_prd.com_raw.kom_providers p
# MAGIC     ON ph.PRIMARY_HCP_NPI = p.NPI
# MAGIC
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC -- Join #2: Reference enrichment (HCO + territory + region)
# MAGIC --   Build a reference subquery that:
# MAGIC --     1) Starts from reference_file_pooja_1703 (HCP ↔ HCO + territory/region names)
# MAGIC --     2) Adds territory_id by mapping territory name -> territory_id via zip_to_territory_mapping
# MAGIC --     3) Adds region_id by mapping region name -> region_id via zip_to_territory_mapping
# MAGIC --     4) Renames hcp_primary_specialty to hcp_specialty (not used in final select here,
# MAGIC --        but retained in the ref dataset)
# MAGIC --   Finally join ref to primary_hcp using HCP_NPI.
# MAGIC -- -----------------------------------------------------------------------------
# MAGIC LEFT JOIN (
# MAGIC     SELECT
# MAGIC       * EXCEPT (hcp_primary_specialty),
# MAGIC       hcp_primary_specialty AS hcp_specialty
# MAGIC     FROM (
# MAGIC       SELECT
# MAGIC         a.*,
# MAGIC         b.territory_id,
# MAGIC         c.region_id
# MAGIC       FROM cmpa_insights_internal_schema.reference_file_pooja_1703 AS a
# MAGIC
# MAGIC       -- Map territory name -> territory_id (distinct pairs)
# MAGIC       LEFT JOIN (
# MAGIC         SELECT DISTINCT territory_id, territory_name
# MAGIC         FROM cmpa_insights_internal_schema.zip_to_territory_mapping
# MAGIC       ) AS b
# MAGIC         ON a.territory = b.territory_name
# MAGIC
# MAGIC       -- Map region name -> region_id (distinct pairs)
# MAGIC       LEFT JOIN (
# MAGIC         SELECT DISTINCT region_id, region_name
# MAGIC         FROM cmpa_insights_internal_schema.zip_to_territory_mapping
# MAGIC       ) AS c
# MAGIC         ON a.region = c.region_name
# MAGIC     )
# MAGIC ) ref
# MAGIC     ON ph.PRIMARY_HCP_NPI = ref.HCP_NPI;
# MAGIC

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
# MAGIC CREATE OR REPLACE TEMPORARY VIEW patient360_master AS
# MAGIC select PATIENT_ID as tivi_patient_id, PATIENT_YOB as tivi_patient_yob, PATIENT_AGE as tivi_patient_age, PATIENT_GENDER as tivi_patient_gender, patient_state as tivi_patient_state, incidence_date as tivi_incidence_date, first_incidence_treatment_date as tivi_first_incidence_treatment_date, latest_claim_date as tivi_latest_claim_date, latest_claim_hcp_npi as tivi_latest_claim_hcp_npi, latest_claim_hcp_name as tivi_latest_claim_hcp_name, latest_claim_hcp_specialty as tivi_latest_claim_hcp_specialty, latest_claim_hcp_visit_count as tivi_latest_claim_hcp_visit_count, latest_claim_hcp_hco_name as tivi_latest_claim_hcp_hco_name, latest_treatment_date as tivi_latest_treatment_date, latest_mpsii_tx_type as tivi_latest_mpsii_tx_type, first_tx_after_diagnosis as tivi_first_tx_after_diagnosis, time_dx_to_first_tx_in_months as tivi_time_dx_to_first_tx_in_months, treatment_period_months as tivi_treatment_period_months, Tivi_fills as tivi_fills, latest_treatment_hcp_npi as tivi_latest_treatment_hcp_npi, latest_treatment_hcp_name as tivi_latest_treatment_hcp_name, latest_treatment_hcp_specialty as tivi_latest_treatment_hcp_specialty, latest_treatment_hcp_visit_count as tivi_latest_treatment_hcp_visit_count, latest_treatment_hcp_hco_name as tivi_latest_treatment_hcp_hco_name, first_dx_hcp_5yr as tivi_first_dx_hcp_5yr, first_dx_all_visit_count_5yr as tivi_first_dx_all_visit_count_5yr, first_dx_last_visit_5yr as tivi_first_dx_last_visit_5yr, first_tx_hcp_5yr as tivi_first_tx_hcp_5yr, first_tx_all_visit_count_5yr as tivi_first_tx_all_visit_count_5yr, first_tx_treatment_visit_count_5yr as tivi_first_tx_treatment_visit_count_5yr, first_tx_last_visit_5yr as tivi_first_tx_last_visit_5yr, most_seen_hcp1_3yr_ranked as tivi_most_seen_hcp1_3yr_ranked, most_seen_hcp1_visit_count_5yr as tivi_most_seen_hcp1_visit_count_5yr, most_seen_hcp1_last_visit_5yr as tivi_most_seen_hcp1_last_visit_5yr, most_seen_hcp2_3yr_ranked as tivi_most_seen_hcp2_3yr_ranked, most_seen_hcp2_visit_count_5yr as tivi_most_seen_hcp2_visit_count_5yr, most_seen_hcp2_last_visit_5yr as tivi_most_seen_hcp2_last_visit_5yr, most_seen_hcp3_3yr_ranked as tivi_most_seen_hcp3_3yr_ranked, most_seen_hcp3_visit_count_5yr as tivi_most_seen_hcp3_visit_count_5yr, most_seen_hcp3_last_visit_5yr as tivi_most_seen_hcp3_last_visit_5yr, most_seen_hcp4_3yr_ranked as tivi_most_seen_hcp4_3yr_ranked, most_seen_hcp4_visit_count_5yr as tivi_most_seen_hcp4_visit_count_5yr, most_seen_hcp4_last_visit_5yr as tivi_most_seen_hcp4_last_visit_5yr, most_seen_hcp5_3yr_ranked as tivi_most_seen_hcp5_3yr_ranked, most_seen_hcp5_visit_count_5yr as tivi_most_seen_hcp5_visit_count_5yr, most_seen_hcp5_last_visit_5yr as tivi_most_seen_hcp5_last_visit_5yr, most_recently_treated_hcp_2yr as tivi_most_recently_treated_hcp_2yr, most_recently_treated_hcp_name_2yr as tivi_most_recently_treated_hcp_name_2yr, most_recently_treated_hcp_2yr_no_of_visits_5yr as tivi_most_recently_treated_hcp_2yr_no_of_visits_5yr, most_recent_tx_hcp_2yr_last_visit_5yr as tivi_most_recent_tx_hcp_2yr_last_visit_5yr, most_recently_treated_hcp_specialty_2yr as tivi_most_recently_treated_hcp_specialty_2yr, most_recently_treated_hcp_hco_name as tivi_most_recently_treated_hcp_hco_name, most_recently_treated_hcp_territory_id_2yr as tivi_most_recently_treated_hcp_territory_id_2yr, most_recently_treated_hcp_territory_2yr as tivi_most_recently_treated_hcp_territory_2yr, most_recently_treated_hcp_region_id_2yr as tivi_most_recently_treated_hcp_region_id_2yr, most_recently_treated_hcp_region_2yr as tivi_most_recently_treated_hcp_region_2yr, PRIMARY_HCP_NPI as tivi_primary_hcp_npi, PRIMARY_HCP_SPECIALTY as tivi_primary_hcp_specialty, SPECIALTY_PRIORITY as tivi_specialty_priority, NO_OF_VISITS as tivi_no_of_visits, DX_VISITS as tivi_dx_visits, TX_VISITS as tivi_tx_visits, MOST_RECENT_VISIT as tivi_most_recent_visit, HCP_RANK as tivi_hcp_rank
# MAGIC
# MAGIC
# MAGIC from (SELECT DISTINCT
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
# MAGIC     -- a.latest_claim_hcp_hco_npi,
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
# MAGIC     --   - Tivi_fills: distinct tx dates in refresh window as defined upstream
# MAGIC     -- -------------------------------------------------------------------------
# MAGIC     a.Tivi_fills,
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
# MAGIC     -- a.latest_treatment_hcp_hco_npi,
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
# MAGIC     c.* EXCEPT (patient_id)
# MAGIC
# MAGIC FROM patient360_base AS a
# MAGIC
# MAGIC -- Left join retains all patients from patient360_base even if no match in most_recently_treated_hcp
# MAGIC LEFT JOIN most_recently_treated_hcp AS b
# MAGIC     ON a.PATIENT_ID = b.patient_id
# MAGIC
# MAGIC -- Left join retains all patients from patient360_base even if no match in primary_hcp
# MAGIC LEFT JOIN primary_hcp AS c
# MAGIC     ON a.PATIENT_ID = c.patient_id)
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from patient360_master;
