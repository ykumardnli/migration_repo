# Databricks notebook source
#Reltio match logic needs to come here, and also VOD match logic

#Based on match DCR type should be made CREATE or UPDATE type


# COMMAND ----------

#issue : No reltio table found for batch data, only streaming data is available which is not running

# COMMAND ----------

# MAGIC %md
# MAGIC Created a general logic for matching records with reltio, once recieved all the access will update the code accordingly
# MAGIC

# COMMAND ----------

# --------------------------------------------------------
# LOGIC:
# 1. Read DCR input
# 2. Fetch candidate records from external system (Reltio placeholder)
# 3. Score candidates using weighted logic (exact + fuzzy)
# 4. Rank candidates
# 5. Log all matches
# 6. Decide action based on thresholds (UPDATE / REVIEW / CREATE)
# 7. Update control table
# --------------------------------------------------------

from pyspark.sql import SparkSession
import json

spark = SparkSession.builder.getOrCreate()

# --------------------------------------------------------
# CONFIG: WEIGHTS
# --------------------------------------------------------

HCP_WEIGHTS = {
    "npi__v": {"score": 100, "match": "exact"},
    "me__v": {"score": 80, "match": "exact"},
    "license__v": {"score": 75, "match": "exact"},
    "last_name_cda__v": {"score": 25, "match": "exact"},
    "first_name_cda__v": {"score": 15, "match": "exact"},
    "first_name_cda__v_fuzzy": {"score": 8, "match": "fuzzy", "base": "first_name_cda__v"},
    "postal_code_cda__v": {"score": 20, "match": "exact"}
}

HCO_WEIGHTS = {
    "name__v": {"score": 35, "match": "exact"},
    "name__v_fuzzy": {"score": 18, "match": "fuzzy", "base": "name__v"},
    "postal_code_cda__v": {"score": 28, "match": "exact"},
    "city_cda__v": {"score": 12, "match": "exact"}
}

AUTO_CONFIRM_THRESHOLD = 80
STEWARD_REVIEW_THRESHOLD = 40


# --------------------------------------------------------
# MAIN MATCH FUNCTION
# --------------------------------------------------------

def entity_matcher(dcr_id: str):

    # Step 1: Get DCR record
    dcr = get_dcr(dcr_id)

    # Step 2: Mark IN_PROGRESS
    update_status(dcr_id, "IN_PROGRESS")

    # Step 3: Pick weights
    weights = HCP_WEIGHTS if dcr["entity_type"] == "HCP" else HCO_WEIGHTS

    # Step 4: Fetch candidates (REPLACE THIS LATER WITH RELTIO)
    candidates = fetch_candidates(dcr)

    if not candidates:
        finalize_match(dcr_id, None, 0, "CREATE")
        return

    # Step 5: Score candidates
    scored = []
    for candidate in candidates:
        score, breakdown = compute_match_score(dcr, candidate, weights)

        scored.append({
            "uri": candidate.get("uri"),
            "score": score,
            "breakdown": breakdown,
            "name": candidate.get("name__v"),
            "zip": candidate.get("postal_code_cda__v")
        })

    # Step 6: Rank
    ranked = sorted(scored, key=lambda x: x["score"], reverse=True)

    # Step 7: Log matches
    for rank, match in enumerate(ranked, start=1):
        spark.sql(f"""
            INSERT INTO dcr_match_log VALUES (
                '{dcr_id}', {rank}, '{match["uri"]}',
                {match["score"]}, '{json.dumps(match["breakdown"])}',
                '{match["name"]}', '{match["zip"]}',
                NULL, current_timestamp()
            )
        """)

    # Step 8: Decision
    best = ranked[0]

    if best["score"] >= AUTO_CONFIRM_THRESHOLD:
        finalize_match(dcr_id, best["uri"], best["score"], "UPDATE")

    elif best["score"] >= STEWARD_REVIEW_THRESHOLD:
        finalize_match(dcr_id, best["uri"], best["score"], "NEEDS_MATCH_REVIEW")

        # Optional: trigger workflow later
        trigger_steward_review(dcr_id, ranked[:3])

    else:
        finalize_match(dcr_id, None, best["score"], "CREATE")


# --------------------------------------------------------
# FETCH CANDIDATES (PLACEHOLDER)
# Replace this with Reltio API or table
# --------------------------------------------------------

def fetch_candidates(dcr: dict):
    """
    TEMP LOGIC:
    Replace with:
    - Reltio API call
    - OR Reltio staging table
    """

    df = spark.table("rlt_entity_stage")  # <-- replace later

    # Dynamic filtering based on available fields
    if dcr.get("npi__v"):
        df = df.filter(f"npi__v = '{dcr['npi__v']}'")

    return [row.asDict() for row in df.limit(50).collect()]


# --------------------------------------------------------
# MATCH SCORING
# --------------------------------------------------------

def compute_match_score(dcr: dict, candidate: dict, weights: dict):

    from rapidfuzz import fuzz

    total = 0
    breakdown = {}

    for field, config in weights.items():

        is_fuzzy = "fuzzy" in config["match"]
        base_field = config.get("base", field)

        dcr_val = dcr.get(base_field)
        cand_val = candidate.get(base_field)

        if not dcr_val or not cand_val:
            breakdown[field] = {"score": 0, "reason": "missing"}
            continue

        dcr_val = str(dcr_val).strip().lower()
        cand_val = str(cand_val).strip().lower()

        if is_fuzzy:
            similarity = fuzz.token_sort_ratio(dcr_val, cand_val)

            if similarity >= 80:
                pts = int(config["score"] * (similarity / 100))
                total += pts
                breakdown[field] = {"score": pts, "similarity": similarity}
            else:
                breakdown[field] = {"score": 0, "similarity": similarity}

        else:
            if dcr_val == cand_val:
                total += config["score"]
                breakdown[field] = {"score": config["score"]}
            else:
                breakdown[field] = {"score": 0, "reason": "mismatch"}

    return total, breakdown


# --------------------------------------------------------
# FINALIZE MATCH
# --------------------------------------------------------

def finalize_match(dcr_id, matched_uri, score, decision):

    dcr_type = decision if decision in ("CREATE", "UPDATE") else "PENDING_REVIEW"

    spark.sql(f"""
        UPDATE dcr_control_tower SET
            dcr_type = '{dcr_type}',
            reltio_uri = {'NULL' if not matched_uri else f"'{matched_uri}'"},
            match_score = {score},
            last_updated_at = current_timestamp()
        WHERE dcr_id = '{dcr_id}'
    """)


# --------------------------------------------------------
# HELPERS (PLUG YOUR OWN)
# --------------------------------------------------------

def get_dcr(dcr_id: str):
    df = spark.table("dcr_control_tower").filter(f"dcr_id = '{dcr_id}'")
    return df.first().asDict()


def update_status(dcr_id: str, status: str):
    spark.sql(f"""
        UPDATE dcr_control_tower
        SET status = '{status}'
        WHERE dcr_id = '{dcr_id}'
    """)


def trigger_steward_review(dcr_id, top_matches):
    print(f"Send to steward: {dcr_id}")

