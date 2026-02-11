import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
import pandas as pd
import requests
from dotenv import find_dotenv, load_dotenv
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

# -----------------------
# Config
# -----------------------
load_dotenv(find_dotenv())
API_KEY = os.getenv("UMLS_API_KEY")
SNOMED_URL = "https://accessgudid.nlm.nih.gov/api/v3/devices/snomed.json"


def _empty_df() -> pd.DataFrame:
    return pd.DataFrame(
        columns=[
            "device_id",
            "snomed_identifier",
            "snomed_ct_name",
            "retrieved_at",
            "error",
        ]
    )


@lru_cache()
def fetch(di: str):
    try:
        r = requests.get(
            SNOMED_URL,
            params={"di": di, "apiKey": API_KEY},
            timeout=15,
        )
        r.raise_for_status()

        concepts = r.json().get("concepts", [])

        if not concepts:
            return {
                "device_id": di,
                "snomed_identifier": None,
                "snomed_ct_name": None,
                "error": "No concepts returned",
            }

        c0 = concepts[0]
        return {
            "device_id": di,
            "snomed_identifier": c0.get("snomedIdentifier"),
            "snomed_ct_name": c0.get("snomedCTName"),
            "error": None,
        }

    except Exception as e:
        return {
            "device_id": di,
            "snomed_identifier": None,
            "snomed_ct_name": None,
            "error": str(e),
        }


@model(
    name="lth_bronze.stg__device_cache",
    # kind=IncrementalByUniqueKeyKind(unique_key=["device_id"]),
    kind={
        "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        "unique_key": ["device_id"],
    },
    columns={
        "device_id": "VARCHAR(64)",
        "snomed_identifier": "BIGINT",
        "snomed_ct_name": "VARCHAR(4000)",
        "retrieved_at": "DATETIME2",
        "error": "VARCHAR(4000)",
    },
    description="Incremental cache of SNOMED CT lookups from AccessGUDID keyed by device_id.",
    depends_on=["lth_bronze.src_bi__devices"],
)
def execute(context: ExecutionContext, **kwargs) -> pd.DataFrame:
    # 1) Pull all distinct device_ids from source

    src = context.fetchdf("""
        SELECT DISTINCT CAST(device_id AS VARCHAR(64)) AS device_id
        FROM lth_bronze.src_bi__devices
        WHERE device_id IS NOT NULL
    """)
    if src.empty:
        # return _empty_df()
        yield from ()
        return

    src["device_id"] = src["device_id"].astype(str).str.strip()
    src = src[src["device_id"] != ""]
    all_ids = src["device_id"].unique().tolist()

    if not all_ids:
        return _empty_df()

    # 2) Pull already-cached device_ids (table may not exist on first run)
    # This will result in a circular dependency!
    try:
        cached = context.engine_adapter.fetchdf("""
            SELECT CAST(device_id AS VARCHAR(64)) AS device_id
            FROM lth_bronze.stg__device_cache
        """)
        cached_ids = set(cached["device_id"].astype(str).str.strip().tolist())
    except Exception:
        cached_ids = set()

    # 3) Only fetch new IDs
    new_ids = [d for d in all_ids if d not in cached_ids]
    if not new_ids:
        return _empty_df()

    # 4) Fetch SNOMED via API

    rows = []
    with ThreadPoolExecutor(max_workers=10) as ex:
        futures = [ex.submit(fetch, di) for di in new_ids]
        for f in as_completed(futures):
            rows.append(f.result())

    df = pd.DataFrame(rows)
    df["retrieved_at"] = pd.Timestamp.utcnow()

    return df[
        [
            "device_id",
            "snomed_identifier",
            "snomed_ct_name",
            "retrieved_at",
            "error",
        ]
    ]


# import os
# import pandas as pd
# import requests
# from concurrent.futures import ThreadPoolExecutor, as_completed

# from dotenv import load_dotenv
# from sqlmesh import model
# from sqlmesh.core.model.kind import ModelKindName

# SNOMED_URL = "https://accessgudid.nlm.nih.gov/api/v3/devices/snomed.json"


# @model(
#     name="lth_bronze.stg__device_cache",
#     kind={
#         "name": ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
#         "unique_key": ["device_id"],
#     },
#     columns={
#         "device_id": "VARCHAR(64)",
#         "snomed_identifier": "BIGINT",
#         "snomed_ct_name": "VARCHAR(4000)",
#         "retrieved_at": "DATETIME2",
#         "error": "VARCHAR(4000)",
#     },
#     description="Incremental cache of SNOMED CT lookups from AccessGUDID keyed by device_id.",
# )
# def execute(context, **kwargs):
#     # --- Your preferred env loading style
#     load_dotenv()
#     api_key = os.getenv("UMLS_API_KEY")
#     if not api_key:
#         raise RuntimeError("UMLS_API_KEY not found in environment")

#     # 1) Pull device ids
#     src = context.engine_adapter.fetchdf("""
#         SELECT DISTINCT CAST(device_id AS VARCHAR(64)) AS device_id
#         FROM lth_bronze.src_bi__devices
#         WHERE device_id IS NOT NULL
#     """)

#     if src.empty:
#         yield from ()
#         return

#     src["device_id"] = src["device_id"].astype(str).str.strip()
#     src = src[src["device_id"] != ""]
#     all_ids = src["device_id"].unique().tolist()

#     if not all_ids:
#         yield from ()
#         return

#     # 2) Cached ids (table may not exist yet)
#     try:
#         cached = context.engine_adapter.fetchdf("""
#             SELECT CAST(device_id AS VARCHAR(64)) AS device_id
#             FROM lth_bronze.stg__device_cache
#         """)
#         cached_ids = set(cached["device_id"].astype(str).str.strip().tolist())
#     except Exception:
#         cached_ids = set()

#     # 3) New ids only
#     new_ids = [d for d in all_ids if d not in cached_ids]
#     if not new_ids:
#         yield from ()
#         return

#     # 4) Fetch SNOMED
#     def fetch(di: str):
#         try:
#             r = requests.get(
#                 SNOMED_URL,
#                 params={"di": di, "apiKey": api_key},
#                 timeout=30,
#             )
#             r.raise_for_status()
#             concepts = r.json().get("concepts") or []
#             if not concepts:
#                 return {
#                     "device_id": di,
#                     "snomed_identifier": None,
#                     "snomed_ct_name": None,
#                     "error": "No concepts returned",
#                 }
#             c0 = concepts[0]
#             return {
#                 "device_id": di,
#                 "snomed_identifier": c0.get("snomedIdentifier"),
#                 "snomed_ct_name": c0.get("snomedCTName"),
#                 "error": None,
#             }
#         except Exception as e:
#             return {
#                 "device_id": di,
#                 "snomed_identifier": None,
#                 "snomed_ct_name": None,
#                 "error": str(e),
#             }

#     rows = []
#     with ThreadPoolExecutor(max_workers=10) as ex:
#         futures = [ex.submit(fetch, di) for di in new_ids]
#         for f in as_completed(futures):
#             rows.append(f.result())

#     df = pd.DataFrame(rows)

#     # ✅ CRITICAL: never yield an empty DataFrame
#     if df.empty:
#         yield from ()
#         return

#     df["retrieved_at"] = pd.Timestamp.utcnow()

#     out = df[["device_id", "snomed_identifier", "snomed_ct_name", "retrieved_at", "error"]]

#     # ✅ CRITICAL: guard again after column selection
#     if out.empty:
#         yield from ()
#         return

#     # ✅ CRITICAL: because we used yield anywhere, we must yield the DF (not return it)
#     yield out
