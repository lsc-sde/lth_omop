import os
import pandas as pd
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

from dotenv import load_dotenv
from sqlmesh import model
from sqlmesh.core.model.kind import ModelKindName

SNOMED_URL = "https://accessgudid.nlm.nih.gov/api/v3/devices/snomed.json"


@model(
    name="stg.stg__device_cache",
    kind=dict(
        name=ModelKindName.INCREMENTAL_BY_UNIQUE_KEY,
        unique_key=["device_id"],
    ),
    columns={
        "device_id": "VARCHAR(64)",
        "snomed_identifier": "BIGINT",
        "snomed_ct_name": "VARCHAR(4000)",
        "retrieved_at": "DATETIME2",
        "error": "VARCHAR(4000)",
    },
    description="Incremental cache of SNOMED CT lookups from AccessGUDID keyed by device_id.",
)
def execute(context, **kwargs):
    # ---- env (your preferred pattern)
    load_dotenv()
    api_key = os.getenv("UMLS_API_KEY")
    if not api_key:
        raise RuntimeError("UMLS_API_KEY not found in environment")

    # ---- source ids
    src = context.engine_adapter.fetchdf("""
        SELECT DISTINCT CAST(device_id AS VARCHAR(64)) AS device_id
        FROM src.src_bi__devices
        WHERE device_id IS NOT NULL
    """)
    if not src.empty:
        src["device_id"] = src["device_id"].astype(str).str.strip()
        src = src[src["device_id"] != ""]
        all_ids = src["device_id"].unique().tolist()
    else:
        all_ids = []

    # ---- cached ids
    cached_ids = set()
    if all_ids:
        try:
            cached = context.engine_adapter.fetchdf("""
                SELECT CAST(device_id AS VARCHAR(64)) AS device_id
                FROM stg.stg__device_cache
            """)
            cached_ids = set(cached["device_id"].astype(str).str.strip().tolist())
        except Exception:
            cached_ids = set()

    # ---- new ids only
    new_ids = [d for d in all_ids if d not in cached_ids] if all_ids else []

    # ---- fetch rows (or none)
    rows = []
    retrieved_at = pd.Timestamp.utcnow()

    if new_ids:
        def fetch(di: str) -> dict:
            try:
                r = requests.get(
                    SNOMED_URL,
                    params={"di": di, "apiKey": api_key},
                    timeout=30,
                )
                r.raise_for_status()
                concepts = r.json().get("concepts") or []

                if not concepts:
                    return {
                        "device_id": di,
                        "snomed_identifier": None,
                        "snomed_ct_name": None,
                        "retrieved_at": retrieved_at,
                        "error": "No concepts returned",
                    }

                c0 = concepts[0]
                return {
                    "device_id": di,
                    "snomed_identifier": c0.get("snomedIdentifier"),
                    "snomed_ct_name": c0.get("snomedCTName"),
                    "retrieved_at": retrieved_at,
                    "error": None,
                }

            except Exception as e:
                return {
                    "device_id": di,
                    "snomed_identifier": None,
                    "snomed_ct_name": None,
                    "retrieved_at": retrieved_at,
                    "error": str(e),
                }

        with ThreadPoolExecutor(max_workers=10) as ex:
            futures = [ex.submit(fetch, di) for di in new_ids]
            for f in as_completed(futures):
                rows.append(f.result())

    # ---- build output df ONCE
    out = pd.DataFrame.from_records(
        rows,
        columns=[
            "device_id",
            "snomed_identifier",
            "snomed_ct_name",
            "retrieved_at",
            "error",
        ],
    )

    # ---- DOC-COMPLIANT EMPTY HANDLING (the important bit)
    if out.empty:
        yield from ()
    else:
        yield out