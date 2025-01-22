import typing as t
from datetime import datetime
from string import ascii_lowercase, digits

import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName

alphanum = ascii_lowercase + digits
charmap = {a: str(n) for a, n in zip(alphanum, range(10, 10 + 36))}


def postcode_to_unique(postcode: str) -> int:
    """
    Converts a given postcode to a unique integer representation.
    This function takes a postcode string, normalizes it by converting it to lowercase
    and removing any spaces, and then maps each character to a corresponding integer
    using a predefined character map. The resulting integers are concatenated to form
    a unique integer representation of the postcode.
    Args:
        postcode (str): The postcode to be converted.
    Returns:
        int: The unique integer representation of the postcode. Returns 0 if the postcode
        contains invalid characters.
    """

    try:
        postcode = postcode.lower().replace(" ", "")
        assert all(p in alphanum for p in postcode)

        out = "".join(str(charmap[p]) for p in postcode)
    except AssertionError as e:
        print(e)
        out = 0

    finally:
        return int(out)


columns = {
    "location_id": "bigint",
    "address_1": "text",
    "address_2": "text",
    "city": "text",
    "state": "text",
    "zip": "text",
    "county": "text",
    "location_source_value": "text",
    "country_concept_id": "int",
    "country_source_value": "text",
    "latitude": "float",
    "longitude": "float",
}


@model(
    "lth_bronze.location",
    depends_on=["lth_bronze.ext__postcodes"],
    kind=ModelKindName.FULL,
    columns=columns,
    cron="@daily",
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    table = context.resolve_table("lth_bronze.ext__postcodes")

    df = context.fetchdf(query=f"select postcode, location_source_value from {table}")

    df["location_id"] = df["postcode"].apply(postcode_to_unique)
    df["address_1"] = None
    df["address_2"] = None
    df["city"] = None
    df["state"] = None
    df["zip"] = df["postcode"]
    df["county"] = None
    df["country_concept_id"] = 42035286
    df["country_source_value"] = "United Kingdom of Great Britain and Northern Ireland"
    df["latitude"] = None
    df["longitude"] = None

    # This is important to ensure the column order is maintained.
    df = df[columns.keys()]

    return df
