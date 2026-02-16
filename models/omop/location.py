import typing as t
from datetime import datetime
from string import ascii_lowercase, digits

import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from sqlglot.expressions import to_column

alphanum = ascii_lowercase + digits
charmap = {a: str(n) for a, n in zip(alphanum, range(10, 10 + 36))}


def normalise_postcode(postcode: str) -> str:
    """
    Normalises a given postcode by converting it to lowercase and removing any spaces.
    If the postcode contains any non-alphanumeric characters, it is considered invalid
    and the function returns "badcode".
    Args:
        postcode (str): The postcode to be normalised.
    Returns:
        str: The normalised postcode or "badcode" if the postcode is invalid.
    """

    try:
        postcode = postcode.lower().replace(" ", "")
        assert all(p in alphanum for p in postcode)

    except AssertionError as e:
        print(e)
        postcode = "badcode"

    finally:
        return postcode


def postcode_to_unique(postcode: str) -> int:
    """
    Converts a postcode string to a unique integer representation.
    Args:
        postcode (str): The postcode to be converted.
    Returns:
        int: The unique integer representation of the postcode. If an error occurs, returns 0.
    Raises:
        AssertionError: If the postcode contains characters not in the charmap.
    """

    try:
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
    "cdm.location",
    depends_on=["cdm.ext__postcodes"],
    kind=ModelKindName.FULL,
    columns=columns,
    cron="@yearly",
    audits=[
        ("not_null", {"columns": [to_column("location_id"), to_column("zip")]}),
        ("unique_values", {"columns": [to_column("location_id"), to_column("zip")]}),
    ],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    table = context.resolve_table("cdm.ext__postcodes")

    df = context.fetchdf(query=f"select postcode, location_source_value from {table}")

    df["zip"] = df["postcode"].apply(normalise_postcode)

    # Remove duplicate postcodes
    df = df.drop_duplicates(subset=["zip"])
    df["location_id"] = df["zip"].apply(postcode_to_unique)
    df["address_1"] = None
    df["address_2"] = None
    df["city"] = None
    df["state"] = None
    df["county"] = None
    df["country_concept_id"] = 42035286
    df["country_source_value"] = "United Kingdom of Great Britain and Northern Ireland"
    df["latitude"] = None
    df["longitude"] = None

    # This is important to ensure the column order is maintained.
    df = df[columns.keys()]

    return df
