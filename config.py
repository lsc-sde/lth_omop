from dotenv import find_dotenv, load_dotenv

import os
from sqlmesh.core.config import Config

from sqlmesh.core.config import (
    ModelDefaultsConfig,
    GatewayConfig,
    MSSQLConnectionConfig,
)
from sqlmesh.core.config.format import FormatConfig
from sqlmesh.core.config.ui import UIConfig
from sqlmesh.core.model import ModelKindName
from pydantic import BaseModel, ConfigDict
from typing import Dict, Any

load_dotenv(find_dotenv(), override=True)

###############################################################################
# SQLMESH CONFIGURATION
###############################################################################

state_schema: str = os.getenv("STATE_SCHEMA", "lth_omop_bronze")
default_gateway: str = "mssql"

gateways = {}

# Setup gateways

# MSSQL

gateway_mssql = GatewayConfig(
    connection=MSSQLConnectionConfig(
        host=os.environ["MSSQL_HOST"],
        database=os.environ["MSSQL_DATABASE"],
        concurrent_tasks=os.getenv("MSSQL_CONCURRENT_TASKS", default=4),
    ),
    state_connection=MSSQLConnectionConfig(
        host=os.environ["MSSQL_STATE_HOST"],
        database=os.environ["MSSQL_STATE_DATABASE"],
    ),
    state_schema=state_schema,
)

gateways["mssql"] = gateway_mssql


class SQLMeshSettings(BaseModel):
    """
    SQLMeshSettings class represents the settings for SQLMesh.

    Attributes:
        project (str): The name of the project.
        model_defaults (ModelDefaultsConfig): The configuration for model defaults.
        gateways (dict): A dictionary of gateways, where the keys are the gateway names and the values are the corresponding gateway functions.
        default_gateway (str): The name of the default gateway.
        variables (dict): A dictionary of variables.
        format (FormatConfig): The configuration for formatting.
    """

    model_config = ConfigDict(protected_namespaces=())

    project: str
    model_defaults: ModelDefaultsConfig = ModelDefaultsConfig(
        kind=ModelKindName.VIEW,
        dialect="tsql,normalization_strategy=case_sensitive",
        cron="@daily",
        owner="LTH DST",
        start="2026-01-01 00:00:00.000",
    )
    gateways: Dict[str, GatewayConfig] = gateways
    default_gateway: str = default_gateway
    variables: Dict[str, Any]
    format: FormatConfig = FormatConfig(
        append_newline=True,
        normalize=True,
        indent=2,
        normalize_functions="lower",
        leading_comma=False,
        max_text_width=80,
        no_rewrite_casts=False,
    )
    ui: UIConfig = UIConfig(format_on_save=False)


variables = {
    "global_start_date": "2005-01-01",
    "minimum_observation_period_start_date": "2005-01-01",
    "catalog_src": os.environ["MSSQL_DATABASE_SOURCE"],
    "schema_src": os.environ["MSSQL_SCHEMA_SOURCE"],
    "schema_vocab": os.environ["MSSQL_SCHEMA_VOCAB"],
    "scr_db": os.environ["MSSQL_SCR_DB"],
    "bi_db": os.environ["MSSQL_BI_DB"],
}

# Statements to execute before and after running SQLMesh
before_all = [
    "@before_all()",
]

after_all = [
    "@after_all()",
]

config = Config(
    **dict(SQLMeshSettings(project="lth_omop_bronze", variables=variables)),
    # before_all=before_all,
    # after_all=after_all,
)
