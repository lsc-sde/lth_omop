import pyodbc
import pandas as pd
from sqlmesh import ExecutionContext, model
from sqlmesh.core.model.kind import ModelKindName
from dotenv import load_dotenv
import os

load_dotenv()

@model(
    name="lth_bronze.cdc_updated_at",
    kind=ModelKindName.FULL,
    depends_on=["lth_bronze.cdc__updated_at_final"],
    cron="@hourly",
    columns={
        "id": "int",
        "name": "string",
        "updated_at": "datetime"
    }
)

def cdc_updated_at(context, snapshot, runtime_stage, start, end, latest, execution_time):

    conn_str = (
        f"DRIVER={{{os.getenv('DB_DRIVER')}}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('MSSQL_DATABASE')};"
        f"Trusted_Connection={os.getenv('DB_TRUSTED_CONNECTION')};"
    )

    connection = pyodbc.connect(conn_str)
    cursor = connection.cursor()

    cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM lth_bronze.cdc__updated_at_final)
            BEGIN
                DROP TABLE IF EXISTS lth_bronze.cdc__updated_at_clone;
                SELECT * INTO lth_bronze.cdc__updated_at_clone FROM lth_bronze.cdc__updated_at_default
            END
            ELSE
            BEGIN
                DROP TABLE IF EXISTS lth_bronze.cdc__updated_at_clone;
                SELECT * INTO lth_bronze.cdc__updated_at_clone FROM (
                SELECT * 
                FROM lth_bronze.cdc__updated_at_final

                UNION ALL

                SELECT * 
                FROM lth_bronze.cdc__updated_at_default
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM lth_bronze.cdc__updated_at_final t
                    WHERE t.model = cdc__updated_at_default.model
                    AND t.datasource = cdc__updated_at_default.datasource
                )
                ) a
            END
    """)

    connection.commit()

    data = [
        {"id": 1, "name": "test", "updated_at": pd.Timestamp.now()},
    ]
    processed_df = pd.DataFrame(data)

    cursor.close()
    connection.close()

    return processed_df
