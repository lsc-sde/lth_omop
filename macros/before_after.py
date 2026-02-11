from sqlmesh import macro
from sqlmesh.core.macros import MacroEvaluator


@macro()
def before_all(evaluator: MacroEvaluator) -> list[str]:
    schema = (
        "lth_bronze"
        if evaluator.this_env == "prod"
        else f"lth_bronze__{evaluator.this_env}"
    )

    return [
        f"""BEGIN TRY
                EXEC ('CREATE SCHEMA {schema}')
            END TRY
            BEGIN CATCH
            -- do nothing
            END CATCH;""",
        f"""
        EXEC('
            IF NOT EXISTS (SELECT 1 FROM {schema}.cdc__updated_at_final)
            BEGIN
                DROP TABLE IF EXISTS {schema}.cdc__updated_at_clone;
                SELECT * INTO {schema}.cdc__updated_at_clone FROM {schema}.cdc__updated_at_default
            END
            ELSE
            BEGIN
                DROP TABLE IF EXISTS {schema}.cdc__updated_at_clone;
                SELECT * INTO {schema}.cdc__updated_at_clone FROM (
                SELECT *
                FROM {schema}.cdc__updated_at_final

                UNION ALL

                SELECT *
                FROM {schema}.cdc__updated_at_default
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM {schema}.cdc__updated_at_final t
                    WHERE t.model = cdc__updated_at_default.model
                    AND t.datasource = cdc__updated_at_default.datasource
                )
                ) a
            END
            ');
        """,
    ]


@macro()
def after_all(evaluator: MacroEvaluator):
    schema = (
        "lth_bronze"
        if evaluator.this_env == "prod"
        else f"lth_bronze__{evaluator.this_env}"
    )

    return [
        f"TRUNCATE TABLE {schema}.cdc__updated_at_clone;",
        f"""INSERT into {schema}.cdc__updated_at_clone
        SELECT * FROM {schema}.cdc__updated_at_final;
        """,
    ]
