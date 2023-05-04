import os

from prefect import flow
from prefect_dbt.cli import DbtCliProfile, DbtCoreOperation


@flow(log_prints=True)
def trigger_dbt_flow():

    dbt_cli_profile_block = os.getenv("DBT_CLI_PROFILE_BLOCK")
    dbt_cli_profile = DbtCliProfile.load(dbt_cli_profile_block)

    with DbtCoreOperation(
        commands=[
            "dbt deps",
            "dbt build dbt_metrics_default_calendar",
            "dbt build --var 'is_test_run: false'",
        ],
        project_dir="~/de-zoomcamp-project/workflow/dbt/nyc_bus/",
        dbt_cli_profile=dbt_cli_profile,
        overwrite_profiles=True,
    ) as dbt_operation:
        dbt_process = dbt_operation.trigger()
        dbt_process.wait_for_completion()
        result = dbt_process.fetch_result()

    return result


if __name__ == "__main__":
    trigger_dbt_flow()
