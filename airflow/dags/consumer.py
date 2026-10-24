
from datetime import datetime

import pendulum

from airflow.sdk import dag, task, Asset
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor


sales_asset = Asset("s3://airflowbucket/silver/sales")


def get_producer_date(
    logical_date,
    *,
    triggering_asset_events=None,
    **kwargs
):
    events = (triggering_asset_events or {}).get(sales_asset, [])

    if not events:
        return logical_date

    producer_date = events[-1].extra.get("producer_date")

    if not producer_date:
        return logical_date

    return pendulum.parse(producer_date)


@dag(
    dag_id="consumer_dag",
    start_date=datetime(2026, 1, 1),
    schedule=[sales_asset],
    catchup=False,
)
def consumer():

    wait_for_load_source = ExternalTaskSensor(
        task_id="wait_for_load_source",
        external_dag_id="producer_dag",
        external_task_id="load_source",
        execution_date_fn=get_producer_date,
        allowed_states=["success"],
        failed_states=["failed"],
        mode="reschedule",
        poke_interval=10,
        timeout=600,
        check_existence=True,
    )

    @task
    def consume_sales():
        print("Sales oxunur")
        print("Consumer tamamlandi")

    wait_for_load_source >> consume_sales()


consumer()
