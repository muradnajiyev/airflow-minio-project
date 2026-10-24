
from datetime import datetime

from airflow.sdk import dag, task, Asset, Metadata, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule


sales_asset = Asset("s3://airflowbucket/silver/sales")


@dag(
    dag_id="producer_dag",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
)
def producer():

    @task
    def load_source():
        s3 = S3Hook(aws_conn_id="minio_conn")

        files = s3.list_keys(
            bucket_name="airflowbucket",
            prefix="bronze/"
        ) or []

        csv_files = [
            f for f in files if f.lower().endswith(".csv")
        ]

        json_files = [
            f for f in files if f.lower().endswith(".json")
        ]

        print("CSV :", csv_files)
        print("JSON :", json_files)

        return {
            "csv": csv_files,
            "json": json_files
        }

    @task
    def get_csv_files(files):
        return files["csv"]

    @task
    def get_json_files(files):
        return files["json"]

    @task
    def process_csv(file):
        print("CSV emal olunur:", file)

    @task
    def process_json(file):
        print("JSON emal olunur:", file)

    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    cleanup = EmptyOperator(
        task_id="cleanup",
        trigger_rule=TriggerRule.ALL_DONE
    )

    @task(outlets=[sales_asset])
    def publish_sales():
        context = get_current_context()

        yield Metadata(
            sales_asset,
            {
                "producer_date": context["logical_date"].isoformat()
            }
        )

        print("Sales asset")

    
    files = load_source()

    
    csv_files = get_csv_files(files)
    json_files = get_json_files(files)

    
    csv_result = process_csv.expand(file=csv_files)
    json_result = process_json.expand(file=json_files)

    
    csv_result >> join
    json_result >> join

    
    join >> cleanup

    
    cleanup >> publish_sales()


producer()
