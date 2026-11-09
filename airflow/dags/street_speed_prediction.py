from datetime import timedelta

import pendulum
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sdk import dag

@dag(
    dag_id="street_speed_prediction",
    schedule="*/5 * * * *",
    start_date=pendulum.datetime(2026, 1, 1, tz="Asia/Tehran"),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    tags=["traffic", "prediction"],
)
def street_speed_prediction():
    SparkSubmitOperator(
        task_id="update_predicted_speeds",
        application="/opt/airflow/sedona/street_speed_prediction.py",
        name="street-speed-prediction",
        conn_id="spark_sedona",
        deploy_mode="client",
        jars=(
            "/opt/spark-jars/iceberg-spark-runtime-4.0_2.13-1.10.2.jar,"
            "/opt/spark-jars/iceberg-aws-bundle-1.10.2.jar"
        ),
        driver_memory="512m",
        executor_memory="512m",
        executor_cores=1,
        total_executor_cores=1,
        conf={
            "spark.driver.host": "airflow",
            "spark.driver.bindAddress": "0.0.0.0",
        },
    )


street_speed_prediction()