from airflow.decorators import dag, task
from datetime import datetime
from airflow.providers.docker.operators.docker import DockerOperator

# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="0 8 1 * *",
    catchup=False,
    default_args={"owner": "kelvin"}
)

def spark_etl():
    
    Ingestion = DockerOperator(
        task_id='Ingestion',
        max_active_tis_per_dag=1,
        image='airflow/spark-app',
        container_name='spark_ingestion',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        command='python3 /app/Ingestion.py'
    )

    Cleansing = DockerOperator(
        task_id='Cleansing',
        max_active_tis_per_dag=1,
        image='airflow/spark-app',
        container_name='spark_cleansing',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        command='python3 /app/Cleansing.py'
    )

    Transformation = DockerOperator(
        task_id='Transformation',
        max_active_tis_per_dag=1,
        image='airflow/spark-app',
        container_name='spark_cleansing',
        api_version='auto',
        auto_remove=True,
        docker_url='tcp://docker-proxy:2375',
        network_mode='container:spark-master',
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False,
        command='python3 /app/Transformation.py'
    )

    Ingestion >> Cleansing >> Transformation

spark_etl()
