# based on example on https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html
import json

import pendulum

from airflow.decorators import dag, task
@dag(
    schedule="* * * * *", # run every minutes
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["minio", "docker", "clickhouse"],
)
def import_minio_to_clickhouse():
    """
    ### Import Docker Logfiles from Minio into Clickhouse with TaskFlow API
    This is a simple data pipeline example. The repository is in
    [github.com/azophy/docker-airflow-clickhouse-example](https://github.com/azophy/docker-minio-airflow-clickhouse-example)
    """
    @task.virtualenv(requirements=["clickhouse-connect", "minio"], system_site_packages=False)
    def create_tables():
        """
        ### Create tables task
        Create table 'docker_logs' & 'docker_logs_migrations' if not exists
        """
        import common.clickhouse_operations as clickhouse

        ch_client = clickhouse.get_client()
        ch_client.query(clickhouse.QUERY_CREATE_DOCKER_LOG_TABLE)
        ch_client.query(clickhouse.QUERY_CREATE_DOCKER_LOG_MIGRATION_TABLE)

    @task.virtualenv(requirements=["clickhouse-connect", "minio"], system_site_packages=False)
    def get_latest_docker_log_migration():
        """
        ### Get latest docker log migration task
        Get only latest migration to make log import more efficient
        """
        import common.clickhouse_operations as clickhouse

        ch_client = clickhouse.get_client()
        latest = ch_client.query(clickhouse.QUERY_GET_LATEST_DOCKER_LOG_MIGRATION)
        start_after = latest.result_rows[0][0] if len(latest.result_rows) > 0 else None
        return start_after

    @task.virtualenv(requirements=["clickhouse-connect", "minio"], system_site_packages=False)
    def load_to_clickhouse(start_after):
        """
        #### Load to clickhouse task
        Load all listed docker logfiles into clickhouse
        """
        import common.clickhouse_operations as clickhouse
        import common.minio_operations as minio

        ch_client = clickhouse.get_client()
        minio_client = minio.get_client()
        log_list = minio.list_docker_logs(minio_client, start_after=start_after);

        for obj in log_list:
            print(obj.object_name, obj.last_modified, obj.size)
            try:
                clickhouse.import_docker_log_from_minio(ch_client, obj.object_name)
                print('success')
            except Exception as err:
                print('error:', err)

    create_step = create_tables()
    start_after = get_latest_docker_log_migration()
    create_step >> start_after
    load_to_clickhouse(start_after)

import_minio_to_clickhouse()

