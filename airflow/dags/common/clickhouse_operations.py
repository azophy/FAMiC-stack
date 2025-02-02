import clickhouse_connect

# in this example we hardcoded the details. obviously this should not be done in production
def get_client():
    return clickhouse_connect.get_client(
        host='clickhouse',
        port=8123,
        username='default',
        password='clickhouse-root',
    )

QUERY_CREATE_DOCKER_LOG_TABLE="""
    CREATE TABLE IF NOT EXISTS docker_logs (
      `datetime` DateTime64(6), -- datetime with microseconds precision
      `stream` String,
      `container_id` String,
      `container_name` String,
      `log` String,
      `filepath` String,
    )
    ENGINE = MergeTree
    ORDER BY (container_name, datetime)
    ;
"""

# in this example we hardcoded the details. obviously this should not be done in production
def import_docker_log_from_minio(client, minio_path):
    minio_url='http://minio:9000'
    minio_username='minio-root'
    minio_password='minio-root'

    query_exists=f"""
        SELECT status
        FROM docker_logs_migrations
        WHERE name='{minio_path}'
    """
    exists_result=client.query(query_exists)
    status = exists_result.first_row[0] if (len(exists_result.result_rows) > 0) else None
    if (status == 'success'):
        raise Exception('this log already imported')

    query=f"""
        INSERT INTO docker_logs
        SELECT
          date as datetime,
          stream,
          container_id,
          container_name,
          log,
          filepath
        FROM
        s3('{minio_url}/container-logs/{minio_path}', '{minio_username}', '{minio_password}', JSONEachRow)
        ;
    """
    client.command(query)

    if (status is None):
        client.command(f"""
            insert into docker_logs_migrations(datetime, name, status)
            values(now(), '{minio_path}', 'success')
         """)
    else:
        client.command(f"""
            update docker_logs_migrations
            set status='success'
            where name='{minio_path}
         """)

QUERY_CREATE_DOCKER_LOG_MIGRATION_TABLE="""
    CREATE TABLE IF NOT EXISTS docker_logs_migrations (
      `datetime` Datetime,
      `name` String,
      `status` String,
    )
    ENGINE = MergeTree
    ORDER BY (datetime, name)
    ;
"""

QUERY_GET_LATEST_DOCKER_LOG_MIGRATION="""
    SELECT name
    FROM docker_logs_migrations
    where status = 'success'
    ORDER BY datetime, name DESC
    LIMIT 1
"""


