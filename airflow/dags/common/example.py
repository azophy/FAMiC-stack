import clickhouse_operations as clickhouse
import minio_operations as minio

ch_client = clickhouse.get_client()
minio_client = minio.get_client()

ch_client.query(clickhouse.QUERY_CREATE_DOCKER_LOG_TABLE)
ch_client.query(clickhouse.QUERY_CREATE_DOCKER_LOG_MIGRATION_TABLE)
latest = ch_client.query(clickhouse.QUERY_GET_LATEST_DOCKER_LOG_MIGRATION)
start_after = latest[0][0] if len(latest) > 0 else None

log_list = minio.list_docker_logs(minio_client, start_after=start_after);
for obj in log_list:
    print(obj.object_name, obj.last_modified, obj.size)
    try:
        clickhouse.import_docker_log_from_minio(ch_client, '/container-logs/' + obj.object_name)
        print('success')
    except Exception as err:
        print('error:', err)
