# file_uploader.py MinIO Python SDK example
from minio import Minio

# in this example we hardcoded the details. obviously this should not be done in production
def get_client():
    return Minio("minio:9000",
        access_key="minio-root",
        secret_key="minio-root",
    )

def list_docker_logs(client, start_after=None):
    return client.list_objects(
        "container-logs", recursive=True, start_after=start_after,
    )
