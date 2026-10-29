import json
import os

from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

CONN = os.environ.get("STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")


def upload_fasta(data: bytes, name: str, conn: str = CONN):
    container = BlobServiceClient.from_connection_string(conn).get_container_client("fasta-input")
    if not container.exists():
        container.create_container()
    container.upload_blob(name, data, overwrite=True)


def start_job(name: str, conn: str = CONN, chunk_size=None, k=None, tag=None):
    """Queue a job for an already-uploaded FASTA. chunk_size / k / tag are optional."""
    queue = QueueClient.from_connection_string(
        conn, "start", message_encode_policy=TextBase64EncodePolicy())
    try:
        queue.create_queue()
    except Exception:
        pass  # already exists
    msg = {"blob": name}
    if chunk_size:
        msg["chunk_size"] = chunk_size
    if k:
        msg["k"] = k
    if tag:
        msg["tag"] = tag
    queue.send_message(json.dumps(msg))


def submit(data: bytes, name: str, conn: str = CONN, **job_options):
    upload_fasta(data, name, conn)
    start_job(name, conn, **job_options)