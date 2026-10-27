import json
import logging
import os
import re
import time
import uuid

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy

from core import kmer

app = func.FunctionApp()

CONN = "AzureWebJobsStorage"                         # app setting holding the storage connection string
DEFAULT_K = int(os.getenv("KMER_SIZE", "8"))         # bigger K = more CPU work
DEFAULT_CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "200000"))  # bases per chunk


def parse_overrides(filename: str):
    """Read optional __c<chunk_size> and __k<k> tags from the uploaded file name."""
    c = re.search(r"__c(\d+)", filename)
    k = re.search(r"__k(\d+)", filename)
    return (int(c.group(1)) if c else DEFAULT_CHUNK_SIZE,
            int(k.group(1)) if k else DEFAULT_K)


# Azure helpers
def _blob_service():
    return BlobServiceClient.from_connection_string(os.environ[CONN])


def _container(name):
    c = _blob_service().get_container_client(name)
    if not c.exists():
        try:
            c.create_container()
        except Exception:
            pass  # another instance created it first
    return c


def _queue(name):
    q = QueueClient.from_connection_string(
        os.environ[CONN], name, message_encode_policy=TextBase64EncodePolicy()
    )
    try:
        q.create_queue()
    except Exception:
        pass  # already exists
    return q


# Function 1: split_fasta is triggered by a blob upload to the fasta-input container.
@app.blob_trigger(arg_name="blob", path="fasta-input/{name}", connection=CONN)
def split_fasta(blob: func.InputStream):
    started_at = time.time()
    job_id = uuid.uuid4().hex[:12]
    input_name = blob.name.split("/")[-1]
    chunk_size, k = parse_overrides(input_name)

    records = kmer.parse_fasta(blob.read().decode("utf-8"))
    chunks = kmer.make_chunks(records, chunk_size, k)

    chunk_container = _container("chunks")
    queue = _queue("chunks")
    for i, chunk in enumerate(chunks):
        blob_name = f"{job_id}/{i}.json"
        chunk_container.upload_blob(blob_name, json.dumps(chunk), overwrite=True)
        queue.send_message(json.dumps({
            "job_id": job_id, "index": i, "total": len(chunks),
            "blob": blob_name, "started_at": started_at,
            "k": k, "chunk_size": chunk_size, "input": input_name,
        }))
    logging.info("Job %s: %s (%d records, K=%d, chunk_size=%d) -> %d chunks",
                 job_id, input_name, len(records), k, chunk_size, len(chunks))


# Function 2: analyse is in the next cell, because it is triggered by a queue message, not a blob upload.
@app.queue_trigger(arg_name="msg", queue_name="chunks", connection=CONN)
def analyse_chunk(msg: func.QueueMessage):
    job = json.loads(msg.get_body().decode("utf-8"))

    raw = _blob_service().get_blob_client("chunks", job["blob"]).download_blob().readall()
    result = kmer.analyse_chunk(json.loads(raw), job["k"])

    results = _container("results")
    results.upload_blob(f"{job['job_id']}/{job['index']}.json",
                        json.dumps(result), overwrite=True)

    # check if all chunks are done, and if so, send a message to the merge queue
    done = sum(1 for _ in results.list_blobs(name_starts_with=f"{job['job_id']}/"))
    if done == job["total"]:
        _queue("merge").send_message(json.dumps({
            "job_id": job["job_id"], "total": job["total"],
            "started_at": job["started_at"],
            "k": job["k"], "chunk_size": job["chunk_size"], "input": job["input"],
        }))


# Function 3: merge_results is triggered by a message on the merge queue, and writes the final report to the final container.
@app.queue_trigger(arg_name="msg", queue_name="merge", connection=CONN)
def merge_results(msg: func.QueueMessage):
    job = json.loads(msg.get_body().decode("utf-8"))
    results = _container("results")

    partials = (json.loads(results.download_blob(f"{job['job_id']}/{i}.json").readall())
                for i in range(job["total"]))
    final = kmer.summarise(kmer.merge(partials), job["k"])
    final.update({
        "job_id": job["job_id"],
        "input": job["input"],
        "chunk_size": job["chunk_size"],
        "chunks": job["total"],
        "elapsed_seconds": round(time.time() - job["started_at"], 2),
    })

    _container("final").upload_blob(f"{job['job_id']}.json",
                                    json.dumps(final, indent=2), overwrite=True)
    logging.info("Job %s finished in %.2fs", job["job_id"], final["elapsed_seconds"])