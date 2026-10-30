import json
import logging
import os
import re
import socket
import time
import uuid
from collections import Counter
from typing import Iterable

import azure.functions as func
from azure.storage.blob import BlobServiceClient
from azure.storage.queue import QueueClient, TextBase64EncodePolicy



# core k-mer logic, independent of Azure Functions. These functions are called by the Azure Functions below.
VALID = set("ACGT") # only unambiguous bases count; N, R, Y, etc. are skipped

def parse_fasta(text: str) -> list[str]: # return a list of strings
    # Function turns FASTA text into one uppercase sequence per record
    # Return an uppercase sequence string per FASTA record (dropping headers)
    records, cur = [], []
    for line in text.splitlines():
        line = line.strip()
        if line.startswith(">"):
            if cur:
                records.append("".join(cur))
                cur = []
        elif line:
            cur.append(line.upper())
    if cur:
        records.append("".join(cur))
    return records

def make_chunks(records: list[str], chunk_size: int, k: int) -> list[dict]: # return a list of dictionaries
    # splits the records into chunks, with each chunk carrying K-1 bases, so boundary k-mers are not lost
    # each chunk also records which FASTA record it came from and its offset within that record
    if chunk_size < 1 or k < 1:
        raise ValueError("chunk_size and k must be >= 1")
    chunks = []
    for rec_idx, seq in enumerate(records): # rec_idx = index of the record this chunk came from
        for start in range(0, len(seq), chunk_size):
            end = start + chunk_size
            chunks.append({
                "seq": seq[start:end + k - 1], # own bases + k-1 tail bases
                "own_len": len(seq[start:end]), # length of the chunk's own region (excludes the tail)
                "record": rec_idx,
                "start": start,
            })
    return chunks

def count_kmers(seq: str, k: int) -> Counter: # return a counter
    # count every length-k window in sequence, returning a counter of k-mers
    if k < 1:
        raise ValueError(f"k must be >= 1, got {k}")
    counts = Counter()
    for i in range(len(seq) - k + 1):
        kmer = seq[i:i + k]
        if set(kmer) <= VALID: # skip any window containing a non-ACGT base
            counts[kmer] += 1
    return counts

def gc_count(seq: str) -> int: # return an integer
    # count the number of G and C bases in a sequence
    return seq.count("G") + seq.count("C")

def process_chunk(chunk: dict, k: int) -> dict: # return a dictionary
    # CPU-bound step (function 2). Returns a JSON-serialisable partial result 
    seq, own_len = chunk["seq"], chunk["own_len"]
    own = seq[:own_len] # own region only: tail belongs to next chunk
    return {
        "kmers": dict(count_kmers(seq, k)),
        "gc": gc_count(own),
        "bases": sum(1 for c in own if c in VALID) # count only valid bases in own region
    }

def merge(partials: Iterable[dict]) -> dict: # return a dictionary
    # sum partial results together
    kmers, gc, bases = Counter(), 0, 0 # initialize empty k-mer counts, GC count, and base count
    for p in partials:
        kmers.update(p["kmers"]) # merge the k-mer counts from the chunk into the total k-mer counts
        gc += p["gc"]  # increment GC count by the GC count of the chunk
        bases += p["bases"] # increment base count by the base count of the chunk
    return {"kmers": kmers, "gc": gc, "bases": bases} # return the total k-mer counts, GC count, and base count

def summarise(merged: dict, k: int, top_n: int = 20) -> dict: # return a dictionary
    # turn the merged totals into the final report, including the top N k-mers and their counts
    bases = merged["bases"] # total number of bases in the merged result
    return {
        "k": k, 
        "total_bases": bases,
        "gc_content_percent": round(100 * merged["gc"] / bases, 3) if bases else 0.0, # calculate GC content percentage, rounded to 3 decimal places
        "distinct_kmers": len(merged["kmers"]), 
        "top_kmers": merged["kmers"].most_common(top_n), # return the top N most common k-mers and their counts
    }


# Azure Functions app, with three functions: split_fasta, analyse_chunk, and merge_results
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
def _conn():
    # connection string for our own blob/queue calls. On some plans AzureWebJobsStorage is
    # identity-based (no connection string), so fall back to STORAGE_CONNECTION_STRING
    return os.environ.get(CONN) or os.environ["STORAGE_CONNECTION_STRING"]


def _blob_service():
    return BlobServiceClient.from_connection_string(_conn())


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
        _conn(), name, message_encode_policy=TextBase64EncodePolicy()
    )
    try:
        q.create_queue()
    except Exception:
        pass  # already exists
    return q


# Function 1: split_fasta is triggered by a message on the start queue (upload.py sends it after uploading the FASTA).
# Queue-triggered, not blob-triggered, because the Flex Consumption plan only supports Event Grid-based blob triggers.
@app.queue_trigger(arg_name="msg", queue_name="start", connection=CONN)
def split_fasta(msg: func.QueueMessage):
    started_at = time.time()
    job_id = uuid.uuid4().hex[:12]

    request = json.loads(msg.get_body().decode("utf-8"))
    input_name = request["blob"]
    chunk_size, k = parse_overrides(input_name)            # file-name tags, else app settings
    chunk_size = request.get("chunk_size") or chunk_size   # values in the message win
    k = request.get("k") or k
    tag = request.get("tag", "")                           # lets a client recognise its own result

    raw = _blob_service().get_blob_client("fasta-input", input_name).download_blob().readall()
    records = parse_fasta(raw.decode("utf-8"))
    chunks = make_chunks(records, chunk_size, k)

    chunk_container = _container("chunks")
    queue = _queue("chunks")
    for i, chunk in enumerate(chunks):
        blob_name = f"{job_id}/{i}.json"
        chunk_container.upload_blob(blob_name, json.dumps(chunk), overwrite=True)
        queue.send_message(json.dumps({
            "job_id": job_id, "index": i, "total": len(chunks),
            "blob": blob_name, "started_at": started_at,
            "k": k, "chunk_size": chunk_size, "input": input_name, "tag": tag,
        }))
    logging.info("Job %s: %s (%d records, K=%d, chunk_size=%d) -> %d chunks",
                 job_id, input_name, len(records), k, chunk_size, len(chunks))


# Function 2: analyse_chunk is triggered by a queue message, one per chunk, so chunks are analysed in parallel.
@app.queue_trigger(arg_name="msg", queue_name="chunks", connection=CONN)
def analyse_chunk(msg: func.QueueMessage):
    t_start = time.time()
    job = json.loads(msg.get_body().decode("utf-8"))

    raw = _blob_service().get_blob_client("chunks", job["blob"]).download_blob().readall()
    chunk = json.loads(raw)
    t_compute = time.time()
    result = process_chunk(chunk, job["k"])
    compute_s = time.time() - t_compute
    # timing + instance info, used by merge_results to show how well the work was parallelised
    result["meta"] = {"start": t_start, "compute_s": compute_s,
                      "host": socket.gethostname(), "pid": os.getpid()}

    results = _container("results")
    result["meta"]["end"] = time.time()
    results.upload_blob(f"{job['job_id']}/{job['index']}.json",
                        json.dumps(result), overwrite=True)

    # check if all chunks are done, and if so, send a message to the merge queue
    done = sum(1 for _ in results.list_blobs(name_starts_with=f"{job['job_id']}/"))
    if done == job["total"]:
        _queue("merge").send_message(json.dumps({
            "job_id": job["job_id"], "total": job["total"],
            "started_at": job["started_at"],
            "k": job["k"], "chunk_size": job["chunk_size"], "input": job["input"],
            "tag": job.get("tag", ""),
            "enqueued_at": time.time(),   # lets merge_results measure how long the message waited
        }))


# Function 3: merge_results is triggered by a message on the merge queue, and writes the final report to the final container.
@app.queue_trigger(arg_name="msg", queue_name="merge", connection=CONN)
def merge_results(msg: func.QueueMessage):
    merge_start = time.time()
    job = json.loads(msg.get_body().decode("utf-8"))
    results = _container("results")

    partials = [json.loads(results.download_blob(f"{job['job_id']}/{i}.json").readall())
                for i in range(job["total"])]
    final = summarise(merge(partials), job["k"])

    metas = [p["meta"] for p in partials if "meta" in p]
    if metas:
        first_start = min(m["start"] for m in metas)
        last_end = max(m["end"] for m in metas)
        events = sorted([(m["start"], 1) for m in metas] + [(m["end"], -1) for m in metas])
        running = peak = 0
        for _, change in events:                 # sweep through start/end times
            running += change
            peak = max(peak, running)
        final.update({
            "split_to_first_chunk_s": round(first_start - job["started_at"], 2),
            "analysis_span_s": round(last_end - first_start, 2),
            "chunk_compute_total_s": round(sum(m["compute_s"] for m in metas), 2),
            "max_parallel_chunks": peak,
            "instances_used": len({m["host"] for m in metas}),
            "workers_used": len({(m["host"], m["pid"]) for m in metas}),
            "last_chunk_to_merge_s": round(merge_start - last_end, 2),   # last chunk done -> merge running
        })
    if "enqueued_at" in job:
        final["merge_pickup_s"] = round(merge_start - job["enqueued_at"], 2)   # queue wait + cold start
    final["merge_s"] = round(time.time() - merge_start, 2)
    final.update({
        "job_id": job["job_id"],
        "input": job["input"],
        "tag": job.get("tag", ""),
        "chunk_size": job["chunk_size"],
        "chunks": job["total"],
        "elapsed_seconds": round(time.time() - job["started_at"], 2),
    })

    _container("final").upload_blob(f"{job['job_id']}.json",
                                    json.dumps(final, indent=2), overwrite=True)
    logging.info("Job %s finished in %.2fs", job["job_id"], final["elapsed_seconds"])