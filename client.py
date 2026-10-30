import argparse
import json
import os
import sys

CONN = os.environ.get("STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")


def make(n, out):
    table = bytes("ACGT"[b & 3].encode()[0] for b in range(256))   # random byte -> A/C/G/T
    line, block = 80, 80 * 100_000
    with open(out, "wb") as f:
        f.write(b">random_test_sequence\n")
        left = n
        while left > 0:
            size = min(block, left)
            data = os.urandom(size).translate(table)
            f.write(b"\n".join(data[i:i + line] for i in range(0, size, line)) + b"\n")
            left -= size
    print(f"wrote {n} bases to {out}")


def upload(path, chunk_size=None, k=None):
    from azure.storage.blob import BlobServiceClient
    from azure.storage.queue import QueueClient, TextBase64EncodePolicy

    name = path.replace("\\", "/").split("/")[-1]
    container = BlobServiceClient.from_connection_string(CONN).get_container_client("fasta-input")
    if not container.exists():
        container.create_container()
    with open(path, "rb") as f:
        container.upload_blob(name, f, overwrite=True)

    queue = QueueClient.from_connection_string(
        CONN, "start", message_encode_policy=TextBase64EncodePolicy())
    try:
        queue.create_queue()
    except Exception:
        pass  # already exists
    msg = {"blob": name}
    if chunk_size:
        msg["chunk_size"] = chunk_size
    if k:
        msg["k"] = k
    queue.send_message(json.dumps(msg))        # this message starts function 1
    print(f"uploaded {name} and queued it for processing")


def results():
    from azure.storage.blob import BlobServiceClient

    container = BlobServiceClient.from_connection_string(CONN).get_container_client("final")
    blobs = list(container.list_blobs()) if container.exists() else []
    if not blobs:
        print("No results yet.")
    for b in blobs:
        print(f"--- {b.name} ---")
        print(container.download_blob(b.name).readall().decode())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("make");    m.add_argument("bases", type=int); m.add_argument("out", nargs="?", default="test.fasta")
    u = sub.add_parser("upload");  u.add_argument("fasta"); u.add_argument("--chunk-size", type=int); u.add_argument("--k", type=int)
    sub.add_parser("results")
    a = ap.parse_args()
    if a.cmd == "make":
        make(a.bases, a.out)
    elif a.cmd == "upload":
        upload(a.fasta, a.chunk_size, a.k)
    else:
        results()