import argparse
import json
import os
import sys

CONN = os.environ.get("STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")


def make(n, out):
    table = bytes("ACGT"[b & 3].encode()[0] for b in range(256))   # random byte 
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
    queue.send_message(json.dumps(msg))        # function 1 is started
    print(f"uploaded {name} and queued it for processing")


def results(csv_path=None, plot_path=None):
    from azure.storage.blob import BlobServiceClient

    container = BlobServiceClient.from_connection_string(CONN).get_container_client("final")
    blobs = list(container.list_blobs()) if container.exists() else []
    if not blobs:
        print("No results yet.")
        return
    reports = []
    for b in blobs:
        report = json.loads(container.download_blob(b.name).readall())
        reports.append(report)
        if not csv_path and not plot_path:
            print(f"--- {b.name} ---")
            print(json.dumps(report, indent=2))

    if csv_path:
        import csv
        cols = ["job_id", "input", "chunks", "chunk_size", "k", "total_bases",
                "gc_content_percent", "distinct_kmers", "elapsed_seconds"]
        with open(csv_path, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=cols, extrasaction="ignore")
            w.writeheader()
            w.writerows(reports)
        top_path = csv_path.rsplit(".", 1)[0] + "_top_kmers.csv"
        with open(top_path, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["job_id", "rank", "kmer", "count"])
            for r in reports:
                for rank, (kmer, count) in enumerate(r["top_kmers"], 1):
                    w.writerow([r["job_id"], rank, kmer, count])
        print(f"saved {len(reports)} job(s) to {csv_path} and their top k-mers to {top_path}")

    if plot_path:
        import matplotlib
        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        from statistics import mean

        by_chunks = {}
        for r in reports:
            by_chunks.setdefault(r["chunks"], []).append(r["elapsed_seconds"])
        xs = sorted(by_chunks)
        fig, ax = plt.subplots(figsize=(7, 4.5))
        for x in xs:
            ax.scatter([x] * len(by_chunks[x]), by_chunks[x], color="grey", alpha=0.6, zorder=2)
        ax.plot(xs, [mean(by_chunks[x]) for x in xs], marker="o", color="tab:blue", zorder=3,
                label="mean of runs" if any(len(v) > 1 for v in by_chunks.values()) else "run")
        ax.set_xscale("log")
        ax.set_xticks(xs)
        ax.set_xticklabels(xs)
        ax.set_xlabel("number of chunks (more chunks = more parallel functions)")
        ax.set_ylabel("end-to-end time (s)")
        ax.set_title(f"Pipeline time vs chunks ({reports[0]['total_bases']:,} bases, K={reports[0]['k']})")
        ax.grid(alpha=0.3)
        ax.legend()
        fig.tight_layout()
        fig.savefig(plot_path, dpi=150)
        print(f"saved graph to {plot_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    sub = ap.add_subparsers(dest="cmd", required=True)
    m = sub.add_parser("make");    m.add_argument("bases", type=int); m.add_argument("out", nargs="?", default="test.fasta")
    u = sub.add_parser("upload");  u.add_argument("fasta"); u.add_argument("--chunk-size", type=int); u.add_argument("--k", type=int)
    r = sub.add_parser("results"); r.add_argument("--csv", help="save results to this CSV file")
    r.add_argument("--plot", help="save a time-vs-chunks graph to this PNG file")
    a = ap.parse_args()
    if a.cmd == "make":
        make(a.bases, a.out)
    elif a.cmd == "upload":
        upload(a.fasta, a.chunk_size, a.k)
    else:
        results(a.csv, a.plot)