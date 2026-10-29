import argparse
import csv
import json
import os
import time

from azure.storage.blob import BlobServiceClient

from submit import CONN, start_job, upload_fasta

reports = {}   # blob name -> report, for everything seen so far


def wait_for_result(final, tag, seen, timeout):
    """Poll the final container until the report carrying this tag appears."""
    start = time.time()
    last_msg = start
    warned = False
    while time.time() - start < timeout:
        for b in final.list_blobs():
            if b.name in seen:
                continue
            seen.add(b.name)
            report = json.loads(final.download_blob(b.name).readall())
            reports[b.name] = report
            if "tag" not in report and not warned:
                print("  WARNING: a new report has no 'tag' field - the deployed functions "
                      "are running OLD code. Redeploy / restart them, then re-run.")
                warned = True
        for report in reports.values():
            if report.get("tag") == tag:
                return report
        if time.time() - last_msg >= 15:
            print(f"  ...still waiting ({int(time.time() - start)}s)")
            last_msg = time.time()
        time.sleep(1)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fasta")
    ap.add_argument("--chunk-sizes", type=int, nargs="+",
                    default=[2_000_000, 500_000, 200_000, 50_000])
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=1800, help="seconds to wait per run")
    ap.add_argument("--out", default="benchmark_results.csv")
    a = ap.parse_args()

    final = BlobServiceClient.from_connection_string(CONN).get_container_client("final")
    if not final.exists():
        final.create_container()
    seen = {b.name for b in final.list_blobs()}      # ignore reports from earlier runs

    stamp = int(time.time())
    stem = os.path.splitext(os.path.basename(a.fasta))[0]
    blob_name = f"{stem}_{stamp}.fasta"
    with open(a.fasta, "rb") as f:
        data = f.read()
    print(f"uploading {len(data) / 1e6:.1f} MB as {blob_name} ...")
    upload_fasta(data, blob_name, CONN)
    print("upload done\n")

    rows = []
    for chunk_size in a.chunk_sizes:
        for rep in range(1, a.repeats + 1):
            tag = f"c{chunk_size}-k{a.k}-r{rep}-{stamp}"
            t0 = time.time()
            start_job(blob_name, CONN, chunk_size=chunk_size, k=a.k, tag=tag)
            print(f"started chunk_size={chunk_size} run {rep} - waiting for result...")
            report = wait_for_result(final, tag, seen, a.timeout)
            wall = time.time() - t0
            if report is None:
                print(f"  TIMED OUT after {a.timeout}s")
                continue
            rows.append({
                "chunk_size": chunk_size, "run": rep, "chunks": report["chunks"],
                "k": report["k"], "total_bases": report["total_bases"],
                "function_seconds": report["elapsed_seconds"],
                "wall_seconds": round(wall, 2),
                "split_to_first_chunk_s": report.get("split_to_first_chunk_s"),
                "analysis_span_s": report.get("analysis_span_s"),
                "chunk_compute_total_s": report.get("chunk_compute_total_s"),
                "max_parallel_chunks": report.get("max_parallel_chunks"),
                "instances_used": report.get("instances_used"),
                "merge_s": report.get("merge_s"),
                "last_chunk_to_merge_s": report.get("last_chunk_to_merge_s"),
                "merge_pickup_s": report.get("merge_pickup_s"),
            })
            print(f"  chunks={report['chunks']:>4}  function={report['elapsed_seconds']:>7.2f}s  "
                  f"wall={wall:>7.2f}s  | split->first chunk={report.get('split_to_first_chunk_s')}s  "
                  f"analysis span={report.get('analysis_span_s')}s  "
                  f"max parallel={report.get('max_parallel_chunks')}  "
                  f"instances={report.get('instances_used')}  merge={report.get('merge_s')}s  "
                  f"last chunk->merge={report.get('last_chunk_to_merge_s')}s  "
                  f"merge pickup={report.get('merge_pickup_s')}s")

    if rows:
        with open(a.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        print(f"\nsaved {len(rows)} rows to {a.out}")


if __name__ == "__main__":
    main()