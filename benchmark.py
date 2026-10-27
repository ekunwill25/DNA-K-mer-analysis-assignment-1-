import argparse
import csv
import json
import os
import time

from azure.storage.blob import BlobServiceClient

CONN = os.environ.get("STORAGE_CONNECTION_STRING", "UseDevelopmentStorage=true")


def wait_for_result(final, input_name, seen, timeout):
    """Poll the final container until a report for this upload appears."""
    start = time.time()
    deadline = start + timeout
    last_msg = start
    warned = False
    while time.time() < deadline:
        for b in final.list_blobs():
            if b.name in seen:
                continue
            seen.add(b.name)
            report = json.loads(final.download_blob(b.name).readall())
            seen_reports[b.name] = report
            if "input" not in report and not warned:
                print("  WARNING: a report with no 'input' field appeared. The running "
                      "functions are using OLD code - stop 'func start' (Ctrl+C) and "
                      "start it again, then re-run this script.")
                warned = True
        for report in seen_reports.values():
            if report.get("input") == input_name:
                return report
        if time.time() - last_msg >= 15:
            print(f"  ...still waiting ({int(time.time() - start)}s). "
                  "Check the func start terminal for activity.")
            last_msg = time.time()
        time.sleep(1)
    return None


seen_reports = {}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("fasta")
    ap.add_argument("--chunk-sizes", type=int, nargs="+",
                    default=[2_000_000, 500_000, 200_000, 50_000])
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--repeats", type=int, default=1)
    ap.add_argument("--timeout", type=int, default=900, help="seconds to wait per run")
    ap.add_argument("--out", default="benchmark_results.csv")
    a = ap.parse_args()

    bs = BlobServiceClient.from_connection_string(CONN)
    inp = bs.get_container_client("fasta-input")
    final = bs.get_container_client("final")
    for c in (inp, final):
        if not c.exists():
            c.create_container()
    seen = {b.name for b in final.list_blobs()}   # ignore reports that existed before this run
    with open(a.fasta, "rb") as f:
        data = f.read()
    stem = os.path.splitext(os.path.basename(a.fasta))[0]
    stamp = int(time.time())

    rows = []
    for chunk_size in a.chunk_sizes:
        for rep in range(1, a.repeats + 1):
            name = f"{stem}__c{chunk_size}__k{a.k}__r{rep}_{stamp}.fasta"
            t0 = time.time()
            inp.upload_blob(name, data, overwrite=True)
            print(f"uploaded {name} - waiting for result...")
            report = wait_for_result(final, name, seen, a.timeout)
            wall = time.time() - t0
            if report is None:
                print(f"chunk_size={chunk_size} run {rep}: TIMED OUT after {a.timeout}s")
                continue
            rows.append({
                "chunk_size": chunk_size, "run": rep, "chunks": report["chunks"],
                "k": report["k"], "total_bases": report["total_bases"],
                "function_seconds": report["elapsed_seconds"],
                "wall_seconds": round(wall, 2),
            })
            print(f"chunk_size={chunk_size:>9}  run {rep}  chunks={report['chunks']:>4}  "
                  f"function={report['elapsed_seconds']:>7.2f}s  wall={wall:>7.2f}s")

    if rows:
        with open(a.out, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=rows[0].keys())
            w.writeheader()
            w.writerows(rows)
        print(f"\nsaved {len(rows)} rows to {a.out}")


if __name__ == "__main__":
    main()