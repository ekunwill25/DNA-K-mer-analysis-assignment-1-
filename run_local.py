import argparse
import time
from multiprocessing import Pool

from core import kmer


def _work(args):
    chunk, k = args
    return kmer.analyse_chunk(chunk, k)


def main(): # run the pipeline on a local FASTA file and print a summary
    ap = argparse.ArgumentParser()
    ap.add_argument("fasta")
    ap.add_argument("--k", type=int, default=8)
    ap.add_argument("--chunk-size", type=int, default=200_000)
    ap.add_argument("--workers", type=int, default=1)
    a = ap.parse_args()

    start = time.time() 
    with open(a.fasta) as f:
        records = kmer.parse_fasta(f.read())
    chunks = kmer.make_chunks(records, a.chunk_size, a.k)

    jobs = [(c, a.k) for c in chunks]
    if a.workers > 1:
        with Pool(a.workers) as pool:
            partials = pool.map(_work, jobs)
    else:
        partials = [_work(j) for j in jobs]

    report = kmer.summarise(kmer.merge(partials), a.k)
    elapsed = time.time() - start

    print(f"records={len(records)} chunks={len(chunks)} workers={a.workers}")
    print(f"total_bases={report['total_bases']}  GC%={report['gc_content_percent']}  "
          f"distinct_kmers={report['distinct_kmers']}")
    print("top 5:", report["top_kmers"][:5])
    print(f"elapsed: {elapsed:.2f}s")


if __name__ == "__main__":
    main()