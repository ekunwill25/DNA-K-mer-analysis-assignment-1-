from collections import Counter
from typing import Iterable

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

def analyse_chunk(chunk: dict, k: int) -> dict: # return a dictionary
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