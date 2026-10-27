# running from project root dir
import random
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent)) # adding project root to path
from core import kmer

def run_pipeline(records, chunk_size, k): # run the pipeline on a list of records which returns the merged result
    chunks = kmer.make_chunks(records, chunk_size, k) # create chunks from the records
    return kmer.merge(kmer.analyse_chunk(c, k) for c in chunks) # analyse each chunk and merge the results

class TestKmer(unittest.TestCase):
    def test_parse_fasta_multi_record(self):
        text = ">a\nACGT\nacgt\n>b\n\nGGCC\n" # test that parse_fasta correctly parses multiple records and uppercases sequences
        self.assertEqual(kmer.parse_fasta(text), ["ACGTACGT", "GGCC"]) # test that parse_fasta correctly parses multiple records and uppercases sequences

    def test_count_kmers_skips_n(self):
        self.assertEqual(kmer.count_kmers("ACNGT", 2), {"AC": 1, "GT": 1}) # test that count_kmers skips kmers containing N

    def test_invalid_k_raises(self): # test that count_kmers raises ValueError for invalid k values
        for bad in (0, -3): # 
            with self.assertRaises(ValueError): 
                kmer.count_kmers("ACGT", bad) 

        def test_chunk_traceability(self): # test that chunks are traceable back to their original record and offset
            chunks = kmer.make_chunks(["AAAAA", "CCC"], 2, 2)
            self.assertEqual([(c["record"], c["start"]) for c in chunks],
                            [(0, 0), (0, 2), (0, 4), (1, 0), (1, 2)])
    
    def test_chunked_equals_unchunked(self):
        # test that chunked analysis produces the same result as unchunked analysis
        random.seed(1)
        records = ["".join(random.choices("ACGTNRY", weights=[10, 10, 10, 10, 1, 1, 1], k=n))
                   for n in (1, 7, 1000, 2503)]
        for k in (1, 3, 8):
            expected = kmer.merge([{
                "kmers": kmer.count_kmers(r, k), "gc": kmer.gc_count(r),
                "bases": sum(1 for c in r if c in kmer.VALID)
            } for r in records])
            for chunk_size in (1, 5, 100, 999, 10_000):
                got = run_pipeline(records, chunk_size, k)
                self.assertEqual(got["kmers"], expected["kmers"], (k, chunk_size))
                self.assertEqual(got["gc"], expected["gc"], (k, chunk_size))
                self.assertEqual(got["bases"], expected["bases"], (k, chunk_size))
 
    def test_summarise(self): # test that summarise correctly calculates GC content and top k-mers
        out = kmer.summarise({"kmers": kmer.Counter({"AA": 3}), "gc": 1, "bases": 4}, 2)
        self.assertEqual(out["gc_content_percent"], 25.0)
        self.assertEqual(out["top_kmers"], [("AA", 3)])
 
 
if __name__ == "__main__":
    unittest.main()
 
