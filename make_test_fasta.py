# asked Ai to provide this code for me, and break it down in steps
import random, sys
n = int(sys.argv[1]) if len(sys.argv) > 1 else 5_000_000
out = sys.argv[2] if len(sys.argv) > 2 else "test.fasta"
with open(out, "w") as f:
    f.write(">random_test_sequence\n")
    for _ in range(0, n, 80):
        f.write("".join(random.choices("ACGT", k=80)) + "\n")
print(f"wrote {n} bases to {out}")