import sys

from submit import CONN, submit

path = sys.argv[1] if len(sys.argv) > 1 else "test.fasta"
name = path.replace("\\", "/").split("/")[-1]
with open(path, "rb") as f:
    submit(f.read(), name, CONN)
print(f"uploaded {name} and queued it for processing")