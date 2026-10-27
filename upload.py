import sys
from azure.storage.blob import BlobServiceClient

CONN = "UseDevelopmentStorage=true"   # local Azurite

path = sys.argv[1] if len(sys.argv) > 1 else "test.fasta"
name = path.replace("\\", "/").split("/")[-1]

container = BlobServiceClient.from_connection_string(CONN).get_container_client("fasta-input")
if not container.exists():
    container.create_container()
with open(path, "rb") as f:
    container.upload_blob(name, f, overwrite=True)
print(f"uploaded {name} to fasta-input")