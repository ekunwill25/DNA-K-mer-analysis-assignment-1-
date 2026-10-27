from azure.storage.blob import BlobServiceClient

CONN = "UseDevelopmentStorage=true"   # local Azurite

container = BlobServiceClient.from_connection_string(CONN).get_container_client("final")
if not container.exists():
    print("No 'final' container yet - the pipeline hasn't finished a job.")
else:
    blobs = list(container.list_blobs())
    if not blobs:
        print("No results yet.")
    for b in blobs:
        print(f"--- {b.name} ---")
        print(container.download_blob(b.name).readall().decode())