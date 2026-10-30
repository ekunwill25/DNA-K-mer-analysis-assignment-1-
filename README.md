# DNA k-mer analysis: three chained Azure Functions

Upload a FASTA file and three functions run one after another, automatically:

```
client.py upload  ->  "start" queue
                          |  queue trigger
                          v
  [1] split_fasta     splits the sequence into chunks, writes them to blob storage,
                      and queues one message per chunk                     (fan-out)
                          |  queue trigger "chunks" (one run per chunk, in parallel)
                          v
  [2] analyse_chunk   counts k-mers and GC bases in its chunk, saves a partial result;
                      the last chunk to finish queues a message on "merge"
                          |  queue trigger "merge"
                          v
  [3] merge_results   adds up all partial results and writes final/<job>.json  (fan-in)
```

Queues (not a blob trigger) start function 1 because the Flex Consumption plan only
supports Event Grid-based blob triggers.

## Files
- `function_app.py` the three functions and the k-mer logic they call
- `client.py` make a test file, upload it, read the results
- `host.json`, `requirements.txt` Azure Functions config and dependencies

## Run locally
Needs Python 3.10-3.14, Azure Functions Core Tools and Azurite (started with `--skipApiVersionCheck`).
```
pip install -r requirements.txt
func start                              # in one terminal, with Azurite running
python client.py make 2000000 test.fasta
python client.py upload test.fasta
python client.py results
```

## Deploy
Create a Python 3.12 Flex Consumption function app, then `func azure functionapp publish <app-name>`.
To use Azure storage from `client.py`, set `STORAGE_CONNECTION_STRING` first.