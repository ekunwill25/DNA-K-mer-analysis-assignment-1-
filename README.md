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
- `client.py` make a test file, upload it, read the results (as JSON, CSV or a graph)
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

## Uploading
```
python client.py upload test.fasta                          # default chunk size (200,000 bases) and K=8
python client.py upload big.fasta --chunk-size 5000000      # fewer, larger chunks = fewer parallel functions
python client.py upload big.fasta --chunk-size 5000000 --k 10
```

## Viewing results
Each finished job writes a report to the `final` storage container. Three ways to read them:

**1. Print them** (JSON, one report per job)
```
python client.py results
```

**2. Save as CSV**
```
python client.py results --csv results.csv
```
Creates two files:
- `results.csv` one row per job: `job_id, input, chunks, chunk_size, k, total_bases, gc_content_percent, distinct_kmers, elapsed_seconds`
- `results_top_kmers.csv` the top 20 k-mers per job: `job_id, rank, kmer, count`

**3. Draw a graph** (end-to-end time against number of chunks)
```
pip install matplotlib                  # only needed for --plot
python client.py results --plot scaling.png
```
A graph needs jobs with *different* chunk counts, so upload the same file at several `--chunk-size`
values first. Wait for each job to finish before starting the next, otherwise the jobs compete for
instances and the timings are not comparable. The first job after a quiet period is slower because
Azure has to start instances, so repeat each setting a few times if you want a clean mean.

`--csv` and `--plot` can be used together:
```
python client.py results --csv results.csv --plot scaling.png
```
Do not add matplotlib to `requirements.txt`; that file is what gets deployed, and the function app does not need it.

## Using real Azure storage
By default `client.py` talks to local Azurite. To use the deployed app's storage account, set the
connection string first (PowerShell; it lasts for that terminal window only):
```
$env:STORAGE_CONNECTION_STRING = (az storage account show-connection-string --name <storage-account> --resource-group <resource-group> --query connectionString -o tsv)
```
Check which one you are pointing at with `echo $env:STORAGE_CONNECTION_STRING` (empty = local Azurite).

## Deploy
Create a Python 3.12 Flex Consumption function app, then:
```
func azure functionapp publish <app-name>
```