# Generic Data-to-Graph Pipeline

This project is a small, auditable, entity-agnostic data pipeline. It ingests
records from any REST API, keeps the original source payload unchanged, maps
it to a canonical attribute shape you declare, and upserts the result into
Neo4j as a knowledge graph. Nothing in the pipeline assumes a specific entity
type (orders, shipments, invoices, customers, ...): the node label, its
attributes, and its relationships are all driven by a declarative mapping
config and a JSON Schema that you supply per data source.

The project is designed to make the boundary between source data, derived
data, and synthetic demonstration data explicit. PostgreSQL is the system of
record for every pipeline layer, MinIO is the immutable raw landing zone,
Prefect orchestrates the pipeline stages, and the Streamlit application
provides a control panel for running the pipeline and inspecting its output.

## What the project does

The default pipeline, orchestrated by Prefect
([`orchestration/flows.py`](orchestration/flows.py)), is:

```text
Any REST API source
      |
      v
MinIO otc-raw bucket            Immutable raw JSON landing object
      |
      v
Parquet twin (MinIO)            Columnar replay copy of the same batch
      |
      v
raw.api_records (Postgres)      Hydrated from the Parquet twin
      |
      v
staging.preprocessed_records    Source payload + mapped attributes + provenance
      |
      v
canonical.otc_records            entity.type / entity.id / attributes / relationships
      |
      v
Neo4j graph                     Node label = entity.type; edges from "relationships"
      |
      v
Streamlit control panel          Runs the flow and renders the resulting graph
```

Each run is tracked with a UUID `batch_id`. The ingestion layer stores both
successful and failed responses with an ingestion status and optional error
message. Later stages are idempotent: records already represented in the next
layer are not inserted again.

## Repository layout

| Path | Purpose |
| --- | --- |
| `app/streamlit_app.py` | Streamlit control panel; runs the default pipeline via Prefect |
| `orchestration/flows.py` | Prefect flow: MinIO ingest -> Parquet -> Postgres -> Neo4j |
| `ingestion/generic_rest.py` | Configurable REST API ingestion for any source |
| `ingestion/object_storage.py` | MinIO/S3 landing-zone upload helpers |
| `ingestion/raw_parquet.py` | Converts MinIO landing batches to Parquet; hydrates Postgres |
| `ingestion/runner.py` | Legacy local ERP-shaped API ingestion prototype |
| `source_api/app/main.py` | FastAPI synthetic `/orders` source |
| `transformation/preprocess.py` | Builds staging records; applies the source mapping |
| `transformation/generic_mapper.py` | Declarative payload -> canonical attribute/relationship mapper |
| `transformation/enrichment.py` | Legacy demo enrichment, used only when no mapping is configured |
| `transformation/normalize.py` | Builds the canonical entity/attributes/relationships envelope |
| `transformation/canonical_schema.py` | Projects and validates canonical output against your JSON Schema |
| `transformation/field_provenance.yaml` | Field-level source and derivation rules for the legacy demo path |
| `upsert_merge.py` | Loads canonical Postgres rows into Neo4j as generic nodes/relationships |
| `database/raw.sql` | PostgreSQL raw schema |
| `database/staging.sql` | PostgreSQL staging schema |
| `database/canonical.sql` | PostgreSQL canonical schema |

The `source_api` and `ingestion/runner.py` files represent an ERP-shaped
orders path, kept as a second test harness with a schema different from a
plain REST API. A live SAP or Oracle connector can replace its HTTP URL
without changing the raw, staging, or canonical interfaces.

## Prerequisites

- Python 3.11 or a compatible recent Python version
- PostgreSQL with permission to create schemas and tables
- MinIO (or another S3-compatible store) for the raw landing zone
- A Neo4j instance (AuraDB or self-hosted) for the graph output
- Credentials for whichever REST API you are ingesting
- Windows PowerShell, macOS/Linux shell, or an equivalent terminal

The Python dependencies are pinned in
[`requirements.txt`](requirements.txt), including `psycopg2-binary`,
`requests`, `PyYAML`, and `streamlit`.

## Installation

Create and activate a virtual environment from the repository root:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

If PowerShell execution policy prevents activation, run the commands through
an already activated Python environment instead.

## Configuration

Set these environment variables before running the application or CLI:

| Variable | Required | Description |
| --- | --- | --- |
| `DATABASE_URL` | Yes | PostgreSQL connection string, for example `postgresql://user:password@localhost:5432/otc` |
| `NEO4J_URI` | Yes | Neo4j connection URI, for example `neo4j+s://<instance>.databases.neo4j.io` |
| `NEO4J_USERNAME` | Yes | Neo4j username |
| `NEO4J_PASSWORD` | Yes | Neo4j password |
| `MINIO_ENDPOINT` | Optional | S3-compatible endpoint, for example `http://localhost:9000`; enables the landing zone |
| `MINIO_ACCESS_KEY` | With MinIO | MinIO application access key; local development may use the root user |
| `MINIO_SECRET_KEY` | With MinIO | MinIO secret key; do not commit it |
| `MINIO_BUCKET` | With MinIO | Private landing bucket, default `otc-raw` |
| `MINIO_SECURE` | With MinIO | Set to `true` for HTTPS, default `false` |
| `SOURCE_MAPPING_PATH` | No | Path to the declarative source-to-canonical mapping JSON for the current source |
| `CANONICAL_SCHEMA_PATH` | No | JSON Schema used to project and validate canonical output |

All components load configuration from the repository-root `.env` file.
Streamlit and command-line scripts also support the split PostgreSQL settings
`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and `DB_PASSWORD`.
`DATABASE_URL` takes precedence when both formats are present.

For a persistent local setup, copy the root template and add local credentials:

```powershell
Copy-Item .env.example .env
```

Then edit the root `.env`:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=otc_platform
DB_USER=postgres
DB_PASSWORD=your_local_postgres_password
NEO4J_URI=neo4j+s://your-instance.databases.neo4j.io
NEO4J_USERNAME=neo4j
NEO4J_PASSWORD=your_neo4j_password
```

Keep the real values local; `.env` is ignored by Git and must never be
committed.

PowerShell example:

```powershell
$env:DATABASE_URL = "postgresql://postgres:password@localhost:5432/otc"
$env:NEO4J_URI = "neo4j+s://your-instance.databases.neo4j.io"
$env:NEO4J_USERNAME = "neo4j"
$env:NEO4J_PASSWORD = "your_neo4j_password"
```

The Streamlit sidebar also has fields for the API URL and token of whichever
REST source you are ingesting for the current run; those values are not
written to the repository or database.

Replace `user`, `password`, `otc`, and the host with the credentials and
database that actually exist on your PostgreSQL server. The values in the
example are placeholders and will produce a password authentication error if
used literally.

If you previously exported an invalid `DATABASE_URL`, clear it before using
the split settings from `.env`:

```powershell
Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue
```

Shell environment variables take precedence, so a shell value is never
replaced by the root `.env`.

Do not commit tokens, passwords, or `.env` files. The repository ignores
`.env` files and Python virtual environments.

## Database setup

Run the schema scripts in dependency order:

```powershell
psql $env:DATABASE_URL -f database\raw.sql
psql $env:DATABASE_URL -f database\staging.sql
psql $env:DATABASE_URL -f database\canonical.sql
```

The database contains three schemas:

- **`raw`**: `raw.api_records` stores source entity, source record ID,
  extraction time, batch ID, complete JSON payload, status, and errors.
- **`staging`**: `staging.preprocessed_records` links to the raw row and
  stores the source payload, enriched attributes, and field provenance.
- **`canonical`**: `canonical.otc_records` links to staging and stores the
  normalized `otc.v1` JSON document.

The SQL scripts use `CREATE SCHEMA IF NOT EXISTS` and
`CREATE TABLE IF NOT EXISTS`, so they can be safely rerun.

## Running the Streamlit workflow

Start the control panel from the repository root:

```powershell
streamlit run app\streamlit_app.py
```

In the browser sidebar, under **Custom REST API**:

1. Enter the **API URL** of the source you want to ingest.
2. Enter the **API token** (or leave blank for an unauthenticated source).
3. Set **Source system**, **Source entity**, **Record ID field**, and
   optionally a **Records path** if the response wraps its array.
4. Paste your **Source-to-canonical mapping JSON** (see
   [Add a new REST API source](#add-a-new-rest-api-source) below).
5. Paste your **Canonical JSON Schema**.
6. Click **Map API -> Postgres -> Neo4j**.

That button runs the full default pipeline as one Prefect flow
(`otc_pipeline_flow` in [`orchestration/flows.py`](orchestration/flows.py)):
ingest to MinIO, convert to Parquet, hydrate and preprocess in Postgres,
normalize to the canonical envelope, then upsert into Neo4j as generic nodes
and relationships. The graph view below the sidebar refreshes from Neo4j
after the run completes.

The UI reports missing configuration and upstream errors rather than
silently treating a run as successful.

## Running the stages from the command line

The same stages can be run without Streamlit:

```powershell
python -m ingestion.generic_rest --config path\to\source_config.json
python -m transformation.preprocess
python -m transformation.normalize
python -m upsert_merge
```

Or run the whole flow at once through Prefect:

```powershell
python -m orchestration.flows
```

Each transformation command reads `DATABASE_URL`, writes to PostgreSQL, and
prints the number of records written. Preprocessing applies the mapping at
`SOURCE_MAPPING_PATH` when one is configured; otherwise it falls back to the
legacy demo enrichment described in
[Preprocessing and enrichment](#preprocessing-and-enrichment).

### Ingest the ERP-shaped API

Start the local source API:

```powershell
uvicorn source_api.app.main:app --port 8000
```

Then, from another terminal with `DATABASE_URL` configured:

```powershell
python -m ingestion.runner
python -m transformation.preprocess
python -m transformation.normalize
```

The ERP path maps `shipment_id`, `order_id`, `tracking_number`, `carrier`,
`warehouse_id`, dates, priority, status, currency, and `updated_at` into the
same canonical contract while retaining the source system in metadata.

### Add a new REST API source

Clone this repository, copy `.env.example` to `.env`, and
replace the API settings:

```powershell
Copy-Item .env.example .env
```

Configure `API_URL`, `API_TOKEN`, `SOURCE_SYSTEM`, `SOURCE_ENTITY`, and
`API_ID_FIELD`. If the response is wrapped, set `API_RECORDS_PATH`, such as
`data.items`. The connector supports bearer tokens and custom token headers.

Write a mapping JSON (referenced by `SOURCE_MAPPING_PATH`, or pasted directly
into the Streamlit sidebar) that declares your entity shape. Nothing in the
pipeline assumes orders or shipments; the mapping alone decides what gets
ingested:

```json
{
  "source": { "entity_id": "id" },
  "entity_type": "Invoice",
  "canonical": {
    "amount_due": "totals.due",
    "customer_id": "customer.id"
  },
  "relationships": [
    { "type": "BILLED_TO", "from": "entity_id", "to": "customer_id" }
  ]
}
```

- `source.entity_id`: dotted path to the record's unique identifier.
- `entity_type`: the Neo4j node label (`entity.type` in the canonical
  envelope); defaults to `SOURCE_ENTITY` if omitted.
- `canonical`: `{attribute key: dotted source path}` -> becomes
  `entity.attributes`.
- `relationships` (optional): declarative edges referencing keys already
  produced under `canonical`, or the literal `"entity_id"`. A relationship
  only materializes in Neo4j once **both** endpoints have themselves been
  ingested as their own entities with matching IDs.

Run:

```powershell
python -m ingestion.generic_rest
python -m transformation.preprocess
python -m transformation.normalize
python -m upsert_merge
```

The generic connector writes every API object unchanged to `raw.api_records`
(via MinIO and Parquet when configured) with a batch ID and source record ID.
Preprocessing applies your mapping; normalization validates the result
against your canonical JSON Schema; the upsert step writes one Neo4j node per
record under the label from `entity_type`, plus any declared relationships.

### Runtime canonical schema

Set `CANONICAL_SCHEMA_PATH` to the supplied JSON Schema. During normalization,
the pipeline projects each normalized record to the schema's declared
properties and validates the result. Adding or removing canonical properties
therefore changes the published shape through the schema, without changing
`normalize.py`. Required fields and types are enforced; invalid records fail
normalization instead of being published as valid canonical data. See
`canonical.schema.example.json` for the expected format.

## Transformation and data lineage

### Ingestion

`ingestion/generic_rest.py` (the default connector, used for any REST source):

- Generates a new UUID batch per run.
- Lands the raw response in the MinIO `otc-raw` bucket when `MINIO_ENDPOINT`
  is configured.
- Uses `ingestion/raw_parquet.py` to convert the landing batch to Parquet and
  hydrate `raw.api_records` in Postgres from that Parquet file.
- Falls back to writing `raw.api_records` directly when MinIO is not
  configured.

### Preprocessing and enrichment

`transformation/preprocess.py` selects successful raw records that have not
already been staged, and stores this structure:

```json
{
  "source_payload": {},
  "enriched_attributes": {},
  "field_provenance": {}
}
```

When `SOURCE_MAPPING_PATH` (or the Streamlit mapping field) is configured,
`enriched_attributes` comes from `transformation/generic_mapper.py`:
entirely declarative, producing `entity_id`, `entity_type`, `attributes`, and
`relationships` from your mapping config. No entity shape is assumed here;
the mapping alone decides what the record becomes.

When no mapping is configured, preprocessing falls back to
`transformation/enrichment.py`, a legacy demo enrichment path that derives
shipment-shaped attributes (customer tier, order value, tracking number,
facility, carrier, route, event) with deterministic synthetic fallbacks. This
fallback exists only so the pipeline is runnable out of the box; real usage
should supply a mapping.

### Normalization

`transformation/normalize.py`:

1. Trims string values recursively.
2. Builds the canonical envelope: `schema_version`, `metadata`,
   `entity.type`/`entity.id`/`entity.attributes`, top-level `relationships`,
   and `derived` fields. When a mapping was used, `entity.type` and
   `relationships` come directly from the mapping's output; the legacy
   fallback path uses a fixed `shipment` entity type and a single `SHIPS_TO`
   relationship instead.
3. Removes the lowest and highest 1% for each numeric field when a batch is
   large enough to have a non-zero 1% trim count.
4. Validates the result against `CANONICAL_SCHEMA_PATH` when configured.
5. Inserts only records not already present in the canonical table.

## Provenance contract

[`transformation/field_provenance.yaml`](transformation/field_provenance.yaml)
documents field origins for the legacy demo enrichment path only (used when
no mapping is configured). It classifies fields as:

- `API-SOURCED`: copied unchanged from the source response;
- `DERIVED`: calculated from source or other pipeline fields;
- `SYNTHETIC`: generated for this demonstration and not claimed to be
  source-provided.

When you supply your own mapping, provenance is whatever your `canonical`
mapping declares as sourced from the payload; this file does not apply.

## Resetting data

There is no built-in "clear and reingest" control in the current pipeline;
runs are idempotent per `batch_id` instead. To reset manually:

```sql
DELETE FROM canonical.otc_records;
DELETE FROM staging.preprocessed_records;
DELETE FROM raw.api_records;
```

Do not run this against a shared or production database.

## Troubleshooting

- **`Set DATABASE_URL before ...`**: define `DATABASE_URL` in the same shell
  used to start Streamlit or the CLI.
- **Missing Neo4j env vars**: define `NEO4J_URI`, `NEO4J_USERNAME`, and
  `NEO4J_PASSWORD`.
- **MinIO connection errors**: confirm the MinIO container/service is running
  and `MINIO_ENDPOINT`/`MINIO_ACCESS_KEY`/`MINIO_SECRET_KEY` are correct.
- **Canonical schema validation failed**: your mapping's `canonical` output
  does not match the `required`/`properties` in the JSON Schema you supplied;
  update one to match the other.
- **Relationship missing in the graph**: both endpoints of a declared
  relationship must exist as their own ingested entities with matching IDs;
  check `relationships_skipped_missing_endpoint` in the upsert result.
- **No staging rows appear**: confirm raw rows have
  `ingestion_status = 'SUCCESS'` and that the three schema scripts ran.
- **No canonical rows appear**: normalize only after preprocessing.
- **PostgreSQL connection errors**: verify that PostgreSQL is running and that
  the connection string points to the correct database and credentials.

## Development notes

- The pipeline is intended for test/demo data; synthetic fields must not be
  presented as source-system facts.
- Raw payloads are preserved so transformations can be audited and rerun.
- The current canonical storage is JSONB inside PostgreSQL, allowing the
  contract to remain stable while source payloads evolve.
- Keep schema changes, enrichment rules, and provenance definitions in sync.