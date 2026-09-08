# OTC Data Preparation Pipeline

This project is a small, auditable order-to-cash (OTC) data preparation
pipeline. It creates test shipments with the Shippo API, keeps the original
API responses unchanged, enriches those responses with deterministic
application attributes, and publishes a stable `otc.v1` canonical record for
each shipment.

The project is designed to make the boundary between source data, derived
data, and synthetic demonstration data explicit. PostgreSQL is the system of
record for every pipeline layer, and the Streamlit application provides a
control panel for running the pipeline and inspecting its output.

## What the project does

The main workflow is:

```text
Shippo test API
      |
      v
raw.api_records                 Original shipment and transaction JSON
      |
      v
staging.preprocessed_records   Source payload + enrichment + provenance
      |
      v
canonical.otc_records          Normalized otc.v1 shipment contract
      |
      v
Streamlit control panel         Tables and JSON views for both layers
```

Each run is tracked with a UUID `batch_id`. The ingestion layer stores both
successful and failed responses with an ingestion status and optional error
message. Later stages process only successful Shippo shipment records and are
idempotent: records already represented in the next layer are not inserted
again.

## Repository layout

| Path | Purpose |
| --- | --- |
| `app/streamlit_app.py` | Streamlit control panel and pipeline controls |
| `ingestion/shippo_client.py` | Shippo test shipment/transaction ingestion and raw persistence |
| `ingestion/shipment_request.example.json` | Safe example request body sent to Shippo |
| `ingestion/odoo_client.py` | Odoo Community stock-picking ingestion |
| `ingestion/generic_rest.py` | Configurable REST API ingestion template |
| `ingestion/.env.example` | Template configuration for a new API source |
| `ingestion/runner.py` | Legacy local ERP-shaped API ingestion prototype |
| `source_api/app/main.py` | FastAPI synthetic `/orders` source |
| `transformation/preprocess.py` | Builds staging records from raw Shippo shipments |
| `transformation/enrichment.py` | Adds deterministic canonical attributes |
| `transformation/normalize.py` | Cleans, filters, and writes `otc.v1` records |
| `transformation/field_provenance.yaml` | Field-level source and derivation rules |
| `database/raw.sql` | PostgreSQL raw schema |
| `database/staging.sql` | PostgreSQL staging schema |
| `database/canonical.sql` | PostgreSQL canonical schema |

The `source_api` and `ingestion/runner.py` files represent an ERP-shaped
orders path. It is intentionally local and deterministic so the pipeline can
be tested against a second, non-Shippo schema. A live SAP or Oracle connector
can replace its HTTP URL without changing the raw, staging, or canonical
interfaces.

## Prerequisites

- Python 3.11 or a compatible recent Python version
- PostgreSQL with permission to create schemas and tables
- A Shippo test-mode API token
- An Odoo Community instance with XML-RPC access (for the Odoo path)
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
| `SHIPPO_API_TOKEN` | Yes for ingestion | Shippo test-mode token |
| `ODOO_URL` | Yes for Odoo ingestion | Odoo base URL, for example `http://localhost:8069` |
| `ODOO_DB` | Yes for Odoo ingestion | Odoo database name |
| `ODOO_USERNAME` | Yes for Odoo ingestion | Odoo user login |
| `ODOO_PASSWORD` | Yes for Odoo ingestion | Odoo password or API key |
| `ODOO_LIMIT` | No | Maximum pickings and orders per run; default `100` |
| `CANONICAL_SCHEMA_PATH` | No | JSON Schema used to project and validate canonical output |

Streamlit also supports the existing split PostgreSQL settings used by
`ingestion\.env`: `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, and
`DB_PASSWORD`. `DATABASE_URL` takes precedence when both formats are present.

For a persistent local setup, add both the database password and Shippo token
to the ignored `ingestion\.env` file:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=otc_platform
DB_USER=postgres
DB_PASSWORD=your_local_postgres_password
SHIPPO_API_TOKEN=shippo_test_your_token
```

The application loads this file automatically. Keep the real values local;
`ingestion\.env` is ignored by Git and must never be committed.

PowerShell example:

```powershell
$env:DATABASE_URL = "postgresql://postgres:password@localhost:5432/otc"
$env:SHIPPO_API_TOKEN = "shippo_test_..."
```

If `SHIPPO_API_TOKEN` is not configured, the Streamlit sidebar provides a
password field where you can enter a Shippo test token for the current
session. The value is not written to the repository or database. A valid
Shippo test-mode token is still required; the application cannot create one.

Replace `user`, `password`, `otc`, and the host with the credentials and
database that actually exist on your PostgreSQL server. The values in the
example are placeholders and will produce a password authentication error if
used literally.

If you previously exported an invalid `DATABASE_URL`, clear it before using
the split settings from `ingestion\.env`:

```powershell
Remove-Item Env:DATABASE_URL -ErrorAction SilentlyContinue
```

When Streamlit starts, it loads `.env` from the repository root and then
`ingestion\.env` as a compatibility fallback for existing local setups.
Already-exported environment variables take precedence, so a shell value is
never replaced by either file.

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

In the browser:

1. Set the number of test shipments in the sidebar (1-100).
2. Select **Ingest from Shippo**.
3. The application clears the previous raw, staging, and canonical output,
   creates that many test shipments, and stores each shipment response.
4. For each shipment, it selects the first Shippo rate, creates a
   transaction/label request, and stores the transaction response in the raw
   layer.
5. The successful shipment rows are preprocessed into staging.
6. Select **Normalize to otc.v1**.
7. Inspect preprocessed rows in the staging tab and flattened/canonical JSON
   rows in the canonical tab.

The UI reports missing configuration and Shippo or PostgreSQL errors rather
than silently treating a run as successful.

## Running the stages from the command line

The same stages can be run without Streamlit:

```powershell
python -m ingestion.shippo_client ingestion\shipment_request.example.json
python -m transformation.preprocess
python -m transformation.normalize
```

The ingestion module command above submits one request to its default
`shipments/` endpoint. To create a selected number of shipments and their
transactions, use the Python API used by the UI or run the UI itself.

Each transformation command reads `DATABASE_URL`, writes to PostgreSQL, and
prints the number of records written. The preprocessing stage reads
`transformation/field_provenance.yaml` on every run, so provenance changes are
captured in the staging payload.

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

Clone this repository, copy `ingestion\.env.example` to `ingestion\.env`, and
replace the API settings:

```powershell
Copy-Item ingestion\.env.example ingestion\.env
```

Configure `API_URL`, `API_TOKEN`, `SOURCE_SYSTEM`, `SOURCE_ENTITY`, and
`API_ID_FIELD`. If the response is wrapped, set `API_RECORDS_PATH`, such as
`data.items`. The connector supports bearer tokens and custom token headers.

Run:

```powershell
python -m ingestion.generic_rest
python -m transformation.preprocess
python -m transformation.normalize
```

The generic connector writes every API object unchanged to `raw.api_records`
with a batch ID and source record ID. The canonical mapping is an external
input to this repository: provide the mapping implementation or generated
mapping artifact at the staging-to-canonical boundary. This repository does
not infer business semantics or invent mappings; it guarantees that the
source payload, identifiers, batch metadata, and extraction metadata are
available for the supplied canonical map.

### Runtime canonical schema

Set `CANONICAL_SCHEMA_PATH` to the supplied JSON Schema. During normalization,
the pipeline projects each normalized record to the schema's declared
properties and validates the result. Adding or removing canonical properties
therefore changes the published shape through the schema, without changing
`normalize.py`. Required fields and types are enforced; invalid records fail
normalization instead of being published as valid canonical data. See
`canonical.schema.example.json` for the expected format.

### Ingest Odoo Community

Configure the Odoo instance in the ignored environment file:

```dotenv
ODOO_URL=http://localhost:8069
ODOO_DB=odoo
ODOO_USERNAME=admin@example.com
ODOO_PASSWORD=your_odoo_password_or_api_key
ODOO_LIMIT=100
```

Then run:

```powershell
python -m ingestion.odoo_client
python -m transformation.preprocess
python -m transformation.normalize
```

The connector reads Odoo `stock.picking` records and joins matching
`sale.order` records. It maps the Odoo picking name to `shipment_id`, the
origin to `order_id`, carrier tracking reference, carrier, customer, warehouse,
status, scheduled dates, order value, currency, and source update timestamp.

## Transformation and data lineage

### Ingestion

`ingestion/shippo_client.py`:

- Generates a new UUID batch for each stored response.
- Sends the example shipment payload to `https://api.goshippo.com`.
- Uses the `SHIPPO_API_TOKEN` in the `ShippoToken` authorization header.
- Stores the complete response before raising an HTTP error.
- Stores shipment responses under `source_entity = shipments` and transaction
  responses under `source_entity = transactions`.
- Uses the first returned rate to create a synchronous transaction request.

### Preprocessing and enrichment

`transformation/preprocess.py` selects successful Shippo shipments that have
not already been staged. It joins a matching transaction response by shipment
ID, adds a deterministic fallback tracking number (`shp_000001` style), and
stores this structure:

```json
{
  "source_payload": {},
  "enriched_attributes": {},
  "field_provenance": {}
}
```

`transformation/enrichment.py` derives or supplies the attributes used by the
canonical contract, including:

- customer tier, order value, priority, and SLA state;
- tracking number, dwell time, and arrival ETA;
- facility identity, region, coordinates, throughput, and status;
- carrier and delivery-partner metrics;
- route IDs, route name, and risk score;
- an initial shipment event and SHA-256 audit hash.

Values that come from Shippo are kept distinct from synthetic benchmark
values. When Shippo does not provide origin coordinates or a tracking number,
the implementation uses deterministic fallback values.

### Normalization

`transformation/normalize.py`:

1. Trims string values recursively.
2. Builds the fixed `otc.v1` envelope with metadata, entity attributes,
   relationships, and derived fields.
3. Removes the lowest and highest 1% for each numeric field when a batch is
   large enough to have a non-zero 1% trim count.
4. Inserts only records not already present in the canonical table.

The canonical entity is a shipment and includes a `SHIPS_TO` relationship from
the origin address ID to the destination address ID.

## Provenance contract

[`transformation/field_provenance.yaml`](transformation/field_provenance.yaml)
classifies fields as:

- `API-SOURCED`: copied from a Shippo response;
- `DERIVED`: calculated from source or other pipeline fields;
- `SYNTHETIC`: generated for this demonstration and not claimed to be
  Shippo-provided.

Examples include Shippo rate provider and zone fields as API-sourced,
estimated delivery as derived, and reliability or capacity benchmarks as
synthetic. Update this file whenever enrichment rules or field origins change.

## Resetting data

The Streamlit **Ingest from Shippo** action calls
`reset_pipeline_data()` first and deletes canonical, staging, and raw rows.
This is intentional for a clean demonstration batch. Do not use that control
against a shared or production database.

To reset manually:

```sql
DELETE FROM canonical.otc_records;
DELETE FROM staging.preprocessed_records;
DELETE FROM raw.api_records;
```

## Troubleshooting

- **`Set DATABASE_URL before ...`**: define `DATABASE_URL` in the same shell
  used to start Streamlit or the CLI.
- **`Set SHIPPO_API_TOKEN before ingesting.`**: define a valid Shippo test-mode
  token. Never use a production token for this test workflow.
- **Shippo rejects the request**: verify the token, test-mode account, request
  addresses/parcels, and returned rates.
- **No staging rows appear**: confirm raw shipment rows have
  `ingestion_status = 'SUCCESS'` and that the three schema scripts ran.
- **No canonical rows appear**: normalize only after preprocessing and use the
  **Normalize to otc.v1** control.
- **PostgreSQL connection errors**: verify that PostgreSQL is running and that
  the connection string points to the correct database and credentials.

## Development notes

- The pipeline is intended for test/demo data; synthetic fields must not be
  presented as source-system facts.
- Raw payloads are preserved so transformations can be audited and rerun.
- The current canonical storage is JSONB inside PostgreSQL, allowing the
  contract to remain stable while source payloads evolve.
- Keep schema changes, enrichment rules, and provenance definitions in sync.