# User-Raised Problems and Fixes

This document records the specific problems raised during the project
discussion and what was changed or decided in response.

## 1. Shippo shipment ID was blank

### Problem raised

The Shippo shipment ID was being ingested, but it appeared blank in the
canonical shipment attributes.

### Cause

Shippo returns the shipment identifier as `object_id`. The pipeline stored it as
`source_record_id` and used it as the canonical entity ID, but did not expose it
as the explicit `shipment.shipment_id` attribute shown by the application.

### Fix

Updated `transformation/enrichment.py`:

```python
"shipment": {
    "shipment_id": object_id,
    ...
}
```

Updated `transformation/field_provenance.yaml` to document:

```text
shipment.shipment_id <- payload.object_id
```

The mapping was verified with a transformation test.

## 2. A second API was needed for testing

### Problem raised

The pipeline needed to ingest a second API that was not Shippo so the multi-source
design could be tested.

### What was tried

The repository already had a local synthetic Orders API. It was used initially
as a second schema to prove that a non-Shippo source could enter the same raw,
staging, and canonical flow.

An Odoo Community connector was also added using Odoo XML-RPC. It reads
`stock.picking` and joins matching `sale.order` records.

### Result

The pipeline can now support:

```text
Shippo -> raw -> staging -> canonical
Odoo   -> raw -> staging -> canonical
```

Odoo fields such as picking name, origin, tracking reference, carrier,
warehouse, state, dates, amount, currency, and update time were mapped into the
existing test contract.

## 3. Odoo required too much setup

### Problem raised

Running Docker, PostgreSQL, and a local Odoo instance was more complicated than
simply configuring a public API endpoint and token.

### Fix and decision

The generic REST connector was added so a user can clone the repository, copy an
environment template, provide an endpoint and token, and ingest without
installing Odoo.

Files added:

- `ingestion/generic_rest.py`
- `.env.example`

Supported configuration includes:

```dotenv
API_URL=https://api.example.com/v1/resources
API_TOKEN=your-token
API_TOKEN_HEADER=Authorization
API_TOKEN_PREFIX=Bearer
SOURCE_SYSTEM=example_api
SOURCE_ENTITY=resources
API_ID_FIELD=id
API_RECORDS_PATH=
```

## 4. The repository needed to be reusable by another team

### Problem raised

The pipeline should be pushable to Git, clonable by another team, and usable
after they provide their own API.

### Fix

The generic REST connector now:

- reads endpoint configuration from environment variables
- supports bearer or custom token headers
- supports configurable record ID fields
- supports wrapped API responses such as `data.items`
- assigns a batch ID
- preserves each raw API object
- writes source metadata and source record IDs to PostgreSQL
- rolls back failed database writes
- rejects malformed responses and records without IDs

The documented execution flow is:

```powershell
Copy-Item .env.example .env
python -m ingestion.generic_rest
python -m transformation.preprocess
python -m transformation.normalize
```

## 5. The canonical map is provided externally

### Problem raised

The canonical map will be supplied separately and is not the concern of this
repository.

### Fix and design boundary

The repository was documented as a transport and persistence layer. It does not
infer business meaning or invent canonical mappings.

Its responsibility is:

```text
authenticate
-> fetch
-> validate transport response
-> assign batch metadata
-> preserve raw payload
-> preserve source identifiers
-> hand off data for the supplied canonical map
```

The supplied mapping can consume the raw and staging data without requiring the
generic connector to know the business semantics of every API.

## 6. Nested values needed clarification

### Problem raised

It was unclear how `normalize.py` handles nested values.

### Answer and current behavior

`clean_values()` recursively walks dictionaries and lists, trimming strings while
preserving the nested structure.

Example:

```text
{"order": {"status": " HIGH "}}
```

becomes:

```text
{"order": {"status": "HIGH"}}
```

Nested canonical objects remain nested JSON. They are not flattened in the
database.

`numeric_values()` recursively finds numeric values inside dictionaries and
records dotted paths such as:

```text
order.value
order.metrics.risk
```

## 7. Comma-delimited values needed clarification

### Problem raised

It was unclear how a value such as `Doe,John` would be handled.

### Answer and design decision

The normalizer treats `Doe,John` as an ordinary string. It trims surrounding
whitespace but does not split the value into first and last names.

This avoids unsafe assumptions because commas can represent names, addresses,
company names, or other values.

If the supplied canonical map requires parsing, it must explicitly define a
transformation such as:

```text
parse_last_first_name
```

No automatic name inference was added.

## 8. Open-source/public API expectations changed

### Problem raised

The desired second source shifted between:

1. a public commercial API with a token,
2. an open-source commercial system,
3. and finally a reusable API ingestion template.

### Resolution

Odoo was identified as an open-source ERP option, but it requires a running
instance and credentials. A hosted commercial shipping API such as EasyPost
would be simpler but is not open-source.

The reusable generic REST connector became the practical solution because it
supports either:

- a public API with a generated token, or
- an internally hosted/open-source API,

without changing the ingestion pipeline.

## 9. What remains intentionally external

The following are not automatically solved by the generic connector:

- the business-specific canonical mapping
- source-specific field transformations
- knowledge-graph entity and relationship rules
- API-specific pagination beyond the current single-response template
- production orchestration and scheduling

These were kept outside the generic transport layer so the repository can be
reused without embedding assumptions about the final company data model.
