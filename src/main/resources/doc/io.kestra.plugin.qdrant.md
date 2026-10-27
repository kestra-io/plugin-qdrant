# Qdrant Plugin

Use the Qdrant plugin to manage vector collections, upsert embeddings, retrieve points, query by vector similarity, and delete points in [Qdrant](https://qdrant.tech/), an open-source vector similarity search engine.

## Connection

All tasks inherit common connection properties:
- `host`: The hostname or IP address of the Qdrant gRPC endpoint (required).
- `port`: The gRPC port (default: `6334`).
- `apiKey`: The API key for authentication with Qdrant Cloud or secure self-hosted instances (optional, secret).
- `tlsEnabled`: Whether to connect using TLS/SSL encryption (default: `false`).

## Tasks

- `CreateCollection`: Creates a new single-vector collection configured with vector dimension size (`vectorSize`), distance metric (`COSINE`, `DOT`, `EUCLID`, or `MANHATTAN`), and optional on-disk payload storage. (Note: Collections with multiple named vectors should be provisioned via the Qdrant API or CLI).
- `DeleteCollection`: Drops a collection and removes all stored vectors and payloads.
- `CollectionInfo`: Retrieves point and vector statistics, distance metric and `payloadSchema`. Each payload index reports `dataType` and `points` (null if Qdrant does not report a count).
- `CreatePayloadIndex`: Creates a payload index using `collectionName`, `fieldName` and `fieldSchema` (`KEYWORD`, `INTEGER`, `FLOAT`, `GEO`, `TEXT`, `BOOL`, `DATETIME` or `UUID`). `wait` defaults to true; false returns an acknowledged operation before the index is necessarily ready. The task `timeout` bounds long index builds. Check the returned operation `status` and `CollectionInfo.payloadSchema` before using an asynchronous index.
- `Upsert`: Upserts points with vector embeddings (single dense vector or named vectors map) and metadata payloads. Supports streaming batch processing from inline lists or Kestra storage URIs (`kestra://`).
- `Get`: Retrieves points by their IDs with options to include payloads and vector embeddings. Outputs results inline (`FETCH`), as a single record (`FETCH_ONE`), or to Kestra internal storage (`STORE`).
- `Query`: Performs vector similarity searches using either a raw embedding vector (`vector`) or an existing point ID (`vectorId`). Supports target vector space (`vectorName`) for collections using named vectors, result count limits (`limit`, with `topK` supported as an alias; `limit` takes precedence), score thresholds, payload filters, and fetch output modes (`FETCH`, `FETCH_ONE`, `STORE`).
- `Delete`: Deletes points by specific point IDs or matching filter criteria (mutually exclusive).

## Filter DSL

Collections with strict-mode filtering can reject queries on unindexed fields. Create the required index before querying:

```yaml
id: index_qdrant_payload
namespace: company.team
tasks:
  - id: index
    type: io.kestra.plugin.qdrant.CreatePayloadIndex
    host: "{{ secret('QDRANT_HOST') }}"
    apiKey: "{{ secret('QDRANT_API_KEY') }}"
    tlsEnabled: true
    collectionName: docs
    fieldName: tenant_id
    fieldSchema: KEYWORD
    wait: true
  - id: inspect
    type: io.kestra.plugin.qdrant.CollectionInfo
    host: "{{ secret('QDRANT_HOST') }}"
    apiKey: "{{ secret('QDRANT_API_KEY') }}"
    tlsEnabled: true
    collectionName: docs
```

Tasks supporting payload filters (`Query`, `Delete`) accept structured filter maps for a core subset of Qdrant filter conditions. Unsupported filter shapes (e.g. geo coordinates, datetime ranges, nested object paths) fail fast with an `IllegalArgumentException` to prevent silent misqueries:

- **Logical Clauses**: Combine conditions using `must` (AND), `should` (OR), or `must_not` (NOT).
- **Keyword & Value Match**: Field equality matches (e.g. `category: "books"`, `active: true`).
- **Any & Except (IN / NOT IN)**: Multi-value matches using `match: { any: [...] }` or `match: { except: [...] }`.
- **Full-Text Match**: Match full-text payloads using `text: "search query"`.
- **Range Queries**: Numeric ranges using `range: { gte: 10, lte: 100 }` (or `gt`, `lt`).
- **Null & Empty Checks**: Check for null or empty values via `is_null: true` or `is_empty: true`.
- **ID Matching**: Match specific point IDs via `has_id: [1, 2, "uuid"]` or `id: 1`.
