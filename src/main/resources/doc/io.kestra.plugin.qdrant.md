# Qdrant Plugin

Use the Qdrant plugin to manage vector collections, upsert embeddings, retrieve points, query by vector similarity, and delete points in [Qdrant](https://qdrant.tech/), an open-source vector similarity search engine.

## Connection

All tasks inherit common connection properties:
- `host`: The hostname or IP address of the Qdrant gRPC endpoint (required).
- `port`: The gRPC port (default: `6334`).
- `apiKey`: The API key for authentication with Qdrant Cloud or secure self-hosted instances (optional, secret).
- `tlsEnabled`: Whether to connect using TLS/SSL encryption (default: `false`).

## Tasks

- `CreateCollection`: Creates a new collection configured with vector dimension size (`vectorSize`), distance metric (`COSINE`, `DOT`, `EUCLID`, or `MANHATTAN`), and optional on-disk payload storage. By default the dense vector is unnamed. Set `vectorName` to name it and `sparseVectors` to add named sparse vector spaces (with an optional `IDF` modifier) for hybrid retrieval. (Note: Collections with several named dense vectors should be provisioned via the Qdrant API or CLI).
- `DeleteCollection`: Drops a collection and removes all stored vectors and payloads.
- `CollectionInfo`: Retrieves point and vector statistics, distance metric and `payloadSchema`. Each payload index reports `dataType` and `points` (null if Qdrant does not report a count).
- `CreatePayloadIndex`: Creates a payload index using `collectionName`, `fieldName` and `fieldSchema` (`KEYWORD`, `INTEGER`, `FLOAT`, `GEO`, `TEXT`, `BOOL`, `DATETIME` or `UUID`). `wait` defaults to true; false returns an acknowledged operation before the index is necessarily ready. The task `timeout` bounds long index builds. Check the returned operation `status` and `CollectionInfo.payloadSchema` before using an asynchronous index.
- `Upsert`: Upserts points with vector embeddings (single dense vector, or a named vectors map whose entries are dense lists or sparse `indices` and `values` maps) and metadata payloads. Supports streaming batch processing from inline lists or Kestra storage URIs (`kestra://`).
- `Get`: Retrieves points by their IDs with options to include payloads and vector embeddings. Outputs results inline (`FETCH`), as a single record (`FETCH_ONE`), or to Kestra internal storage (`STORE`).
- `Query`: Performs vector similarity searches using either a raw embedding vector (`vector`) or an existing point ID (`vectorId`). Supports target vector space (`vectorName`) for collections using named vectors, result count limits (`limit`, with `topK` supported as an alias; `limit` takes precedence), score thresholds, payload filters, and fetch output modes (`FETCH`, `FETCH_ONE`, `STORE`).
- `HybridQuery`: Queries one named dense vector space and one named sparse vector space in a single request and lets Qdrant fuse both candidate lists with Reciprocal Rank Fusion. See [Hybrid retrieval](#hybrid-retrieval).
- `Delete`: Deletes points by specific point IDs or matching filter criteria (mutually exclusive).

## Hybrid retrieval

`HybridQuery` sends one `QueryPoints` request containing two prefetch branches, one per vector space, and a fusion query. Qdrant ranks each branch, then merges the two ranked lists with native Reciprocal Rank Fusion and returns the fused top results. Scoring and fusion happen inside Qdrant, so the returned `score` is the fusion score, not a cosine or dot product similarity, and it is not comparable with a `Query` score.

### Requirements

- Qdrant server 1.10 or later, which is where the `QueryPoints` prefetch and fusion API landed. The plugin is built against the Qdrant Java client 1.19.0 and is tested against Qdrant 1.19.1.
- The collection must declare both spaces: a named dense vector in its `vectors` config and a named sparse vector in its `sparse_vectors` config. `CreateCollection` can create that layout with `vectorName` and `sparseVectors` (a sparse space can use the `IDF` modifier for BM25-like weighting), and `Upsert` loads points whose named `vectors` map holds a dense list and a sparse `{indices, values}` map. `HybridQuery` itself only queries: it neither creates collections nor writes points. Point `denseVectorName` and `sparseVectorName` at the configured names. An unknown name fails the run with the server error `Not existing vector name error`.

### Query vectors

- `denseVector` is the dense query embedding. Its dimension must match the dense vector space, otherwise Qdrant answers with `Vector dimension error`.
- `sparseIndices` and `sparseValues` describe the sparse query vector as two positionally paired lists. Both are rendered at runtime and validated before any network call: they must be non-empty and of equal length, indices must be distinct non-negative whole numbers, and all weights must be finite. Each violation raises an `IllegalArgumentException` naming the property.

### Limits

- `limit` bounds the fused result (default `10`, from `1` to `1000`).
- `candidateLimit` bounds how many candidates each branch hands to fusion (default `50`, from `1` to `10000`). `denseCandidateLimit` and `sparseCandidateLimit` override it per branch. Every effective candidate limit must be greater than or equal to `limit`, so fusion always has enough candidates to fill the requested result. Raising the candidate limits gives fusion a wider view at the cost of more work per branch.
- `queryTimeout` defaults to `PT30S`, is truncated to whole seconds (minimum 1 second) and bounds both the server-side request and the gRPC call deadline, so a stalled server or network cannot hold a worker thread indefinitely. Raise it for very large candidate limits. The per-run gRPC client is closed when the task ends.

### Filters

`filter` accepts the same shapes as `Query` and is applied to both prefetch branches and to the fused query, so a point excluded by the filter can enter neither candidate list. Because the filter changes each branch ranking, filtered fusion scores differ from unfiltered ones for the same points. An empty result is a valid outcome in every fetch mode.

### Scope

Only RRF is exposed. Distribution-Based Score Fusion and RRF parameter tuning are not part of this task. Hybrid retrieval is opt-in and `Query` is unchanged: fusion does not improve every workload, so measure dense-only against fused retrieval on your own labelled queries before adopting it.

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

Tasks supporting payload filters (`Query`, `HybridQuery`, `Delete`) accept structured filter maps for a core subset of Qdrant filter conditions. Unsupported filter shapes (e.g. geo coordinates, datetime ranges, nested object paths) fail fast with an `IllegalArgumentException` to prevent silent misqueries:

- **Logical Clauses**: Combine conditions using `must` (AND), `should` (OR), or `must_not` (NOT).
- **Keyword & Value Match**: Field equality matches (e.g. `category: "books"`, `active: true`).
- **Any & Except (IN / NOT IN)**: Multi-value matches using `match: { any: [...] }` or `match: { except: [...] }`.
- **Full-Text Match**: Match full-text payloads using `text: "search query"`.
- **Range Queries**: Numeric ranges using `range: { gte: 10, lte: 100 }` (or `gt`, `lt`).
- **Null & Empty Checks**: Check for null or empty values via `is_null: true` or `is_empty: true`.
- **ID Matching**: Match specific point IDs via `has_id: [1, 2, "uuid"]` or `id: 1`.
