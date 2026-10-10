# Kestra Qdrant Plugin

## What

Provides Kestra plugin tasks under `io.kestra.plugin.qdrant` to interact with [Qdrant](https://qdrant.tech), an open-source vector similarity search engine and vector database.

## Why

Teams building AI-powered workflows and retrieval-augmented generation (RAG) systems need to store, manage, and query dense vector embeddings. This plugin allows Kestra workflows to orchestrate Qdrant operations—creating and deleting collections, ingesting vectors in batches from storage or inline data, performing semantic similarity searches, retrieving points, and deleting points.

## How

### Architecture

Single-module plugin with a flat package layout (no sub-packages):
- `QdrantConnectionInterface` — interface for shared connection properties (`host`, `port`, `apiKey`, `tlsEnabled`).
- `QdrantConnection` — abstract base `Task` implementing connection properties, gRPC client lifecycle (`buildClient`), and conversion helpers between Java/Kestra types and Qdrant protobuf structures.
- All tasks extend `QdrantConnection`.
- SDK: `io.qdrant:client:1.19.0`.

### Key Plugin Classes

| Class | Description |
|---|---|
| `io.kestra.plugin.qdrant.QdrantConnection` | Abstract base with shared connection properties and gRPC client builder |
| `io.kestra.plugin.qdrant.CreateCollection` | Create a collection with vector dimension size and distance metric |
| `io.kestra.plugin.qdrant.DeleteCollection` | Delete a collection and all points contained within it |
| `io.kestra.plugin.qdrant.CollectionInfo` | Retrieve status, vector dimension, and point statistics for a collection |
| `io.kestra.plugin.qdrant.Upsert` | Batch upsert points with embeddings and payloads from inline list or Kestra storage URI |
| `io.kestra.plugin.qdrant.Get` | Retrieve points by ID with `FETCH`, `FETCH_ONE`, or `STORE` fetch modes |
| `io.kestra.plugin.qdrant.Query` | Perform vector similarity search by embedding vector or existing point ID |
| `io.kestra.plugin.qdrant.Delete` | Delete points by ID or matching filter |

### Project Structure

```
plugin-qdrant/
|-- src/main/java/io/kestra/plugin/qdrant/
|   |-- QdrantConnectionInterface.java
|   |-- QdrantConnection.java
|   |-- Distance.java
|   |-- CreateCollection.java
|   |-- DeleteCollection.java
|   |-- CollectionInfo.java
|   |-- Upsert.java
|   |-- Get.java
|   |-- Query.java
|   |-- Delete.java
|   \-- package-info.java
|-- src/main/resources/
|   |-- doc/io.kestra.plugin.qdrant.md
|   |-- icons/plugin-icon.svg
|   |-- icons/io.kestra.plugin.qdrant.svg
|   |-- metadata/index.yaml
|   \-- META-INF/services/io.grpc.LoadBalancerProvider
|-- src/test/java/io/kestra/plugin/qdrant/
|-- build.gradle
|-- README.md
\-- AGENTS.md
```

## References

- https://kestra.io/docs/plugin-developer-guide
- https://qdrant.tech/documentation/
