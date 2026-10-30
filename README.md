<p align="center">
  <a href="https://www.kestra.io">
    <img src="https://kestra.io/banner.png"  alt="Kestra workflow orchestrator" />
  </a>
</p>

<h1 align="center" style="border-bottom: none">
    Event-Driven Declarative Orchestrator
</h1>

<div align="center">
 <a href="https://github.com/kestra-io/kestra/releases"><img src="https://img.shields.io/github/tag-pre/kestra-io/kestra.svg?color=blueviolet" alt="Last Version" /></a>
  <a href="https://github.com/kestra-io/kestra/blob/develop/LICENSE"><img src="https://img.shields.io/github/license/kestra-io/kestra?color=blueviolet" alt="License" /></a>
  <a href="https://github.com/kestra-io/kestra/stargazers"><img src="https://img.shields.io/github/stars/kestra-io/kestra?color=blueviolet&logo=github" alt="Github star" /></a> <br>
<a href="https://kestra.io"><img src="https://img.shields.io/badge/Website-kestra.io-192A4E?color=blueviolet" alt="Kestra infinitely scalable orchestration and scheduling platform"></a>
<a href="https://kestra.io/slack"><img src="https://img.shields.io/badge/Slack-Join%20Community-blueviolet?logo=slack" alt="Slack"></a>
</div>

<br />

# Kestra Qdrant Plugin

## Why

Teams building AI and retrieval-augmented generation (RAG) pipelines require scalable, real-time vector search capabilities. This plugin enables declarative orchestration of [Qdrant](https://qdrant.tech/) collections, batch vector embeddings ingestion, semantic search queries, and lifecycle operations natively within Kestra workflows.

## What

Provides tasks under `io.kestra.plugin.qdrant`:
- `CreateCollection`: Create vector collections with specified dimensions and distance metrics (`COSINE`, `DOT`, `EUCLID`, `MANHATTAN`), optionally with a named dense vector and named sparse vector spaces (with an `IDF` modifier) for hybrid retrieval.
- `DeleteCollection`: Remove collections and their associated vectors.
- `CollectionInfo`: Retrieve metrics and payload indexes for a collection.
- `CreatePayloadIndex`: Index a payload field with an explicit data type and optional completion wait.
- `Upsert`: Batch upsert points with embeddings (dense, named and sparse) and payload metadata from inline lists or Kestra storage URIs (`kestra://`).
- `Get`: Retrieve points by ID with `FETCH`, `FETCH_ONE`, or `STORE` fetch modes.
- `Query`: Vector similarity search supporting raw vectors or point IDs, filters, score thresholds, and multiple fetch modes.
- `SparseQuery`: Sparse-only retrieval over a named sparse vector space, the sparse counterpart of `Query`, for comparing dense-only, sparse-only and fused retrieval on labelled queries.
- `HybridQuery`: Hybrid retrieval over a named dense vector space and a named sparse vector space, bounded per-branch candidate limits, pagination, and server-side Reciprocal Rank Fusion (with `k` and branch weights) or Distribution-Based Score Fusion.
- `Delete`: Delete points by ID or matching filter criteria.

## Documentation
* Full documentation: [kestra.io/docs](https://kestra.io/docs)
* Plugin Developer Guide: [kestra.io/docs/plugin-developer-guide/](https://kestra.io/docs/plugin-developer-guide/)

## License
Apache 2.0 (c) [Kestra Technologies](https://kestra.io)
