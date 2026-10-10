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
- `CreateCollection`: Create vector collections with specified dimensions and distance metrics (`COSINE`, `DOT`, `EUCLID`, `MANHATTAN`).
- `DeleteCollection`: Remove collections and their associated vectors.
- `CollectionInfo`: Retrieve metrics and status for a collection.
- `Upsert`: Batch upsert points with embeddings and payload metadata from inline lists or Kestra storage URIs (`kestra://`).
- `Get`: Retrieve points by ID with `FETCH`, `FETCH_ONE`, or `STORE` fetch modes.
- `Query`: Vector similarity search supporting raw vectors or point IDs, filters, score thresholds, and multiple fetch modes.
- `Delete`: Delete points by ID or matching filter criteria.

## Documentation
* Full documentation: [kestra.io/docs](https://kestra.io/docs)
* Plugin Developer Guide: [kestra.io/docs/plugin-developer-guide/](https://kestra.io/docs/plugin-developer-guide/)

## License
Apache 2.0 (c) [Kestra Technologies](https://kestra.io)
