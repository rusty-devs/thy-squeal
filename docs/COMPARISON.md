# Comparison with Other Database Engines

This document compares **thy-squeal** with other popular database engines to help you understand its positioning, strengths, and trade-offs.

## Overview

thy-squeal is a **hybrid in-memory database** that combines:
1.  **Relational SQL** (similar to SQLite/MySQL/PostgreSQL)
2.  **Full-Text Search** (similar to Elasticsearch/Tantivy)
3.  **Key-Value Operations** (similar to Redis)

| Feature | thy-squeal | SQLite | Redis | MySQL | PostgreSQL | Elasticsearch |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Primary Model** | Relational (SQL) | Relational (SQL) | Key-Value | Relational | Relational | Document (Search) |
| **Storage** | In-Mem (+ Sled) | Disk (B-Tree) | In-Mem (+ RDB) | Disk (B+Tree) | Disk (B-Tree) | Disk (Lucene) |
| **Language** | Rust | C | C | C++ | C | Java |
| **Full-Text Search** | Native (Tantivy) | FTS extension | RediSearch | Basic (MyISAM/Inno) | Advanced (GIN/GiST) | Native / Core |
| **Protocols** | HTTP + SQL (pl) | C API | RESP | MySQL Binary | Postgres Binary | HTTP JSON |
| **Joins** | Inner/Left | Full Support | No | Full Support | Full Support | Limited |
| **ACID** | Planned | Full Support | Limited | Full Support | Full Support | No |

---

## thy-squeal vs. MySQL

**When to choose thy-squeal:**
- **Developer Experience**: You want a "battery-included" server that starts instantly and provides a native HTTP JSON API.
- **Hybrid Needs**: You need to perform full-text searches and relational joins in the same engine without setting up external search plugins.
- **Resource Constraints**: You need a lightweight binary (~20MB) rather than a heavy RDBMS installation.

**When to choose MySQL:**
- **Ecosystem**: You need compatibility with thousands of existing tools, ORMs, and drivers.
- **Scaling**: You require proven replication, clustering, and high-availability features.
- **Complex DDL**: You need advanced schema management like triggers, stored procedures, or views.

---

## thy-squeal vs. PostgreSQL

**When to choose thy-squeal:**
- **Speed**: You prioritize raw in-memory execution speed for read-heavy workloads.
- **Simplicity**: You prefer a simplified SQL dialect that is easy to learn and embed.
- **Rust Integration**: You want a database built in a memory-safe language that can be compiled directly into your Rust binary.

**When to choose PostgreSQL:**
- **Extensibility**: You need PostGIS for GIS data, custom types, or advanced indexing (GIN/BRIN).
- **Data Integrity**: You require strict compliance with complex relational constraints and window functions.
- **Proven Reliability**: You are building a mission-critical system where data loss is not an option and requires battle-tested WAL and recovery.

---

## thy-squeal vs. SQLite

**When to choose thy-squeal:**
- You need an **HTTP API** out of the box for remote access.
- You require **Full-Text Search** as a first-class citizen integrated into your SQL workflow.
- High-speed in-memory performance is a priority over massive disk-based datasets.

**When to choose SQLite:**
- You need a single-file database for local application state.
- You require deep, battle-tested SQL compatibility (CTEs, etc.).

---

## thy-squeal vs. Redis

**When to choose thy-squeal:**
- You need **Relational Querying (Joins, Aggregations)** which Redis doesn't handle natively.
- You prefer **SQL** over the Redis command set for complex data manipulation.

**When to choose Redis:**
- You need ultra-low latency sub-millisecond responses for simple key lookups.
- You require complex data structures like Sorted Sets or Streams.

---

## thy-squeal vs. Elasticsearch

**When to choose thy-squeal:**
- You want a **lightweight** engine (Elasticsearch requires significant JVM resources).
- You need to perform **SQL Joins** between your searchable data and relational tables.

**When to choose Elasticsearch:**
- You are dealing with **Terabytes** of log data or documents.
- You need advanced distributed search features (sharding, geo-queries).

---

## Key Differentiators

1.  **Memory Safety**: Built in 100% safe Rust, preventing entire classes of memory-related bugs common in C/C++ based engines.
2.  **Hybrid Core**: Unlike other engines that "bolt on" search or SQL, thy-squeal's AST and Executor are designed from the ground up to support both relational and search queries in a single execution pipeline.
3.  **Embeddability**: While it runs as a standalone server, the modular architecture allows the `sql` and `storage` crates to be used as a library within other Rust projects.
