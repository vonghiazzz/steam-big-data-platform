# Architecture Decision Records

## ADR-001 — Fixed research snapshot is not an architecture limit

**Decision:** The current 50 games and 25,000 reviews form a versioned initial research snapshot. A future Game Registry / Watchlist can add games.

**Reason:** Fixed snapshots make EDA, ML, and benchmarks reproducible while the scalable control plane remains dynamic.

## ADR-002 — HDFS is authoritative for Bronze, Silver, and Gold

**Decision:** HDFS is the data lake system of record. Local `data/raw/` folders are staging/crawl/backup areas.

**Reason:** This separates development files from distributed, replayable, governed lake data.

## ADR-003 — Bronze remains raw for the managed scope

**Decision:** Bronze stores raw Steam records, historical reviews, metadata, retained source pages, and planned raw events with minimal transformation.

**Reason:** Selecting scope or checking completeness before upload does not make records analytically cleaned; raw replay must remain possible.

## ADR-004 — NEW games use historical batch backfill

**Decision:** A qualified `NEW` registry entry receives historical metadata/review backfill before becoming `ACTIVE`.

**Reason:** New games need a baseline history before incremental monitoring has meaningful state.

## ADR-005 — ACTIVE games use API polling plus Kafka

**Decision:** `ACTIVE` games are polled incrementally; detected changes become internal Kafka events. Steam is not treated as a native push source.

**Reason:** Polling supports near-real-time change capture without repeating full historical crawls.

## ADR-006 — Kafka events have an independent raw Bronze archive

**Decision:** One Kafka consumer/path archives event envelopes to HDFS Bronze while another feeds Structured Streaming.

**Reason:** An immutable archive supports audit and replay even if analytical streaming fails or rules change.

## ADR-007 — Batch and streaming converge on shared Silver/Gold contracts

**Decision:** Both processing modes write compatible governed datasets rather than separate truths.

**Reason:** Shared schemas, keys, and idempotent rules enable reconciliation and batch rebuilds of incremental state.

## ADR-008 — ML uses fixed/versioned snapshots

**Decision:** Every reported ML experiment references an immutable/versioned dataset and feature contract.

**Reason:** Dynamic discovery and streaming updates must not silently change training/evaluation data behind published metrics.

## ADR-009 — Discovery is scope control, not cleaning or target optimization

**Decision:** Qualification may use metadata completeness, review availability, feasibility, genre, and popularity diversity, but not `voted_up` or recommendation-rate outcomes.

**Reason:** Keeping target outcomes out of selection reduces analytical/ML selection bias and preserves a clear pipeline boundary.

## ADR-010 — One-time backfill and routine polling are separate operations

**Decision:** An `ACTIVE` game is not fully recrawled on each discovery cycle. Full rebuild/backfill is explicit and auditable.

**Reason:** Separating lifecycle operations avoids redundant API load and ambiguous duplicate histories.
