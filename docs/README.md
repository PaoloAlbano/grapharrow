# GraphArrow Documentation

This directory contains project documentation and shareable technical analysis.
All versioned documents, source code and comments are written in English.

## Technical guides

- [CI and Releases](releases.md): Python test matrices, tag-driven GitHub Release
  assets and Docker image versioning for amd64 and arm64.

- [Server Startup and Authentication](server-configuration.md): HTTP/ANA and Bolt
  startup, separate credentials, client examples, bind addresses and Docker.
- [ANA HTTP Interface](ana-interface.md): default agent tools, RPC, sessions and runtime limits.
- [Interface Comparison](interface-comparison.md): expected and actual graph
  results across Python, REST, Bolt, MCP, ANA and CLI ingestion.
- [Transactions and Sessions](transactions.md): write visibility, explicit Bolt
  transactions, embedded session behavior and writer-coordination limits.
- [Cypher V1](cypher-v1.md): supported query features, execution and limitations.
- [Known Cypher Issues](known-cypher-issues.md): reproducible correctness defects
  and current conformance limits.
- [Query Execution Architecture](query-execution-architecture.md): parsing,
  compilation, planning, storage access and public interfaces.
- [Engine Performance](engine-performance.md): discovery, bounded aggregation and
  ordered limits, physical equality indexes, file reuse, and Cypher syntax caching.
- [Parquet Predicate Pushdown](parquet-predicate-pushdown.md): reader filters,
  schema checks, residual execution, and row-group pruning.
- [Columnar Cypher Execution](cypher-columnar-execution.md): narrow property
  scans and Arrow filters/projections for composable read pipelines.
- [Planner V1](planner-v1.md): logical operators, supported query shapes and fallback behavior.
- [MCP Interface](mcp-interface-design.md): the current Cypher-oriented tool interface.
- [Dependencies and Installation](dependency-compatibility.md): Python and Arrow
  requirements, locked environments, runtime installs, and storage compatibility.
- [Real Object Storage Testing](real-object-storage-testing.md): optional live
  integration tests for S3, GCS and Azure.

## Analysis and benchmarks

- [ANA Session Resources](benchmarks/ana-session-resources.md): session latency,
  resident memory and retained-state quota accounting on a deterministic graph.
- [Concurrent ANA Traversals](benchmarks/ana-traversal-stress.md) measures
  high-degree traversals, small output limits, concurrent workflows, and peak RSS.
- [Streaming ANA Patterns](benchmarks/ana-pattern-streaming.md) compares
  pattern latency, memory, and complete branch counts with progressive execution.
- [Shared Snapshot Adjacency](benchmarks/shared-adjacency.md) compares the common
  engine through native Cypher, REST, Bolt, explicit transactions and ANA workflows.
- [ANA Seed Filtering and Ranked Discovery](benchmarks/ana-seed-discovery.md)
  measures filtering before payload construction and uncached entity resolution.
- [Data Access Backend Evaluation](data-access-backend-evaluation.md): the
  rationale for using PyArrow as the storage and execution substrate.
- [Benchmark Reports](benchmarks/): recorded results, comparisons and supporting profiles.

Benchmark reports describe the configuration and code available when each run
was recorded. They are historical measurements, not performance guarantees for
every graph or environment.

Private plans, TODO lists, roadmaps, internal ADRs and investigation notes belong
in the ignored local working directories. They are not part of this documentation.
