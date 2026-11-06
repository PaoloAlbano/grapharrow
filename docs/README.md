# GraphArrow Documentation

This directory contains project documentation and shareable technical analysis.
All versioned documents, source code and comments are written in English.

## Technical guides

- [ANA HTTP Interface](ana-interface.md): default agent tools, RPC, sessions and compatibility limits.
- [Interface Comparison](interface-comparison.md): expected and actual graph
  results across Python, REST, Bolt, MCP, ANA and CLI ingestion.
- [Cypher V1](cypher-v1.md): supported query features, execution and limitations.
- [Query Execution Architecture](query-execution-architecture.md): parsing,
  compilation, planning, storage access and public interfaces.
- [Planner V1](planner-v1.md): logical operators, supported query shapes and fallback behavior.
- [MCP Interface](mcp-interface-design.md): the current Cypher-oriented tool interface.
- [Real Object Storage Testing](real-object-storage-testing.md): optional live
  integration tests for S3, GCS and Azure.

## Analysis and benchmarks

- [ANA Session Resources](benchmarks/ana-session-resources.md): session latency,
  resident memory and retained-state quota accounting on a deterministic graph.
- [Benchmark Reports](benchmarks/): recorded results, comparisons and supporting profiles.

Benchmark reports describe the configuration and code available when each run
was recorded. They are historical measurements, not performance guarantees for
every graph or environment.

Private plans, TODO lists, roadmaps, internal ADRs and investigation notes belong
in the ignored local working directories. They are not part of this documentation.
