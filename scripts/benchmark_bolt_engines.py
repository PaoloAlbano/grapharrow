"""Benchmark the same Cypher workload against Bolt-compatible graph engines."""

from __future__ import annotations

import argparse
import concurrent.futures
import hashlib
import json
import shutil
import statistics
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, TypeVar, cast
from urllib.parse import urlparse

import numpy as np
import pyarrow as pa  # type: ignore[import-untyped]
from neo4j import GraphDatabase
from neo4j.graph import Node, Relationship
from neo4j.graph import Path as Neo4jPath

from grapharrow import Database
from grapharrow.config import GraphArrowSettings
from grapharrow.interfaces.bolt import run_bolt_server

if TYPE_CHECKING:
    from scripts.benchmark_cypher_engine import COUNTRIES, build_queries
else:
    try:
        from benchmark_cypher_engine import COUNTRIES, build_queries
    except ModuleNotFoundError:
        from scripts.benchmark_cypher_engine import COUNTRIES, build_queries

T = TypeVar("T")


@dataclass(frozen=True, slots=True)
class EngineConfig:
    name: str
    uri: str
    user: str | None
    password: str | None
    seed: bool


@dataclass(frozen=True, slots=True)
class BoltQueryResult:
    engine: str
    query: str
    status: str
    median_seconds: float | None
    best_seconds: float | None
    worst_seconds: float | None
    runs: tuple[float, ...]
    row_count: int | None
    checksum: str | None
    semantic_checksum: str | None
    error: str | None = None


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Cypher queries over Bolt.")
    parser.add_argument("--nodes", type=int, default=20_000)
    parser.add_argument("--edges", type=int, default=120_000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument("--warmups", type=int, default=1)
    parser.add_argument(
        "--query-set",
        choices=("synthetic", "got", "movies"),
        default="synthetic",
        help="Cypher workload to run. Non-synthetic query sets require preloaded data.",
    )
    parser.add_argument(
        "--no-seed",
        action="store_true",
        help=(
            "Do not clear or seed any engine. The benchmark connects to the configured "
            "Bolt URIs and runs queries against the data already present."
        ),
    )
    parser.add_argument(
        "--only-query",
        action="append",
        default=[],
        help="Run only this query name. Repeat to select multiple queries.",
    )
    parser.add_argument(
        "--skip-query",
        action="append",
        default=[],
        help="Skip this query name. Repeat to skip multiple queries.",
    )
    parser.add_argument(
        "--create-lookup-indexes",
        action="store_true",
        help="Create Person(external_id) lookup indexes on engines selected for seeding.",
    )
    parser.add_argument(
        "--grapharrow-bulk-seed-path",
        type=Path,
        default=None,
        help=(
            "Seed this GraphArrow database path with native bulk ingest instead of Bolt. "
            "The path is removed and rebuilt when grapharrow seeding is enabled."
        ),
    )
    parser.add_argument(
        "--preload-grapharrow-path",
        type=Path,
        default=None,
        help=(
            "Use an existing GraphArrow database as the benchmark dataset. GraphArrow is "
            "served from this path, while external engines are cleared and loaded from it."
        ),
    )
    parser.add_argument(
        "--json-out",
        type=Path,
        default=Path("docs") / "benchmarks" / "bolt-engine-comparison.json",
    )
    parser.add_argument(
        "--markdown-out",
        type=Path,
        default=Path("docs") / "benchmarks" / "bolt-engine-comparison.md",
    )

    add_engine_args(
        parser,
        "grapharrow",
        default_uri="bolt://127.0.0.1:18080",
        default_user=None,
        default_password=None,
        default_seed=True,
    )
    add_engine_args(
        parser,
        "neo4j",
        default_uri="bolt://127.0.0.1:7687",
        default_user="neo4j",
        default_password="password",
    )
    add_engine_args(
        parser,
        "memgraph",
        default_uri="bolt://127.0.0.1:7688",
        default_user=None,
        default_password=None,
    )
    args = parser.parse_args()

    engines = engine_configs_from_args(args)
    if args.no_seed:
        engines = tuple(replace(engine, seed=False) for engine in engines)
    if (
        args.query_set != "synthetic"
        and args.preload_grapharrow_path is None
        and any(engine.seed for engine in engines)
    ):
        raise SystemExit(
            f"--query-set {args.query_set!r} runs against preloaded data; pass --no-seed."
        )
    queries = selected_queries(
        build_queries_for_set(str(args.query_set)),
        only=set(cast(list[str], args.only_query)),
        skip=set(cast(list[str], args.skip_query)),
    )
    query_params: dict[str, dict[str, object]] = {
        "unwind_parameter_pipeline": {"values": [f"Tag-{index:04d}" for index in range(500)]},
        "got_unwind_parameter_names": {
            "names": [
                "Jon Snow",
                "Arya Stark",
                "Tyrion Lannister",
                "Daenerys Targaryen",
                "Cersei Lannister",
            ]
        },
    }
    dataset = (
        generate_synthetic_dataset(
            node_count=args.nodes,
            edge_count=args.edges,
            seed=args.seed,
        )
        if any(engine.seed for engine in engines)
        else None
    )

    managed_grapharrow_server = None
    all_results: list[BoltQueryResult] = []
    try:
        measured_engines: list[EngineConfig] = []
        for engine in engines:
            if engine.name == "grapharrow" and args.preload_grapharrow_path is not None:
                grapharrow_path = cast(Path, args.preload_grapharrow_path)
                host, port = _host_port_from_bolt_uri(engine.uri)
                _log(
                    f"Starting managed GraphArrow Bolt server on {host}:{port} "
                    f"for preloaded database {grapharrow_path}."
                )
                managed_grapharrow_server = run_bolt_server(
                    grapharrow_path,
                    settings=GraphArrowSettings(
                        database_path=str(grapharrow_path),
                        bolt_host=host,
                        bolt_port=port,
                        bolt_auth_token=engine.password,
                    ),
                )
                managed_grapharrow_server.__enter__()
                engine = EngineConfig(
                    name=engine.name,
                    uri=engine.uri,
                    user=engine.user,
                    password=engine.password,
                    seed=False,
                )
            elif engine.name == "grapharrow" and engine.seed and args.grapharrow_bulk_seed_path:
                assert dataset is not None
                grapharrow_path = cast(Path, args.grapharrow_bulk_seed_path)
                seed_grapharrow_bulk(
                    database_path=grapharrow_path,
                    dataset=dataset,
                    create_lookup_indexes=args.create_lookup_indexes,
                )
                host, port = _host_port_from_bolt_uri(engine.uri)
                _log(
                    f"Starting managed GraphArrow Bolt server on {host}:{port} "
                    f"for {grapharrow_path}."
                )
                managed_grapharrow_server = run_bolt_server(
                    grapharrow_path,
                    settings=GraphArrowSettings(
                        database_path=str(grapharrow_path),
                        bolt_host=host,
                        bolt_port=port,
                        bolt_auth_token=engine.password,
                    ),
                )
                managed_grapharrow_server.__enter__()
                engine = EngineConfig(
                    name=engine.name,
                    uri=engine.uri,
                    user=engine.user,
                    password=engine.password,
                    seed=False,
                )
            measured_engines.append(engine)

        for engine in measured_engines:
            _log(f"Connecting to {engine.name} at {engine.uri}.")
            with driver_for(engine) as driver:
                verify_connectivity(driver, engine)
                if args.preload_grapharrow_path is not None and engine.name != "grapharrow":
                    seed_engine_from_grapharrow(
                        driver,
                        engine=engine,
                        database_path=cast(Path, args.preload_grapharrow_path),
                    )
                elif engine.seed:
                    assert dataset is not None
                    seed_engine(
                        driver,
                        engine=engine,
                        dataset=dataset,
                        create_lookup_indexes=args.create_lookup_indexes,
                    )
                for query_name, query in queries.items():
                    _log(f"Benchmarking {engine.name}/{query_name}.")
                    result = benchmark_query(
                        driver,
                        engine=engine.name,
                        query_name=query_name,
                        query=query,
                        params=query_params.get(query_name, {}),
                        repeats=args.repeats,
                        warmups=args.warmups,
                    )
                    all_results.append(result)
                    if result.status == "ok":
                        assert result.median_seconds is not None
                        _log(
                            f"Completed {engine.name}/{query_name} "
                            f"(median={result.median_seconds:.4f}s, rows={result.row_count})."
                        )
                    else:
                        _log(f"Failed {engine.name}/{query_name}: {result.error}")
    finally:
        if managed_grapharrow_server is not None:
            managed_grapharrow_server.__exit__(None, None, None)

    args.json_out.parent.mkdir(parents=True, exist_ok=True)
    args.markdown_out.parent.mkdir(parents=True, exist_ok=True)
    write_json_report(args.json_out, args=args, engines=engines, results=all_results)
    write_markdown_report(args.markdown_out, args=args, engines=engines, results=all_results)
    _log(f"Wrote JSON report to {args.json_out}")
    _log(f"Wrote Markdown report to {args.markdown_out}")
    print(f"Wrote JSON report to {args.json_out}")
    print(f"Wrote Markdown report to {args.markdown_out}")


def add_engine_args(
    parser: argparse.ArgumentParser,
    name: str,
    *,
    default_uri: str,
    default_user: str | None,
    default_password: str | None,
    default_seed: bool = False,
) -> None:
    parser.add_argument(f"--{name}-uri", default=default_uri)
    parser.add_argument(f"--{name}-user", default=default_user)
    parser.add_argument(f"--{name}-password", default=default_password)
    parser.add_argument(
        f"--{name}-disabled",
        action="store_true",
        help=f"Do not benchmark {name}.",
    )
    if default_seed:
        parser.add_argument(
            f"--{name}-no-seed",
            dest=f"{name.replace('-', '_')}_seed",
            action="store_false",
            default=True,
            help=f"Skip clearing and seeding {name} with the synthetic benchmark dataset.",
        )
    else:
        parser.add_argument(
            f"--{name}-seed",
            action="store_true",
            help=f"Clear and seed {name} with the synthetic benchmark dataset before measuring.",
        )


def engine_configs_from_args(args: argparse.Namespace) -> tuple[EngineConfig, ...]:
    engines: list[EngineConfig] = []
    for name in ("grapharrow", "neo4j", "memgraph"):
        if bool(getattr(args, f"{name}_disabled")):
            continue
        engines.append(
            EngineConfig(
                name=name,
                uri=str(getattr(args, f"{name}_uri")),
                user=_empty_to_none(getattr(args, f"{name}_user")),
                password=_empty_to_none(getattr(args, f"{name}_password")),
                seed=bool(getattr(args, f"{name}_seed")),
            )
        )
    return tuple(engines)


def selected_queries(
    queries: Mapping[str, str],
    *,
    only: set[str],
    skip: set[str],
) -> dict[str, str]:
    missing = (only | skip) - set(queries)
    if missing:
        raise SystemExit(f"Unknown query names: {', '.join(sorted(missing))}")
    return {
        name: query
        for name, query in queries.items()
        if (not only or name in only) and name not in skip
    }


def build_queries_for_set(query_set: str) -> dict[str, str]:
    if query_set == "synthetic":
        return build_queries()
    if query_set == "got":
        return build_got_queries()
    if query_set == "movies":
        return build_movies_queries()
    raise SystemExit(f"Unsupported query set: {query_set}")


def build_got_queries() -> dict[str, str]:
    """Queries based on Memgraph's Game of Thrones deaths example dataset."""

    return {
        "got_schema_labels": (
            "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count DESC"
        ),
        "got_schema_relationship_types": (
            "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC"
        ),
        "got_character_name_projection": (
            "MATCH (c:Character) RETURN c.name AS name ORDER BY name ASC LIMIT 100"
        ),
        "got_character_name_starts_with": (
            "MATCH (c:Character) "
            "WHERE c.name STARTS WITH 'Jon' "
            "RETURN c.name AS name "
            "ORDER BY name ASC"
        ),
        "got_character_name_tolower_contains": (
            "MATCH (c:Character) "
            "WHERE toLower(c.name) CONTAINS 'snow' "
            "RETURN c.name AS name "
            "ORDER BY name ASC"
        ),
        "got_character_keys_sample": ("MATCH (c:Character) RETURN keys(c) AS keys LIMIT 20"),
        "got_episode_deaths": (
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "RETURN e.name AS episode_name, count(d) AS kill_count "
            "ORDER BY kill_count DESC"
        ),
        "got_season_deaths": (
            "MATCH (d:Death)-[:HAPPENED_IN]->(s:Season) "
            "RETURN s.number AS season_number, count(d) AS death_count "
            "ORDER BY season_number ASC"
        ),
        "got_location_deaths": (
            "MATCH (l:Location)<-[:HAPPENED_IN]-(d:Death) "
            "RETURN l.name AS location_name, count(d) AS death_count "
            "ORDER BY death_count DESC"
        ),
        "got_castle_location_text_filter": (
            "MATCH (l:Location)<-[:HAPPENED_IN]-(d:Death) "
            "WHERE l.name CONTAINS 'Castle' "
            "RETURN l.name AS location, count(d) AS deaths "
            "ORDER BY deaths DESC"
        ),
        "got_relationship_type_text_filter_count": (
            "MATCH ()-[r]->() WHERE type(r) STARTS WITH 'KILL' RETURN count(r) AS count"
        ),
        "got_killed_method_text_filter_count": (
            "MATCH (:Character)-[r:KILLED]->(:Character) "
            "WHERE r.method CONTAINS 'sword' "
            "RETURN count(r) AS count"
        ),
        "got_episode_avg_rating": (
            "MATCH (e:Episode)-[:PART_OF]->(s:Season) "
            "RETURN s.number AS season_number, round(100 * avg(e.imdb_rating))/100 AS rating "
            "ORDER BY rating DESC"
        ),
        "got_episode_collect_death_orders": (
            "MATCH (d:Death)-[:HAPPENED_IN]->(e:Episode) "
            "WITH e.name AS episode, collect(d.order) AS death_orders "
            "RETURN episode, size(death_orders) AS death_count "
            "ORDER BY size(death_orders) DESC "
            "LIMIT 20"
        ),
        "got_victim_location_two_hop_projection": (
            "MATCH (c:Character)-[:VICTIM_IN]->(d:Death)-[:HAPPENED_IN]->(l:Location) "
            "RETURN c.name AS character, l.name AS location "
            "ORDER BY character ASC, location ASC "
            "LIMIT 100"
        ),
        "got_unwind_parameter_names": (
            "UNWIND $names AS name RETURN toLower(name) AS normalized ORDER BY toLower(name) ASC"
        ),
        "got_visual_death_location": (
            "MATCH (l:Location)<-[h:HAPPENED_IN]-(d:Death) "
            "RETURN l, h, d "
            "ORDER BY d.order ASC "
            "LIMIT 50"
        ),
        "got_jon_snow_death_locations": (
            "MATCH (jon:Character {name: 'Jon Snow'})-[:KILLED]->(victim:Character) "
            "MATCH (jon)-[:KILLER_IN]->(death:Death)<-[victim_to_death:VICTIM_IN]-(victim) "
            "MATCH (death)-[death_to_location:HAPPENED_IN]->(location:Location) "
            "RETURN victim, victim_to_death, death, death_to_location, location"
        ),
    }


def build_movies_queries() -> dict[str, str]:
    """Queries for Neo4j Movies/Recommendations-style datasets."""

    return {
        "movies_schema_labels": (
            "MATCH (n) RETURN labels(n) AS labels, count(n) AS count ORDER BY count DESC"
        ),
        "movies_schema_relationship_types": (
            "MATCH ()-[r]->() RETURN type(r) AS type, count(r) AS count ORDER BY count DESC"
        ),
        "movies_top_actors": (
            "MATCH (p:Person)-[:ACTED_IN]->(m:Movie) "
            "RETURN p.name AS person, count(m) AS movies "
            "ORDER BY movies DESC "
            "LIMIT 25"
        ),
        "movies_content_edges": (
            "MATCH (p:Person)-[:ACTED_IN|DIRECTED]->(m:Movie) "
            "RETURN p.name AS person, count(m) AS content_edges "
            "ORDER BY content_edges DESC "
            "LIMIT 25"
        ),
        "movies_title_text_filter": (
            "MATCH (m:Movie) "
            "WHERE m.title CONTAINS 'Matrix' "
            "RETURN m.title AS title "
            "ORDER BY title ASC"
        ),
        "movies_recommendations_common_raters": (
            "MATCH (m:Movie {title: 'Crimson Tide'})<-[:RATED]-"
            "(u:User)-[:RATED]->(rec:Movie) "
            "WITH rec, count(*) AS usersWhoAlsoWatched "
            "ORDER BY usersWhoAlsoWatched DESC "
            "LIMIT 25 "
            "RETURN rec.title AS recommendation, usersWhoAlsoWatched"
        ),
    }


def driver_for(engine: EngineConfig) -> Any:
    auth = None if engine.user is None else (engine.user, engine.password or "")
    return GraphDatabase.driver(engine.uri, auth=auth)


def verify_connectivity(driver: Any, engine: EngineConfig) -> None:
    try:
        driver.verify_connectivity()
    except Exception as exc:  # pragma: no cover - exercised by live runs.
        raise SystemExit(f"Cannot connect to {engine.name} at {engine.uri}: {exc}") from exc


def _host_port_from_bolt_uri(uri: str) -> tuple[str, int]:
    parsed = urlparse(uri)
    if parsed.scheme not in {"bolt", "neo4j"}:
        raise SystemExit(f"Unsupported GraphArrow Bolt URI scheme: {parsed.scheme}")
    return parsed.hostname or "127.0.0.1", parsed.port or 7687


@dataclass(frozen=True, slots=True)
class SyntheticDataset:
    nodes: tuple[dict[str, object], ...]
    knows_edges: tuple[dict[str, object], ...]
    follows_edges: tuple[dict[str, object], ...]


def generate_synthetic_dataset(
    *,
    node_count: int,
    edge_count: int,
    seed: int,
) -> SyntheticDataset:
    rng = np.random.default_rng(seed)
    node_ids: Any = np.arange(1, node_count + 1, dtype=np.int64)
    anchor_count = min(500, node_count)
    countries = rng.choice(COUNTRIES, size=node_count).tolist()
    active_values = (rng.random(node_count) > 0.25).tolist()
    scores = rng.random(node_count).tolist()
    nodes = tuple(
        {
            "bench_id": int(node_id),
            "external_id": f"person-{node_id:06d}",
            "name": f"Person {node_id:06d}",
            "country": str(countries[index]),
            "active": bool(active_values[index]),
            "score": float(scores[index]),
            "anchor": bool(node_id <= anchor_count),
        }
        for index, node_id in enumerate(node_ids)
    )

    random_edges = edge_count - (anchor_count * 3)
    src_random = rng.integers(1, node_count + 1, size=max(random_edges, 0), dtype=np.int64)
    dst_random = rng.integers(1, node_count + 1, size=max(random_edges, 0), dtype=np.int64)
    controlled_sources: Any = np.repeat(np.arange(1, anchor_count + 1, dtype=np.int64), 3)
    controlled_targets = np.concatenate(
        [
            controlled_sources + 1,
            controlled_sources + 2,
            controlled_sources + 3,
        ]
    )[: anchor_count * 3]
    controlled_targets = ((controlled_targets - 1) % node_count) + 1
    src_ids = np.concatenate([controlled_sources, src_random])
    dst_ids = np.concatenate([controlled_targets, dst_random])
    rel_types = np.array(["KNOWS"] * len(src_ids), dtype=object)
    if len(rel_types) > len(controlled_sources):
        random_rel_count = len(rel_types) - len(controlled_sources)
        rel_types[len(controlled_sources) :] = rng.choice(
            np.array(["KNOWS", "FOLLOWS"], dtype=object),
            size=random_rel_count,
            p=[0.8, 0.2],
        )
    weights = rng.random(len(src_ids)).tolist()
    knows_edges: list[dict[str, object]] = []
    follows_edges: list[dict[str, object]] = []
    for src_id, dst_id, rel_type, weight in zip(
        src_ids.tolist(),
        dst_ids.tolist(),
        rel_types.tolist(),
        weights,
        strict=True,
    ):
        edge: dict[str, object] = {
            "src_id": int(src_id),
            "dst_id": int(dst_id),
            "weight": float(weight),
        }
        if rel_type == "FOLLOWS":
            follows_edges.append(edge)
        else:
            knows_edges.append(edge)
    return SyntheticDataset(
        nodes=nodes,
        knows_edges=tuple(knows_edges),
        follows_edges=tuple(follows_edges),
    )


def seed_engine(
    driver: Any,
    *,
    engine: EngineConfig,
    dataset: SyntheticDataset,
    create_lookup_indexes: bool,
) -> None:
    _log(f"Clearing {engine.name}.")
    execute_write(driver, "MATCH (n) DETACH DELETE n", {})
    create_best_effort_index(driver, engine.name, "bench_id")
    if create_lookup_indexes:
        create_best_effort_index(driver, engine.name, "external_id")

    # GraphArrow does not support UNWIND in mixed read+write queries, and does not
    # support concurrent write transactions — seed sequentially (workers=1).
    use_unwind = engine.name != "grapharrow"
    seed_workers = 1 if engine.name == "grapharrow" else 8

    _log(f"Seeding {engine.name}: {len(dataset.nodes)} nodes.")
    if use_unwind:
        for batch in batched(dataset.nodes, 2_000):
            execute_write(
                driver,
                (
                    "UNWIND $rows AS row "
                    "CREATE (:Person {"
                    "bench_id: row.bench_id, external_id: row.external_id, name: row.name, "
                    "country: row.country, active: row.active, score: row.score, anchor: row.anchor"
                    "})"
                ),
                {"rows": list(batch)},
            )
    else:
        node_stmt = (
            "CREATE (:Person {"
            "bench_id: $bench_id, external_id: $external_id, name: $name, "
            "country: $country, active: $active, score: $score, anchor: $anchor"
            "})"
        )
        seed_individually(driver, dataset.nodes, node_stmt, workers=seed_workers)

    _log(f"Seeding {engine.name}: {len(dataset.knows_edges)} KNOWS edges.")
    seed_edges(
        driver, dataset.knows_edges, rel_type="KNOWS", use_unwind=use_unwind, workers=seed_workers
    )
    _log(f"Seeding {engine.name}: {len(dataset.follows_edges)} FOLLOWS edges.")
    seed_edges(
        driver,
        dataset.follows_edges,
        rel_type="FOLLOWS",
        use_unwind=use_unwind,
        workers=seed_workers,
    )


def seed_grapharrow_bulk(
    *,
    database_path: Path,
    dataset: SyntheticDataset,
    create_lookup_indexes: bool,
) -> None:
    """Seed GraphArrow with native bulk ingest so Bolt only measures query serving."""

    if database_path.exists():
        _log(f"Removing existing GraphArrow database at {database_path}.")
        shutil.rmtree(database_path)

    database = Database(database_path)
    database.initialize()
    node_table = pa.table(
        {
            "bench_id": pa.array([row["bench_id"] for row in dataset.nodes], type=pa.int64()),
            "external_id": pa.array(
                [row["external_id"] for row in dataset.nodes], type=pa.string()
            ),
            "name": pa.array([row["name"] for row in dataset.nodes], type=pa.string()),
            "country": pa.array([row["country"] for row in dataset.nodes], type=pa.string()),
            "active": pa.array([row["active"] for row in dataset.nodes], type=pa.bool_()),
            "score": pa.array([row["score"] for row in dataset.nodes], type=pa.float64()),
            "anchor": pa.array([row["anchor"] for row in dataset.nodes], type=pa.bool_()),
        }
    )
    edge_rows = [{**row, "rel_type": "KNOWS"} for row in dataset.knows_edges] + [
        {**row, "rel_type": "FOLLOWS"} for row in dataset.follows_edges
    ]
    edge_table = pa.table(
        {
            "src_id": pa.array([row["src_id"] for row in edge_rows], type=pa.int64()),
            "dst_id": pa.array([row["dst_id"] for row in edge_rows], type=pa.int64()),
            "rel_type": pa.array([row["rel_type"] for row in edge_rows], type=pa.string()),
            "weight": pa.array([row["weight"] for row in edge_rows], type=pa.float64()),
        }
    )

    _log(f"Bulk seeding GraphArrow: {len(dataset.nodes)} nodes.")
    database._app.use_cases.graph.create_nodes_bulk(
        table=node_table,
        default_labels=("Person",),
    )
    _log(f"Bulk seeding GraphArrow: {len(edge_rows)} edges.")
    database._app.use_cases.graph.create_edges_bulk(table=edge_table)
    if create_lookup_indexes:
        database.create_index(
            name="person_external_id_idx",
            kind="range",
            owner_kind="label",
            owner_name="Person",
            property_names=("external_id",),
            if_not_exists=True,
        )
    _log(f"Finished GraphArrow bulk seed at {database_path}.")


def seed_engine_from_grapharrow(
    driver: Any,
    *,
    engine: EngineConfig,
    database_path: Path,
) -> None:
    """Load an external Bolt engine from an existing GraphArrow snapshot."""

    database = Database(database_path)
    state = database._app.use_cases.graph.load_tabular_state()
    node_rows = [
        {
            "id": int(row["node_id"]),
            "labels": tuple(_normalize_cypher_name(label) for label in row["labels"]),
            "props": _normalize_property_keys(json.loads(str(row["properties_json"]))),
        }
        for row in state.node_table.to_pylist()
    ]
    edge_rows = [
        {
            "id": int(row["edge_id"]),
            "type": _normalize_cypher_name(str(row["rel_type"])),
            "src_id": int(row["src_id"]),
            "dst_id": int(row["dst_id"]),
            "props": _normalize_property_keys(json.loads(str(row["properties_json"]))),
        }
        for row in state.edge_table.to_pylist()
    ]

    _log(
        f"Clearing {engine.name} before GraphArrow preload "
        f"({len(node_rows)} nodes, {len(edge_rows)} edges)."
    )
    execute_write(driver, "MATCH (n) DETACH DELETE n", {})

    nodes_by_labels: dict[tuple[str, ...], list[dict[str, object]]] = {}
    for row in node_rows:
        labels = cast(tuple[str, ...], row["labels"])
        nodes_by_labels.setdefault(labels, []).append(row)

    for labels, rows in sorted(nodes_by_labels.items()):
        label_clause = "".join(f":{_quote_identifier(label)}" for label in labels)
        statement = (
            f"UNWIND $rows AS row CREATE (n{label_clause} {{__ga_id: row.id}}) SET n += row.props"
        )
        _log(f"Seeding {engine.name}: {len(rows)} nodes with labels {labels}.")
        for batch in batched(rows, 1_000):
            execute_write(driver, statement, {"rows": list(batch)})

    create_grapharrow_id_indexes(driver, engine.name, nodes_by_labels.keys())

    edges_by_type: dict[str, list[dict[str, object]]] = {}
    for row in edge_rows:
        rel_type = cast(str, row["type"])
        edges_by_type.setdefault(rel_type, []).append(row)

    for rel_type, rows in sorted(edges_by_type.items()):
        statement = (
            "UNWIND $rows AS row "
            "MATCH (a {__ga_id: row.src_id}) "
            "MATCH (b {__ga_id: row.dst_id}) "
            f"CREATE (a)-[r:{_quote_identifier(rel_type)}]->(b) "
            "SET r += row.props"
        )
        _log(f"Seeding {engine.name}: {len(rows)} {rel_type} edges.")
        for batch in batched(rows, 1_000):
            execute_write(driver, statement, {"rows": list(batch)})


def create_grapharrow_id_indexes(
    driver: Any,
    engine_name: str,
    label_groups: Iterable[tuple[str, ...]],
) -> None:
    """Best-effort indexes used only to speed up real dataset preload edge matching."""

    labels = sorted({label for group in label_groups for label in group})
    for label in labels:
        statements = (
            f"CREATE INDEX {_safe_index_name(label)}_ga_id_idx IF NOT EXISTS "
            f"FOR (n:{_quote_identifier(label)}) ON (n.__ga_id)",
            f"CREATE INDEX ON :{_quote_identifier(label)}(__ga_id)",
        )
        for statement in statements:
            try:
                execute_write(driver, statement, {})
                _log(f"Created/kept {engine_name} preload index on {label}(__ga_id).")
                break
            except Exception:
                continue


def _normalize_property_keys(properties: Mapping[str, object]) -> dict[str, object]:
    return {_normalize_cypher_name(key): value for key, value in properties.items()}


def _normalize_cypher_name(name: str) -> str:
    if len(name) >= 2 and name.startswith("`") and name.endswith("`"):
        return name[1:-1].replace("``", "`")
    return name


def _quote_identifier(name: str) -> str:
    return f"`{name.replace('`', '``')}`"


def _safe_index_name(label: str) -> str:
    return "".join(char.lower() if char.isalnum() else "_" for char in label).strip("_") or "node"


def create_best_effort_index(driver: Any, engine_name: str, property_name: str) -> None:
    statements = (
        f"CREATE INDEX person_{property_name}_idx IF NOT EXISTS "
        f"FOR (n:Person) ON (n.{property_name})",
        f"CREATE INDEX ON :Person({property_name})",
    )
    for statement in statements:
        try:
            execute_write(driver, statement, {})
            _log(f"Created/kept {engine_name} index on Person({property_name}).")
            return
        except Exception:
            continue
    _log(f"Could not create {engine_name} index on Person({property_name}); continuing.")


def seed_edges(
    driver: Any,
    edges: Sequence[dict[str, object]],
    *,
    rel_type: str,
    use_unwind: bool = True,
    workers: int = 8,
) -> None:
    if not edges:
        return
    if use_unwind:
        statement = (
            "UNWIND $rows AS row "
            "MATCH (a:Person {bench_id: row.src_id}) "
            "MATCH (b:Person {bench_id: row.dst_id}) "
            f"CREATE (a)-[:{rel_type} {{weight: row.weight}}]->(b)"
        )
        for batch in batched(edges, 2_000):
            execute_write(driver, statement, {"rows": list(batch)})
    else:
        edge_stmt = (
            "MATCH (a:Person {bench_id: $src_id}), (b:Person {bench_id: $dst_id}) "
            f"CREATE (a)-[:{rel_type} {{weight: $weight}}]->(b)"
        )
        seed_individually(driver, edges, edge_stmt, workers=workers)


def seed_individually(
    driver: Any,
    rows: Sequence[dict[str, object]],
    statement: str,
    batch_size: int = 500,
    workers: int = 8,
) -> None:
    """Execute *statement* once per row, grouping rows into batched transactions run in parallel."""
    all_batches = list(batched(rows, batch_size))

    def _write_batch(batch: Sequence[dict[str, object]]) -> None:
        with driver.session() as session:

            def _run(tx: Any, b: Sequence[dict[str, object]] = batch) -> None:
                for row in b:
                    tx.run(statement, dict(row))

            session.execute_write(_run)

    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_write_batch, batch) for batch in all_batches]
        for future in concurrent.futures.as_completed(futures):
            future.result()  # re-raise any exception


def execute_write(driver: Any, query: str, params: Mapping[str, object]) -> None:
    with driver.session() as session:
        session.execute_write(lambda tx: tx.run(query, dict(params)).consume())


def benchmark_query(
    driver: Any,
    *,
    engine: str,
    query_name: str,
    query: str,
    params: Mapping[str, object],
    repeats: int,
    warmups: int,
) -> BoltQueryResult:
    try:
        for _ in range(warmups):
            run_query(driver, query, params)
        runs: list[float] = []
        row_count = 0
        checksum = ""
        semantic_checksum = ""
        for _ in range(repeats):
            started = time.perf_counter()
            rows = run_query(driver, query, params)
            elapsed = time.perf_counter() - started
            runs.append(elapsed)
            normalized_rows = normalize_value(rows)
            checksum = hashlib.sha256(
                json.dumps(normalized_rows, sort_keys=True).encode("utf-8")
            ).hexdigest()
            semantic_rows = semantic_normalize_rows(rows)
            semantic_checksum = hashlib.sha256(
                json.dumps(semantic_rows, sort_keys=True).encode("utf-8")
            ).hexdigest()
            row_count = len(rows)
        return BoltQueryResult(
            engine=engine,
            query=query_name,
            status="ok",
            median_seconds=statistics.median(runs),
            best_seconds=min(runs),
            worst_seconds=max(runs),
            runs=tuple(runs),
            row_count=row_count,
            checksum=checksum,
            semantic_checksum=semantic_checksum,
        )
    except Exception as exc:
        return BoltQueryResult(
            engine=engine,
            query=query_name,
            status="error",
            median_seconds=None,
            best_seconds=None,
            worst_seconds=None,
            runs=(),
            row_count=None,
            checksum=None,
            semantic_checksum=None,
            error=str(exc),
        )


def run_query(driver: Any, query: str, params: Mapping[str, object]) -> list[dict[str, Any]]:
    with driver.session() as session:
        result = session.run(query, dict(params))
        return [dict(record) for record in result]


def normalize_value(value: object) -> object:
    if isinstance(value, Node):
        return {
            "id": value.element_id,
            "labels": sorted(value.labels),
            "properties": normalize_value(dict(value)),
        }
    if isinstance(value, Relationship):
        start_node = value.start_node
        end_node = value.end_node
        return {
            "id": value.element_id,
            "type": value.type,
            "start": start_node.element_id if start_node is not None else None,
            "end": end_node.element_id if end_node is not None else None,
            "properties": normalize_value(dict(value)),
        }
    if isinstance(value, Neo4jPath):
        return {
            "nodes": [normalize_value(node) for node in value.nodes],
            "relationships": [normalize_value(rel) for rel in value.relationships],
        }
    if isinstance(value, Mapping):
        return {str(key): normalize_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        return [normalize_value(item) for item in value]
    return value


def semantic_normalize_rows(rows: Sequence[Mapping[str, object]]) -> list[object]:
    """Build an engine-independent result signature for cross-engine validation."""

    normalized_rows = [semantic_normalize_value(row) for row in rows]
    return sorted(normalized_rows, key=_json_sort_key)


def semantic_normalize_value(value: object) -> object:
    if isinstance(value, Node):
        return {
            "kind": "node",
            "labels": sorted(value.labels),
            "properties": semantic_normalize_value(_semantic_node_properties(value)),
        }
    if isinstance(value, Relationship):
        start_node = value.start_node
        end_node = value.end_node
        return {
            "kind": "relationship",
            "type": value.type,
            "start": semantic_node_identity(start_node) if start_node is not None else None,
            "end": semantic_node_identity(end_node) if end_node is not None else None,
            "properties": semantic_normalize_value(dict(value)),
        }
    if isinstance(value, Neo4jPath):
        return {
            "kind": "path",
            "nodes": [semantic_normalize_value(node) for node in value.nodes],
            "relationships": [semantic_normalize_value(rel) for rel in value.relationships],
        }
    if isinstance(value, Mapping):
        return {str(key): semantic_normalize_value(item) for key, item in value.items()}
    if isinstance(value, list | tuple):
        normalized_items = [semantic_normalize_value(item) for item in value if item != "__ga_id"]
        return sorted(normalized_items, key=_json_sort_key)
    return value


def semantic_node_identity(node: Node) -> object:
    properties = _semantic_node_properties(node)
    for key in ("external_id", "bench_id", "name"):
        if key in properties:
            return {"labels": sorted(node.labels), key: semantic_normalize_value(properties[key])}
    return {"labels": sorted(node.labels), "properties": semantic_normalize_value(properties)}


def _semantic_node_properties(node: Node) -> dict[str, object]:
    return {key: value for key, value in dict(node).items() if key != "__ga_id"}


def _json_sort_key(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def batched(items: Sequence[T], size: int) -> Iterable[Sequence[T]]:
    for offset in range(0, len(items), size):
        yield items[offset : offset + size]


def write_json_report(
    path: Path,
    *,
    args: argparse.Namespace,
    engines: tuple[EngineConfig, ...],
    results: list[BoltQueryResult],
) -> None:
    payload = {
        "config": {
            "query_set": args.query_set,
            "nodes": args.nodes,
            "edges": args.edges,
            "seed": args.seed,
            "seed_mode": "disabled" if args.no_seed else "per_engine",
            "repeats": args.repeats,
            "warmups": args.warmups,
            "create_lookup_indexes": args.create_lookup_indexes,
            "engines": [asdict(engine) for engine in engines],
        },
        "results": [asdict(result) for result in results],
    }
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def write_markdown_report(
    path: Path,
    *,
    args: argparse.Namespace,
    engines: tuple[EngineConfig, ...],
    results: list[BoltQueryResult],
) -> None:
    lines = [
        "# Bolt Engine Comparison Benchmark",
        "",
        "This benchmark measures end-to-end Bolt response time for the shared Cypher workload.",
        "",
        "## Configuration",
        "",
        f"- Query set: `{args.query_set}`",
        f"- Nodes: `{args.nodes}`",
        f"- Edges: `{args.edges}`",
        f"- Repeats: `{args.repeats}`",
        f"- Warmups: `{args.warmups}`",
        f"- Seed: `{args.seed}`",
        f"- Seed mode: `{'disabled' if args.no_seed else 'per-engine'}`",
        f"- Lookup indexes during seed: `{'yes' if args.create_lookup_indexes else 'no'}`",
        f"- Engines: `{', '.join(engine.name for engine in engines)}`",
        "",
        "## Results",
        "",
        (
            "| Engine | Query | Status | Median (s) | Best (s) | Worst (s) | Rows | "
            "Checksum | Semantic |"
        ),
        "| --- | --- | --- | ---: | ---: | ---: | ---: | --- | --- |",
    ]
    for result in results:
        if result.status == "ok":
            assert result.median_seconds is not None
            assert result.best_seconds is not None
            assert result.worst_seconds is not None
            assert result.row_count is not None
            assert result.checksum is not None
            assert result.semantic_checksum is not None
            lines.append(
                "| "
                f"{result.engine} | {result.query} | ok | "
                f"{result.median_seconds:.6f} | "
                f"{result.best_seconds:.6f} | "
                f"{result.worst_seconds:.6f} | "
                f"{result.row_count} | "
                f"`{result.checksum[:12]}` | "
                f"`{result.semantic_checksum[:12]}` |"
            )
        else:
            lines.append(
                "| "
                f"{result.engine} | {result.query} | error |  |  |  |  | "
                f"{(result.error or '')[:120]} |  |"
            )
    lines.extend(
        [
            "",
            "## Notes",
            "",
            "- Memory is not measured for external engines in this benchmark.",
            "- Timings include Bolt round trip, result pull, and Python driver materialization.",
            "- Seed time is not included in query timings.",
            (
                "- Graph/value checksums are engine-local and are not expected "
                "to match across engines."
            ),
            (
                "- Semantic checksums ignore engine-local graph IDs and compare a "
                "canonicalized unordered result multiset."
            ),
        ]
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _empty_to_none(value: object) -> str | None:
    if value is None:
        return None
    text = str(value)
    return text if text else None


def _log(message: str) -> None:
    timestamp = datetime.now(UTC).astimezone().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}", file=sys.stderr, flush=True)


if __name__ == "__main__":
    main()
