"""Small deterministic graph shared by native and reference ANA workflow tests."""

from pathlib import Path
from typing import Any

import pyarrow as pa

from grapharrow.app import GraphArrowApplication, build_application

NODES = [
    ("Person", {"name": "Alice", "age": 50, "countries": ["IT", "FR"]}),
    ("Person", {"name": "Bob", "age": 40, "countries": ["IT"]}),
    ("Person", {"name": "Carol", "age": 60, "countries": ["FR"]}),
    ("Person", {"name": "Dana", "countries": []}),
    ("Person", {"name": "Alex", "age": 30, "countries": ["IT"]}),
    ("Person", {"name": "Alex", "age": 30, "countries": ["IT"]}),
    ("Company", {"name": "Acme", "year": 2000}),
    ("Company", {"name": "Beta", "year": 2010}),
    ("Company", {"name": "Gamma", "year": 1990}),
    ("Company", {"name": "Empty", "year": 1999}),
    ("Country", {"name": "Italy"}),
    ("Country", {"name": "France"}),
]
EDGES = [
    (6, 0, "CEO", {"since": 2000}),
    (6, 0, "CEO", {"since": 2001}),
    (7, 0, "CEO", {"since": 2010}),
    (8, 1, "CEO", {"since": 1990}),
    (9, 4, "CEO", {"since": 1999}),
    (9, 5, "CEO", {"since": 1999}),
    (6, 0, "FOUNDER", {}),
    (7, 1, "FOUNDER", {}),
    (8, 1, "FOUNDER", {}),
    (9, 4, "FOUNDER", {}),
    (9, 5, "FOUNDER", {}),
    (6, 10, "LOCATED", {}),
    (7, 11, "LOCATED", {}),
    (8, 10, "LOCATED", {}),
    (0, 6, "WORKS", {}),
    (1, 6, "WORKS", {}),
    (1, 7, "WORKS", {}),
    (2, 7, "WORKS", {}),
]


def load_grapharrow(path: Path) -> GraphArrowApplication:
    application = build_application(path)
    nodes = []
    for label, properties in NODES:
        _, node = application.use_cases.graph.create_node(labels=(label,), properties=properties)
        nodes.append(node)
    application.use_cases.graph.create_edges_bulk(
        table=pa.Table.from_pylist(
            [
                {
                    "src_id": nodes[source].node_id,
                    "dst_id": nodes[target].node_id,
                    "rel_type": relationship,
                    **properties,
                }
                for source, target, relationship, properties in EDGES
            ]
        )
    )
    return application


def fetch(backend: Any, tool: str, args: dict[str, Any]) -> dict[str, Any]:
    result = backend.call(tool, args)
    return dict(backend.call("fetch", {"from": result["handle"], "limit": 1000}))


ROLE_ARGS: dict[str, Any] = {
    "source_label": "Person",
    "target_label": "Company",
    "relationships": [
        {"relationship_type": "CEO", "direction": "in"},
        {"relationship_type": "FOUNDER", "direction": "in"},
    ],
}
SHARED_ARGS: dict[str, Any] = {
    "seed_label": "Person",
    "shared_label": "Company",
    "peer_label": "Person",
    "seed_relationship": {"relationship_type": "WORKS", "direction": "out"},
    "peer_relationship": {"relationship_type": "WORKS", "direction": "out"},
    "seed_filters": [{"var": "seed", "property": "name", "op": "eq", "value": "Alice"}],
}
