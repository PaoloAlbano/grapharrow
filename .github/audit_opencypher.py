"""Run the openCypher M23 scenarios against the GraphArrow query API."""

from __future__ import annotations

import argparse
import gc
import hashlib
import json
import math
import re
import signal
import tempfile
import time
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Value:
    kind: str
    data: Any = None


class HarnessError(Exception):
    pass


class CaseTimeout(Exception):
    pass


class ValueParser:
    """Parse the TCK value notation without executing arbitrary input."""

    def __init__(self, text: str):
        self.text, self.pos = text, 0

    def ws(self):
        while self.pos < len(self.text) and self.text[self.pos].isspace():
            self.pos += 1

    def eat(self, token):
        self.ws()
        if self.text.startswith(token, self.pos):
            self.pos += len(token)
            return True
        return False

    def need(self, token):
        if not self.eat(token):
            raise HarnessError(f"Expected {token!r} at {self.pos}: {self.text!r}")

    def name(self):
        self.ws()
        match = re.match(r"[a-zA-Z0-9$_]+", self.text[self.pos :])
        if not match:
            raise HarnessError(f"Expected symbolic name at {self.pos}: {self.text!r}")
        self.pos += len(match[0])
        return match[0]

    def value(self):
        self.ws()
        if self.eat("'"):
            chunks = []
            while self.pos < len(self.text):
                char = self.text[self.pos]
                self.pos += 1
                if char == "'":
                    return Value("string", "".join(chunks))
                if char == "\\":
                    if self.pos == len(self.text) or self.text[self.pos] not in "\\'":
                        raise HarnessError(f"Invalid TCK string escape: {self.text!r}")
                    char = self.text[self.pos]
                    self.pos += 1
                chunks.append(char)
            raise HarnessError("Unterminated string")
        if self.eat("("):
            labels = []
            while self.eat(":"):
                labels.append(self.name())
            props = self.value() if self.peek("{") else Value("map", ())
            self.need(")")
            return Value("node", (tuple(sorted(set(labels))), props))
        if self.eat("["):
            if self.eat(":"):
                kind = self.name()
                props = self.value() if self.peek("{") else Value("map", ())
                self.need("]")
                return Value("relationship", (kind, props))
            items = []
            if not self.eat("]"):
                while True:
                    items.append(self.value())
                    if self.eat("]"):
                        break
                    self.need(",")
            return Value("list", tuple(items))
        if self.eat("{"):
            pairs = []
            if not self.eat("}"):
                while True:
                    key = self.name()
                    self.need(":")
                    pairs.append((key, self.value()))
                    if self.eat("}"):
                        break
                    self.need(",")
            return Value("map", tuple(sorted(pairs)))
        if self.eat("<"):
            start = self.value()
            links = []
            while not self.eat(">"):
                backward = self.eat("<-")
                if not backward:
                    self.need("-")
                rel = self.value()
                self.need("-" if backward else "->")
                node = self.value()
                links.append((backward, rel, node))
            return Value("path", (start, tuple(links)))
        for token, kind, data in [
            ("null", "null", None),
            ("true", "bool", True),
            ("false", "bool", False),
            ("NaN", "nan", None),
            ("-Inf", "float", -math.inf),
            ("Inf", "float", math.inf),
        ]:
            if self.eat(token):
                return Value(kind, data)
        self.ws()
        match = re.match(
            r"-?(?:(?:\d+\.\d+|\.\d+)(?:[eE][+-]?\d+)?|\d+[eE][+-]?\d+|\d+)", self.text[self.pos :]
        )
        if not match:
            raise HarnessError(f"Unknown value at {self.pos}: {self.text!r}")
        self.pos += len(match[0])
        token = match[0]
        return (
            Value("float", float(token))
            if any(c in token for c in ".eE")
            else Value("int", int(token))
        )

    def peek(self, token):
        self.ws()
        return self.text.startswith(token, self.pos)

    def parse(self):
        value = self.value()
        self.ws()
        if self.pos != len(self.text):
            raise HarnessError(f"Trailing value input: {self.text!r}")
        return value


def parse_value(text):
    return ValueParser(text).parse()


def parameter(value):
    if value.kind in ("null", "bool", "int", "float", "string"):
        return value.data
    if value.kind == "nan":
        return math.nan
    if value.kind == "list":
        return [parameter(v) for v in value.data]
    if value.kind == "map":
        return {k: parameter(v) for k, v in value.data}
    raise HarnessError(f"Unsupported parameter {value.kind}")


def actual_value(value, *, entities=True):
    from grapharrow.use_cases.cypher_path import PathResult

    if entities and isinstance(value, PathResult):
        nodes = value["nodes"]
        links = tuple(
            (edge["source"] != left["id"], actual_value(edge), actual_value(right))
            for left, edge, right in zip(nodes[:-1], value["relationships"], nodes[1:], strict=True)
        )
        return Value("path", (actual_value(nodes[0]), links))
    if value is None:
        return Value("null")
    if isinstance(value, bool):
        return Value("bool", value)
    if isinstance(value, int):
        return Value("int", value)
    if isinstance(value, float):
        return Value("nan") if math.isnan(value) else Value("float", value)
    if isinstance(value, str):
        return Value("string", value)
    if isinstance(value, (list, tuple)):
        return Value("list", tuple(actual_value(v, entities=entities) for v in value))
    if isinstance(value, dict):
        if (
            entities
            and set(value) == {"id", "labels", "properties"}
            and isinstance(value["labels"], list)
            and isinstance(value["properties"], dict)
        ):
            return Value(
                "node",
                (
                    tuple(sorted(set(value["labels"]))),
                    actual_value(value["properties"], entities=False),
                ),
            )
        if (
            entities
            and set(value) == {"id", "type", "source", "target", "properties"}
            and isinstance(value["properties"], dict)
        ):
            return Value(
                "relationship", (value["type"], actual_value(value["properties"], entities=False))
            )
        return Value(
            "map", tuple(sorted((k, actual_value(v, entities=entities)) for k, v in value.items()))
        )
    return Value("unknown", repr(value))


def unordered_lists(value):
    if isinstance(value, Value):
        if value.kind == "list":
            return Value("list", tuple(sorted((unordered_lists(v) for v in value.data), key=repr)))
        return Value(value.kind, unordered_lists(value.data))
    if isinstance(value, tuple):
        return tuple(unordered_lists(v) for v in value)
    return value


def table(step):
    return [
        [cell["value"] for cell in row["cells"]] for row in step["argument"]["dataTable"]["rows"]
    ]


def snapshot(app):
    state = app.use_cases.graph.load_state()
    nodes = {n.node_id for n in state.nodes}
    edges = {r.edge_id for r in state.edges}
    labels = {label for n in state.nodes for label in n.labels}
    props = Counter(
        (owner, ident, k, actual_value(v, entities=False))
        for owner, records, field in [
            ("node", state.nodes, "node_id"),
            ("relationship", state.edges, "edge_id"),
        ]
        for entity in records
        for ident in [getattr(entity, field)]
        for k, v in entity.properties.items()
    )
    return nodes, edges, labels, props


def effects(before, after):
    out = {}
    for name, old, new in zip(
        ("nodes", "relationships", "labels", "properties"), before, after, strict=True
    ):
        out["+" + name] = sum((new - old).values()) if isinstance(new, Counter) else len(new - old)
        out["-" + name] = sum((old - new).values()) if isinstance(new, Counter) else len(old - new)
    return out


def seed_named_graph(app, name):
    """Import the exact upstream binary tree via the native fixture API."""
    from grapharrow.repositories import EdgeRecord, NodeRecord

    if name not in ("binary-tree-1", "binary-tree-2"):
        raise HarnessError(f"Unknown named graph {name}")
    names = (
        ["a"] + [f"b{i}" for i in range(1, 5)] + [f"c{i}{j}" for i in range(1, 5) for j in (1, 2)]
    )
    ids = {name: i + 1 for i, name in enumerate(names)}
    nodes = tuple(
        NodeRecord(
            ids[n],
            (
                "A"
                if n == "a"
                else "Y"
                if name == "binary-tree-2" and n.startswith("c") and n.endswith("2")
                else "X",
            ),
            {"name": n},
        )
        for n in names
    )
    links = [("a", f"b{i}", "KNOWS" if i <= 2 else "FOLLOWS") for i in range(1, 5)]
    links += [(f"b{i}", f"c{i}{j}", "FRIEND") for i in range(1, 5) for j in (1, 2)]
    links += [(f"b{i}", f"b{i % 4 + 1}", "FRIEND") for i in range(1, 5)]
    edges = tuple(EdgeRecord(i + 1, typ, ids[a], ids[b], {}) for i, (a, b, typ) in enumerate(links))
    app.use_cases.graph.publish_records(
        previous_state=app.use_cases.graph.load_state(), new_nodes=nodes, new_edges=edges
    )


def compare_result(result, step):
    text = step["text"]
    if text == "the result should be empty":
        # The upstream CypherValueRecords.empty has both empty headers and rows.
        if result.columns or result.rows:
            return {
                "kind": "result_mismatch",
                "expected": "empty columns and rows",
                "actual": repr(result),
            }
        return None
    rows = table(step)
    headers = tuple(rows[0])
    expected = [tuple(parse_value(v) for v in row) for row in rows[1:]]
    if tuple(result.columns) != headers:
        return {"kind": "header_mismatch", "expected": headers, "actual": result.columns}
    actual = [tuple(actual_value(row[h]) for h in headers) for row in result.rows]
    if "ignoring element order for lists" in text:
        expected = [tuple(unordered_lists(v) for v in row) for row in expected]
        actual = [tuple(unordered_lists(v) for v in row) for row in actual]
    equal = actual == expected if ", in order" in text else Counter(actual) == Counter(expected)
    if not equal:
        return {"kind": "result_mismatch", "expected": repr(expected), "actual": repr(actual)}
    return None


def run_case(case, timeout=10):
    from grapharrow.app import build_application
    from grapharrow.core.errors import GraphArrowError, QueryError

    start = time.monotonic()
    negative = any("should be raised" in s["text"] for s in case["steps"])
    record = {
        "id": case["case_id"],
        "feature": case["uri"],
        "name": case["name"],
        "negative": negative,
        "tags": [t["name"] for t in case["tags"]],
    }
    app = None
    stage = "setup"
    error = None
    result = None
    before = None
    unverified = False
    query = ""

    def finish(status, **detail):
        return dict(record, status=status, seconds=round(time.monotonic() - start, 6), **detail)

    def alarm(*_):
        raise CaseTimeout("Per-scenario time limit exceeded")

    previous = signal.signal(signal.SIGALRM, alarm)
    signal.setitimer(signal.ITIMER_REAL, timeout)
    try:
        with tempfile.TemporaryDirectory(prefix="grapharrow-tck-") as path:
            app = build_application(Path(path) / "db")
            app.use_cases.admin.initialize()
            params = {}
            for step in case["steps"]:
                text = step["text"]
                if text == "an empty graph":
                    continue
                if text == "any graph":
                    seed_named_graph(app, "binary-tree-1")
                    continue
                named = re.fullmatch(r"the (.+) graph", text)
                if named:
                    seed_named_graph(app, named[1])
                    continue
                if text.startswith("there exists a procedure "):
                    return finish("unsupported_procedure_fixture", step=text)
                if text == "parameters are:":
                    params = {k: parameter(parse_value(v)) for k, v in table(step)}
                    continue
                if text in ("having executed:", "executing query:", "executing control query:"):
                    query = step["argument"]["docString"]["content"]
                    stage = "setup" if text == "having executed:" else "query"
                    if text == "executing query:":
                        before = snapshot(app)
                    result = None
                    error = None
                    try:
                        result = app.use_cases.cypher.execute(query, params)
                    except (CaseTimeout, HarnessError):
                        raise
                    except Exception as exc:
                        error = exc
                        if stage == "setup":
                            return finish(
                                "blocked_setup",
                                query=query,
                                error_type=type(exc).__name__,
                                error=str(exc),
                            )
                    continue
                if "should be raised" in text:
                    if error is None:
                        return finish(
                            "invalid_query_accepted",
                            query=query,
                            expected_error=text,
                            actual=repr(result),
                        )
                    changed = {k: v for k, v in effects(before, snapshot(app)).items() if v}
                    if changed:
                        return finish(
                            "error_side_effects", query=query, error=str(error), effects=changed
                        )
                    if not isinstance(error, GraphArrowError):
                        return finish(
                            "internal_error",
                            query=query,
                            error_type=type(error).__name__,
                            error=str(error),
                            expected_error=text,
                        )
                    expected = re.fullmatch(
                        r"(?:a|an) (\w+) should be raised at (compile time|runtime): (\w+)", text
                    )
                    record.update(
                        expected_error=text,
                        error_type=type(error).__name__,
                        error=str(error),
                        query=query,
                    )
                    if not isinstance(error, QueryError) or not expected:
                        unverified = True
                        continue
                    record["error_metadata"] = error.metadata()
                    if (
                        error.category in {"QueryError", "UnsupportedFeature"}
                        or error.detail == "Unclassified"
                    ):
                        unverified = True
                        continue
                    category, phase, detail = expected.groups()
                    if (category, phase.removesuffix(" time"), detail) != (
                        error.category,
                        error.phase,
                        error.detail,
                    ):
                        return finish("error_mismatch")
                    continue
                if text.startswith("the result should be"):
                    if error is not None:
                        return finish(
                            "query_rejected"
                            if isinstance(error, GraphArrowError)
                            else "internal_error",
                            query=query,
                            error_type=type(error).__name__,
                            error=str(error),
                        )
                    mismatch = compare_result(result, step)
                    if mismatch:
                        status = mismatch.pop("kind")
                        return finish(status, query=query, **mismatch)
                    continue
                if text in ("no side effects", "the side effects should be:"):
                    expected = {k: 0 for k in effects(before, before)}
                    if text != "no side effects":
                        expected.update({k: int(v) for k, v in table(step)})
                    observed = effects(before, snapshot(app))
                    if expected != observed:
                        return finish(
                            "side_effect_mismatch", query=query, expected=expected, actual=observed
                        )
                    continue
                raise HarnessError(f"Unsupported step {text}")
            return finish(
                "negative_rejected_unverified"
                if unverified
                else "negative_passed"
                if negative
                else "passed"
            )
    except CaseTimeout as exc:
        return finish("timeout", stage=stage, query=query, error=str(exc))
    except Exception as exc:
        return finish(
            "harness_error", stage=stage, query=query, error_type=type(exc).__name__, error=str(exc)
        )
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def ast_lines(item: object, locations: dict[str, int]) -> None:
    if isinstance(item, dict):
        if "id" in item and "location" in item:
            locations[item["id"]] = item["location"]["line"]
        for value in item.values():
            ast_lines(value, locations)
    elif isinstance(item, list):
        for value in item:
            ast_lines(value, locations)


def load_cases(root):
    from gherkin.parser import Parser
    from gherkin.pickles.compiler import Compiler

    cases = []
    for path in sorted(root.rglob("*.feature")):
        doc = Parser().parse(path.read_text())
        doc["uri"] = str(path.relative_to(root))
        locations: dict[str, int] = {}
        ast_lines(doc, locations)
        for case in Compiler().compile(doc):
            if any(tag["name"] == "@ignore" for tag in case["tags"]):
                continue
            case["case_id"] = (
                case["uri"] + ":" + ":".join(str(locations[i]) for i in case["astNodeIds"])
            )
            cases.append(case)
    return cases


def preflight(cases):
    errors = []
    for case in cases:
        for step in case["steps"]:
            text = step["text"]
            if "dataTable" not in step.get("argument", {}):
                continue
            values = []
            if text.startswith("the result should be"):
                values = [v for row in table(step)[1:] for v in row]
            elif text == "parameters are:":
                values = [row[1] for row in table(step)]
            for value in values:
                try:
                    parse_value(value)
                except HarnessError as exc:
                    errors.append({"id": case["case_id"], "value": value, "error": str(exc)})
    return errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--features", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--match", default="")
    parser.add_argument("--limit", type=int)
    parser.add_argument("--timeout", type=float, default=10)
    parser.add_argument("--preflight-only", action="store_true")
    args = parser.parse_args()
    cases = [c for c in load_cases(args.features) if args.match in c["case_id"]][: args.limit]
    if not cases:
        parser.error("No scenarios selected. Check the features directory and filters.")
    errors = preflight(cases)
    print(
        json.dumps(
            {
                "cases": len(cases),
                "negative": sum(
                    any("should be raised" in s["text"] for s in c["steps"]) for c in cases
                ),
                "value_parse_errors": len(errors),
                "examples": errors[:5],
            }
        ),
        flush=True,
    )
    if errors:
        raise SystemExit("Fix harness value parsing before measuring coverage")
    if args.preflight_only:
        return
    args.output.parent.mkdir(parents=True, exist_ok=True)
    counts = Counter()
    categories = {}
    start = time.monotonic()
    with args.output.open("w") as stream:
        for i, case in enumerate(cases, 1):
            result = run_case(case, args.timeout)
            stream.write(json.dumps(result, ensure_ascii=False, default=str) + "\n")
            stream.flush()
            counts[result["status"]] += 1
            category = "/".join(case["uri"].split("/")[:2])
            categories.setdefault(category, Counter())[result["status"]] += 1
            if i % 100 == 0 or i == len(cases):
                print(
                    json.dumps(
                        {
                            "completed": i,
                            "seconds": round(time.monotonic() - start, 2),
                            "statuses": counts,
                        }
                    ),
                    flush=True,
                )
                gc.collect()
    summary = {
        "total": len(cases),
        "seconds": round(time.monotonic() - start, 3),
        "statuses": counts,
        "categories": categories,
        "features_sha256": hashlib.sha256(
            b"".join(
                str(p.relative_to(args.features)).encode() + p.read_bytes()
                for p in sorted(args.features.rglob("*.feature"))
            )
        ).hexdigest(),
    }
    args.output.with_suffix(".summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
