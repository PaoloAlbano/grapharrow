"""Scope and operand checks shared by the legacy Cypher execution paths.

Validation reads syntax and parameter types only. It completes before graph
mutation, including when a preceding MATCH would produce no rows.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from grapharrow.core.errors import QueryError
from grapharrow.use_cases.cypher_expression import children, identifier, rule


def _walk(node: Any) -> Any:
    yield node
    for child in children(node):
        yield from _walk(child)


def _kind(value: object) -> str:
    if value is None:
        return "null"
    if type(value) is bool:
        return "bool"
    if isinstance(value, int | float):
        return "number"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "map"
    if isinstance(value, list | tuple):
        return "list"
    return "any"


class LegacyValidator:
    def __init__(self, parameters: Mapping[str, object]) -> None:
        self.parameters = parameters

    def expression(self, node: Any, scope: dict[str, str], *, predicate: bool = False) -> str:
        name = rule(node)
        parts = children(node)
        if name == "oC_Variable":
            variable = identifier(node.getText())
            if variable not in scope:
                raise QueryError.syntax(f"Unknown variable: {variable}", "UndefinedVariable")
            return scope[variable]
        if name == "oC_Parameter":
            parameter = identifier(node.getText()[1:])
            if parameter not in self.parameters:
                raise QueryError.missing_parameter(parameter)
            return _kind(self.parameters[parameter])
        if name == "oC_ExistentialSubquery":
            local = self.pattern(node.oC_Pattern(), scope, creating=False)
            if node.oC_Where() is not None:
                self.expression(node.oC_Where().oC_Expression(), local, predicate=True)
            return "bool"
        if name in {"oC_RelationshipsPattern", "oC_PatternPredicate"}:
            if not predicate:
                raise QueryError("Pattern predicates are only supported in predicate contexts.")
            for part in _walk(node):
                if rule(part) == "oC_Variable":
                    self.expression(part, scope)
            return "bool"
        if name in {"oC_ListComprehension", "oC_Quantifier"}:
            filtering = node.oC_FilterExpression()
            binding = filtering.oC_IdInColl()
            self.expression(binding.oC_Expression(), scope)
            local = scope | {identifier(binding.oC_Variable().getText()): "any"}
            where = filtering.oC_Where()
            if where is not None:
                self.expression(where.oC_Expression(), local, predicate=True)
            projection = node.oC_Expression() if name == "oC_ListComprehension" else None
            if projection is not None:
                self.expression(projection, local)
            return "list" if name == "oC_ListComprehension" else "bool"
        if name in {"oC_PropertyKeyName", "oC_PropertyLookup", "oC_NodeLabels"}:
            return "any"
        if name == "oC_FunctionInvocation":
            function = node.oC_FunctionName().getText().lower()
            operands = [self.expression(p, scope) for p in node.oC_Expression()]
            allowed = {
                "id": {"node", "relationship"},
                "labels": {"node"},
                "type": {"relationship"},
                "keys": {"node", "relationship", "map"},
                "abs": {"number"},
                "tolower": {"string"},
                "toupper": {"string"},
                "size": {"list", "string"},
            }
            if function in allowed and (
                len(operands) != 1 or operands[0] not in allowed[function] | {"any", "null"}
            ):
                raise QueryError.invalid_type(f"Invalid operand for {function}", phase="compile")
            return "list" if function in {"collect", "labels", "keys"} else "any"
        kinds = [self.expression(p, scope, predicate=predicate) for p in parts]
        if name == "oC_NonArithmeticOperatorExpression" and len(parts) > 1:
            if node.oC_PropertyLookup() and kinds[0] not in {
                "node",
                "relationship",
                "map",
                "any",
                "null",
            }:
                raise QueryError.invalid_type(
                    "Property access requires a node, relationship or map", phase="compile"
                )
            return "any"
        if name == "oC_ListLiteral":
            return "list"
        if name == "oC_MapLiteral":
            return "map"
        if name in {"oC_AndExpression", "oC_OrExpression", "oC_XorExpression"} and len(parts) > 1:
            if any(kind not in {"bool", "null", "any"} for kind in kinds):
                raise QueryError.invalid_type(
                    "Boolean operators require boolean or null operands", phase="compile"
                )
            return "bool"
        if name == "oC_NotExpression" and node.NOT():
            if kinds[0] not in {"bool", "null", "any"}:
                raise QueryError.invalid_type(
                    "NOT requires a boolean or null operand", phase="compile"
                )
            return "bool"
        if name == "oC_ComparisonExpression" and len(parts) > 1:
            return "bool"
        if name == "oC_StringListNullPredicateExpression" and len(parts) > 1:
            return "bool"
        if name in {
            "oC_AddOrSubtractExpression",
            "oC_MultiplyDivideModuloExpression",
            "oC_PowerOfExpression",
            "oC_UnaryAddOrSubtractExpression",
        }:
            if len(parts) > 1 or node.getText().startswith(("+", "-")):
                return "number" if all(k == "number" for k in kinds) else "any"
        if len(parts) == 1:
            return kinds[0]
        if not parts:
            from grapharrow.use_cases.cypher import _parse_value

            try:
                return _kind(_parse_value(node.getText(), dict(self.parameters)))
            except QueryError:
                return "any"
        return "any"

    def pattern(self, node: Any, scope: dict[str, str], *, creating: bool) -> dict[str, str]:
        visible = dict(scope)
        properties = []
        relationships: set[str] = set()
        for part in _walk(node):
            name = rule(part)
            if name not in {"oC_NodePattern", "oC_RelationshipDetail"}:
                continue
            variable = part.oC_Variable()
            if variable is not None:
                key = identifier(variable.getText())
                kind = "node" if name == "oC_NodePattern" else "relationship"
                if name == "oC_RelationshipDetail":
                    if key in relationships:
                        raise QueryError.syntax(
                            "A relationship variable cannot be repeated in one MATCH pattern.",
                            "RelationshipUniquenessViolation",
                        )
                    relationships.add(key)
                if name == "oC_RelationshipDetail" and part.oC_RangeLiteral() is not None:
                    kind = "list"
                if key in visible and visible[key] not in {kind, "any"}:
                    raise QueryError.syntax(
                        f"Variable {key} cannot be used as both {visible[key]} and {kind}",
                        "VariableTypeConflict",
                    )
                visible[key] = kind
            value = part.oC_Properties()
            if value is not None:
                properties.append(value)
        for value in properties:
            self.expression(value, scope if creating else visible)
        return visible

    def projection(self, node: Any, scope: dict[str, str], *, terminal: bool) -> dict[str, str]:
        body = node.oC_ProjectionBody()
        items = body.oC_ProjectionItems()
        output = dict(scope) if items.getText().startswith("*") else {}
        if items.getText().startswith("*") and not scope:
            raise QueryError.syntax(
                "A star projection requires visible bindings", "NoVariablesInScope"
            )
        for item in items.oC_ProjectionItem():
            expression = item.oC_Expression()
            kind = self.expression(expression, scope)
            alias = item.oC_Variable()
            text = identifier(expression.getText())
            if not terminal and alias is None and text not in scope:
                raise QueryError.syntax(
                    "WITH expressions require an explicit alias", "NoExpressionAlias"
                )
            output[identifier(alias.getText()) if alias is not None else text] = kind
        where = node.oC_Where() if not terminal else None
        if where is not None:
            self.expression(where.oC_Expression(), scope | output, predicate=True)
        order = body.oC_Order()
        if order is not None:
            for item in order.oC_SortItem():
                self.expression(item.oC_Expression(), scope | output)
        return output

    def validate(self, tree: Any) -> None:
        regular = tree.oC_Statement().oC_Query().oC_RegularQuery()
        if regular is None:
            return
        for single in [regular.oC_SingleQuery(), *(u.oC_SingleQuery() for u in regular.oC_Union())]:
            multi = single.oC_MultiPartQuery()
            parts = children(multi) if multi is not None else [single.oC_SinglePartQuery()]
            ordered = []
            for part in parts:
                ordered.extend(children(part) if rule(part) == "oC_SinglePartQuery" else [part])
            scope: dict[str, str] = {}
            for clause in ordered:
                name = rule(clause)
                if name in {"oC_With", "oC_Return"}:
                    scope = self.projection(clause, scope, terminal=name == "oC_Return")
                elif name == "oC_ReadingClause":
                    match, unwind = clause.oC_Match(), clause.oC_Unwind()
                    if match is not None:
                        scope = self.pattern(match.oC_Pattern(), scope, creating=False)
                        if match.oC_Where() is not None:
                            self.expression(match.oC_Where().oC_Expression(), scope, predicate=True)
                    elif unwind is not None:
                        self.expression(unwind.oC_Expression(), scope)
                        scope[identifier(unwind.oC_Variable().getText())] = "any"
                elif name == "oC_UpdatingClause":
                    create, merge = clause.oC_Create(), clause.oC_Merge()
                    if create is not None or merge is not None:
                        update = create if create is not None else merge
                        pattern = (
                            create.oC_Pattern() if create is not None else update.oC_PatternPart()
                        )
                        scope = self.pattern(pattern, scope, creating=True)
                        if merge is not None:
                            for action in merge.oC_MergeAction():
                                self.expression(action.oC_Set(), scope)
                    else:
                        self.expression(clause, scope)
