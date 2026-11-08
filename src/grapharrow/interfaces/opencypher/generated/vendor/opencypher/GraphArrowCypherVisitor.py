# Generated from vendor/opencypher/GraphArrowCypher.g4 by ANTLR 4.13.2
from antlr4 import *
if "." in __name__:
    from .GraphArrowCypherParser import GraphArrowCypherParser
else:
    from GraphArrowCypherParser import GraphArrowCypherParser

# This class defines a complete generic visitor for a parse tree produced by GraphArrowCypherParser.

class GraphArrowCypherVisitor(ParseTreeVisitor):

    # Visit a parse tree produced by GraphArrowCypherParser#oC_ReadingClause.
    def visitOC_ReadingClause(self, ctx:GraphArrowCypherParser.OC_ReadingClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SubqueryCall.
    def visitOC_SubqueryCall(self, ctx:GraphArrowCypherParser.OC_SubqueryCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SubqueryScope.
    def visitOC_SubqueryScope(self, ctx:GraphArrowCypherParser.OC_SubqueryScopeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ExistentialSubquery.
    def visitOC_ExistentialSubquery(self, ctx:GraphArrowCypherParser.OC_ExistentialSubqueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ExistentialBody.
    def visitOC_ExistentialBody(self, ctx:GraphArrowCypherParser.OC_ExistentialBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Cypher.
    def visitOC_Cypher(self, ctx:GraphArrowCypherParser.OC_CypherContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Statement.
    def visitOC_Statement(self, ctx:GraphArrowCypherParser.OC_StatementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Query.
    def visitOC_Query(self, ctx:GraphArrowCypherParser.OC_QueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RegularQuery.
    def visitOC_RegularQuery(self, ctx:GraphArrowCypherParser.OC_RegularQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Union.
    def visitOC_Union(self, ctx:GraphArrowCypherParser.OC_UnionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SingleQuery.
    def visitOC_SingleQuery(self, ctx:GraphArrowCypherParser.OC_SingleQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SinglePartQuery.
    def visitOC_SinglePartQuery(self, ctx:GraphArrowCypherParser.OC_SinglePartQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_MultiPartQuery.
    def visitOC_MultiPartQuery(self, ctx:GraphArrowCypherParser.OC_MultiPartQueryContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_UpdatingClause.
    def visitOC_UpdatingClause(self, ctx:GraphArrowCypherParser.OC_UpdatingClauseContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Match.
    def visitOC_Match(self, ctx:GraphArrowCypherParser.OC_MatchContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Unwind.
    def visitOC_Unwind(self, ctx:GraphArrowCypherParser.OC_UnwindContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Merge.
    def visitOC_Merge(self, ctx:GraphArrowCypherParser.OC_MergeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_MergeAction.
    def visitOC_MergeAction(self, ctx:GraphArrowCypherParser.OC_MergeActionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Create.
    def visitOC_Create(self, ctx:GraphArrowCypherParser.OC_CreateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Set.
    def visitOC_Set(self, ctx:GraphArrowCypherParser.OC_SetContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SetItem.
    def visitOC_SetItem(self, ctx:GraphArrowCypherParser.OC_SetItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Delete.
    def visitOC_Delete(self, ctx:GraphArrowCypherParser.OC_DeleteContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Remove.
    def visitOC_Remove(self, ctx:GraphArrowCypherParser.OC_RemoveContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RemoveItem.
    def visitOC_RemoveItem(self, ctx:GraphArrowCypherParser.OC_RemoveItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_InQueryCall.
    def visitOC_InQueryCall(self, ctx:GraphArrowCypherParser.OC_InQueryCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_StandaloneCall.
    def visitOC_StandaloneCall(self, ctx:GraphArrowCypherParser.OC_StandaloneCallContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_YieldItems.
    def visitOC_YieldItems(self, ctx:GraphArrowCypherParser.OC_YieldItemsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_YieldItem.
    def visitOC_YieldItem(self, ctx:GraphArrowCypherParser.OC_YieldItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_With.
    def visitOC_With(self, ctx:GraphArrowCypherParser.OC_WithContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Return.
    def visitOC_Return(self, ctx:GraphArrowCypherParser.OC_ReturnContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ProjectionBody.
    def visitOC_ProjectionBody(self, ctx:GraphArrowCypherParser.OC_ProjectionBodyContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ProjectionItems.
    def visitOC_ProjectionItems(self, ctx:GraphArrowCypherParser.OC_ProjectionItemsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ProjectionItem.
    def visitOC_ProjectionItem(self, ctx:GraphArrowCypherParser.OC_ProjectionItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Order.
    def visitOC_Order(self, ctx:GraphArrowCypherParser.OC_OrderContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Skip.
    def visitOC_Skip(self, ctx:GraphArrowCypherParser.OC_SkipContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Limit.
    def visitOC_Limit(self, ctx:GraphArrowCypherParser.OC_LimitContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SortItem.
    def visitOC_SortItem(self, ctx:GraphArrowCypherParser.OC_SortItemContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Where.
    def visitOC_Where(self, ctx:GraphArrowCypherParser.OC_WhereContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Pattern.
    def visitOC_Pattern(self, ctx:GraphArrowCypherParser.OC_PatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PatternPart.
    def visitOC_PatternPart(self, ctx:GraphArrowCypherParser.OC_PatternPartContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_AnonymousPatternPart.
    def visitOC_AnonymousPatternPart(self, ctx:GraphArrowCypherParser.OC_AnonymousPatternPartContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PatternElement.
    def visitOC_PatternElement(self, ctx:GraphArrowCypherParser.OC_PatternElementContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RelationshipsPattern.
    def visitOC_RelationshipsPattern(self, ctx:GraphArrowCypherParser.OC_RelationshipsPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NodePattern.
    def visitOC_NodePattern(self, ctx:GraphArrowCypherParser.OC_NodePatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PatternElementChain.
    def visitOC_PatternElementChain(self, ctx:GraphArrowCypherParser.OC_PatternElementChainContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RelationshipPattern.
    def visitOC_RelationshipPattern(self, ctx:GraphArrowCypherParser.OC_RelationshipPatternContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RelationshipDetail.
    def visitOC_RelationshipDetail(self, ctx:GraphArrowCypherParser.OC_RelationshipDetailContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Properties.
    def visitOC_Properties(self, ctx:GraphArrowCypherParser.OC_PropertiesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RelationshipTypes.
    def visitOC_RelationshipTypes(self, ctx:GraphArrowCypherParser.OC_RelationshipTypesContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NodeLabels.
    def visitOC_NodeLabels(self, ctx:GraphArrowCypherParser.OC_NodeLabelsContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NodeLabel.
    def visitOC_NodeLabel(self, ctx:GraphArrowCypherParser.OC_NodeLabelContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RangeLiteral.
    def visitOC_RangeLiteral(self, ctx:GraphArrowCypherParser.OC_RangeLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_LabelName.
    def visitOC_LabelName(self, ctx:GraphArrowCypherParser.OC_LabelNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RelTypeName.
    def visitOC_RelTypeName(self, ctx:GraphArrowCypherParser.OC_RelTypeNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PropertyExpression.
    def visitOC_PropertyExpression(self, ctx:GraphArrowCypherParser.OC_PropertyExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Expression.
    def visitOC_Expression(self, ctx:GraphArrowCypherParser.OC_ExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_OrExpression.
    def visitOC_OrExpression(self, ctx:GraphArrowCypherParser.OC_OrExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_XorExpression.
    def visitOC_XorExpression(self, ctx:GraphArrowCypherParser.OC_XorExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_AndExpression.
    def visitOC_AndExpression(self, ctx:GraphArrowCypherParser.OC_AndExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NotExpression.
    def visitOC_NotExpression(self, ctx:GraphArrowCypherParser.OC_NotExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ComparisonExpression.
    def visitOC_ComparisonExpression(self, ctx:GraphArrowCypherParser.OC_ComparisonExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PartialComparisonExpression.
    def visitOC_PartialComparisonExpression(self, ctx:GraphArrowCypherParser.OC_PartialComparisonExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_StringListNullPredicateExpression.
    def visitOC_StringListNullPredicateExpression(self, ctx:GraphArrowCypherParser.OC_StringListNullPredicateExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_StringPredicateExpression.
    def visitOC_StringPredicateExpression(self, ctx:GraphArrowCypherParser.OC_StringPredicateExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ListPredicateExpression.
    def visitOC_ListPredicateExpression(self, ctx:GraphArrowCypherParser.OC_ListPredicateExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NullPredicateExpression.
    def visitOC_NullPredicateExpression(self, ctx:GraphArrowCypherParser.OC_NullPredicateExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_AddOrSubtractExpression.
    def visitOC_AddOrSubtractExpression(self, ctx:GraphArrowCypherParser.OC_AddOrSubtractExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_MultiplyDivideModuloExpression.
    def visitOC_MultiplyDivideModuloExpression(self, ctx:GraphArrowCypherParser.OC_MultiplyDivideModuloExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PowerOfExpression.
    def visitOC_PowerOfExpression(self, ctx:GraphArrowCypherParser.OC_PowerOfExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_UnaryAddOrSubtractExpression.
    def visitOC_UnaryAddOrSubtractExpression(self, ctx:GraphArrowCypherParser.OC_UnaryAddOrSubtractExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NonArithmeticOperatorExpression.
    def visitOC_NonArithmeticOperatorExpression(self, ctx:GraphArrowCypherParser.OC_NonArithmeticOperatorExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ListOperatorExpression.
    def visitOC_ListOperatorExpression(self, ctx:GraphArrowCypherParser.OC_ListOperatorExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PropertyLookup.
    def visitOC_PropertyLookup(self, ctx:GraphArrowCypherParser.OC_PropertyLookupContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Atom.
    def visitOC_Atom(self, ctx:GraphArrowCypherParser.OC_AtomContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_CaseExpression.
    def visitOC_CaseExpression(self, ctx:GraphArrowCypherParser.OC_CaseExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_CaseAlternative.
    def visitOC_CaseAlternative(self, ctx:GraphArrowCypherParser.OC_CaseAlternativeContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ListComprehension.
    def visitOC_ListComprehension(self, ctx:GraphArrowCypherParser.OC_ListComprehensionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PatternComprehension.
    def visitOC_PatternComprehension(self, ctx:GraphArrowCypherParser.OC_PatternComprehensionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Quantifier.
    def visitOC_Quantifier(self, ctx:GraphArrowCypherParser.OC_QuantifierContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_FilterExpression.
    def visitOC_FilterExpression(self, ctx:GraphArrowCypherParser.OC_FilterExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PatternPredicate.
    def visitOC_PatternPredicate(self, ctx:GraphArrowCypherParser.OC_PatternPredicateContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ParenthesizedExpression.
    def visitOC_ParenthesizedExpression(self, ctx:GraphArrowCypherParser.OC_ParenthesizedExpressionContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_IdInColl.
    def visitOC_IdInColl(self, ctx:GraphArrowCypherParser.OC_IdInCollContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_FunctionInvocation.
    def visitOC_FunctionInvocation(self, ctx:GraphArrowCypherParser.OC_FunctionInvocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_FunctionName.
    def visitOC_FunctionName(self, ctx:GraphArrowCypherParser.OC_FunctionNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ExplicitProcedureInvocation.
    def visitOC_ExplicitProcedureInvocation(self, ctx:GraphArrowCypherParser.OC_ExplicitProcedureInvocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ImplicitProcedureInvocation.
    def visitOC_ImplicitProcedureInvocation(self, ctx:GraphArrowCypherParser.OC_ImplicitProcedureInvocationContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ProcedureResultField.
    def visitOC_ProcedureResultField(self, ctx:GraphArrowCypherParser.OC_ProcedureResultFieldContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ProcedureName.
    def visitOC_ProcedureName(self, ctx:GraphArrowCypherParser.OC_ProcedureNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Namespace.
    def visitOC_Namespace(self, ctx:GraphArrowCypherParser.OC_NamespaceContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Variable.
    def visitOC_Variable(self, ctx:GraphArrowCypherParser.OC_VariableContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Literal.
    def visitOC_Literal(self, ctx:GraphArrowCypherParser.OC_LiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_BooleanLiteral.
    def visitOC_BooleanLiteral(self, ctx:GraphArrowCypherParser.OC_BooleanLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_NumberLiteral.
    def visitOC_NumberLiteral(self, ctx:GraphArrowCypherParser.OC_NumberLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_IntegerLiteral.
    def visitOC_IntegerLiteral(self, ctx:GraphArrowCypherParser.OC_IntegerLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_DoubleLiteral.
    def visitOC_DoubleLiteral(self, ctx:GraphArrowCypherParser.OC_DoubleLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ListLiteral.
    def visitOC_ListLiteral(self, ctx:GraphArrowCypherParser.OC_ListLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_MapLiteral.
    def visitOC_MapLiteral(self, ctx:GraphArrowCypherParser.OC_MapLiteralContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_PropertyKeyName.
    def visitOC_PropertyKeyName(self, ctx:GraphArrowCypherParser.OC_PropertyKeyNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Parameter.
    def visitOC_Parameter(self, ctx:GraphArrowCypherParser.OC_ParameterContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SchemaName.
    def visitOC_SchemaName(self, ctx:GraphArrowCypherParser.OC_SchemaNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_ReservedWord.
    def visitOC_ReservedWord(self, ctx:GraphArrowCypherParser.OC_ReservedWordContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_SymbolicName.
    def visitOC_SymbolicName(self, ctx:GraphArrowCypherParser.OC_SymbolicNameContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_LeftArrowHead.
    def visitOC_LeftArrowHead(self, ctx:GraphArrowCypherParser.OC_LeftArrowHeadContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_RightArrowHead.
    def visitOC_RightArrowHead(self, ctx:GraphArrowCypherParser.OC_RightArrowHeadContext):
        return self.visitChildren(ctx)


    # Visit a parse tree produced by GraphArrowCypherParser#oC_Dash.
    def visitOC_Dash(self, ctx:GraphArrowCypherParser.OC_DashContext):
        return self.visitChildren(ctx)



del GraphArrowCypherParser