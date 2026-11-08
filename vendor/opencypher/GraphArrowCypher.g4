/**
 * GraphArrow implementation extensions to Cypher.
 *
 * Imports the unchanged Apache-2.0 openCypher grammar in Cypher.g4. These
 * extensions implement read subqueries and do not claim openCypher approval.
 */
grammar GraphArrowCypher;

import Cypher;

oC_ReadingClause
    : oC_Match | oC_Unwind | oC_InQueryCall | oC_SubqueryCall
    ;

oC_SubqueryCall
    : (OPTIONAL SP)? CALL SP? oC_SubqueryScope? SP? '{' SP? oC_RegularQuery SP? '}'
    ;

oC_SubqueryScope
    : '(' SP? ('*' | oC_Variable (SP? ',' SP? oC_Variable)*)? SP? ')'
    ;

oC_ExistentialSubquery
    : EXISTS SP? '{' SP?
      (oC_RegularQuery | oC_ExistentialBody | (oC_Pattern (SP? oC_Where)?))
      SP? '}'
    ;

oC_ExistentialBody
    : (oC_ReadingClause | oC_With) (SP? (oC_ReadingClause | oC_With))*
    ;
