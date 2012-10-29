tree grammar MetricArithmeticExpressionTreeWalker;

options {
    tokenVocab=MetricArithmeticExpression;
    ASTLabelType=CommonTree;
}

@header { package net.opentsdb.expression; }

/*-----------------------------------------------------------------------------
 * PARSER RULES
 *-----------------------------------------------------------------------------
 */

parse returns[ArithmeticNode node]: e = expr { $node = $e.node; };

expr returns[ArithmeticNode node]
        : ^(ADD a=expr b=expr ) { node = new OperatorNode(a, Operator.ADD, b); }
        | ^(SUBTRACT a=expr b=expr ) { node = new OperatorNode(a, Operator.SUBTRACT, b); }
        | ^(MULTIPLY a=expr b=expr ) { node = new OperatorNode(a, Operator.MULTIPLY, b); }
        | ^(DIVIDE a=expr b=expr ) { node = new OperatorNode(a, Operator.DIVIDE, b); }
        | ^(IDENTIFIER p=params ) { node = new FunctionNode($IDENTIFIER.text, p); }
        | metric = METRIC { node = new MetricNode($metric.text); }
        | literal = LITERAL { node = new LiteralNode($literal.text); }
        | (PARAM_SEPARATOR) { }
        ;
        
params returns[List result]
        @init { $result = new ArrayList(); }
        : (e=expr { $result.add(e); })+
        ; 
