grammar MetricArithmeticExpression;

options {
    output = AST;
}

tokens {
    ADD = '+';
    SUBTRACT = '-';
    MULTIPLY = '*';
    DIVIDE = '/';
    LEFT_PARENTHESIS = '(';
    RIGHT_PARENTHESIS = ')';
}

@parser::header { package net.opentsdb.expression; }
@lexer::header { package net.opentsdb.expression; }
 
/*-----------------------------------------------------------------------------
 * PARSER RULES
 *-----------------------------------------------------------------------------
 */

parse: expr;

expr: term ((ADD^ | SUBTRACT^) term)*;

term: factor ((MULTIPLY^ | DIVIDE^) factor)*;

factor: LITERAL | METRIC | function | (LEFT_PARENTHESIS! expr RIGHT_PARENTHESIS!);

function: FUNCTION_NAME^ LEFT_PARENTHESIS! expr (PARAM_SEPARATOR! expr)* RIGHT_PARENTHESIS!;

/*-----------------------------------------------------------------------------
 * LEXER RULES
 *-----------------------------------------------------------------------------
 */

IDENTIFIER : Letter (Letter|Digit|Separator)*;

LITERAL : Digit+ ('.' Digit+)?;

METRIC : IDENTIFIER (':' IDENTIFIER)* ('{' IDENTIFIER '=' (IDENTIFIER | '\*') (',' IDENTIFIER '=' (IDENTIFIER | '\*'))* '}')?;

FUNCTION_NAME : '_' IDENTIFIER; 

PARAM_SEPARATOR : ',';
 
WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ { $channel = HIDDEN; };

fragment Letter: 'A'..'Z' | 'a'..'z';
fragment Digit: '0'..'9';
fragment Separator: '.' | '-' | '\*';
