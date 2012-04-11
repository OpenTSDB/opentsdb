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

@members {
}
 
/*-----------------------------------------------------------------------------
 * PARSER RULES
 *-----------------------------------------------------------------------------
 */

parse: expr;

expr: term ((ADD^ | SUBTRACT^) term)*;
 
term: factor ((MULTIPLY^ | DIVIDE^) factor)*;

factor: METRIC | LEFT_PARENTHESIS! expr RIGHT_PARENTHESIS!;
 
 
/*-----------------------------------------------------------------------------
 * LEXER RULES
 *-----------------------------------------------------------------------------
 */
 
METRIC : Letter (Letter|Digit|Separator)*;
 
WHITESPACE : ( '\t' | ' ' | '\r' | '\n'| '\u000C' )+ { $channel = HIDDEN; };
 
fragment Letter: 'A'..'Z' | 'a'..'z';
fragment Digit: '0'..'9';
fragment Separator: '.' | '-' | ':' | '{' | '}' | '=' | ',';