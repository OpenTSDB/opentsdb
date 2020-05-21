// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
grammar MetricExpression;

prog: expression;

expression : arthmeticExpression #arithmetic
           | logicalExpression #logical
           | relationalExpression #relational
           | ternaryExpression   #ternary
           ;

logicalExpression : '(' logicalExpression ')'                  #paren_logical_rule
                    | not logicalExpression                    #logical_expr_not_rule
                    | logicalExpression and logicalExpression  #logical_expr_and_rule
                    | logicalExpression or logicalExpression   #logical_expr_or_rule
                    | logicalOperands                          #logical_operands_rule
           ;


logicalOperands : relationalExpression | TRUE | FALSE
           ;

arthmeticExpression : '(' arthmeticExpression ')'                       #paren_arith_rule
                    |  arthmeticExpression '%' arthmeticExpression      #mod_arith_rule
                    | arthmeticExpression ('/'|'*') arthmeticExpression #divmul_arith_rule
                    | arthmeticExpression ('-'|'+') arthmeticExpression #addsub_arith_rule
                    | '-'metric                                         #minus_metric_rule
                    | arithmeticOperands                                #arith_operands_rule
           ;

arithmeticOperands : (metric|NUM)   #arithmetic_operands_rule
        ;


relationalExpression : '(' relationalExpression ')'                         #paren_relational_rule
                      |  relationalOperands relationalop relationalOperands #main_relational_rule
            ;

relationalOperands : (arthmeticExpression | TRUE | FALSE)  #relational_operands_rule
            ;

logicop: and | or | not ;

relationalop: '<'|'>'|'=='|'<='|'>='|'!=';

ternaryExpression : '(' ternaryExpression ')'                                                   #paren_ternary_rule
                  | relationalExpression '?'  ternaryOperands ':' ternaryOperands       #main_ternary_rule
                  ;

ternaryOperands:  arthmeticExpression | logicalExpression              // We can label alertnatives - but seems to me like the output will have a single type
                ;


and : A N D | '&&'

    ;

or  : O R | '||'
    ;

not : N O T | '!'
    ;

TRUE : T R U E;
FALSE: F A L S E;

modulo : '%' ;

metric: REX;

REX : [a-zA-Z_0-9]+[.a-zA-Z_\-0-9]* ;


QUOTE : '("|\')';


ID : [a-zA-Z_]+[a-zA-Z_\-0-9]*;
NUM :  [-]?[0-9]+[.]?[0-9]* ;

WS  :   [ \t]+ -> skip;
BACKSLASH : [\\ \n]+ -> skip;

fragment A:('a'|'A');
fragment B:('b'|'B');
fragment C:('c'|'C');
fragment D:('d'|'D');
fragment E:('e'|'E');
fragment F:('f'|'F');
fragment G:('g'|'G');
fragment H:('h'|'H');
fragment I:('i'|'I');
fragment J:('j'|'J');
fragment K:('k'|'K');
fragment L:('l'|'L');
fragment M:('m'|'M');
fragment N:('n'|'N');
fragment O:('o'|'O');
fragment P:('p'|'P');
fragment Q:('q'|'Q');
fragment R:('r'|'R');
fragment S:('s'|'S');
fragment T:('t'|'T');
fragment U:('u'|'U');
fragment V:('v'|'V');
fragment W:('w'|'W');
fragment X:('x'|'X');
fragment Y:('y'|'Y');
fragment Z:('z'|'Z');
