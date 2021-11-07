grammar NumericExpression;

fragment DIGIT : [0-9] ;

NUMBER: MINUS? (DIGIT+('.' DIGIT+)?| DIGIT* '.' DIGIT+);

PLUS:'+';
MINUS:'-';
DIVIDE: '/';
MULTIPLY: '*';

fragment LETTER
    : [a-zA-Z$_] // these are the "java letters" below 0x7F
    | ~[\u0000-\u007F\uD800-\uDBFF] // covers all characters above 0x7F which are not a surrogate
    | [\uD800-\uDBFF] [\uDC00-\uDFFF] // covers UTF-16 surrogate pairs encodings for U+10000 to U+10FFFF
    ;

fragment LETTERORDIGIT
    : LETTER
    | DIGIT
    ;

IDENTIFIER: LETTER LETTERORDIGIT*;

WS: [ \t\r\n\u000C]+ -> channel(HIDDEN);

expression: body EOF;
body
    : body (DIVIDE| MULTIPLY) body
    | body (PLUS | MINUS) body
    | '('body')'
    | identWithDot
    | NUMBER;
identWithDot: '_'?IDENTIFIER('.'IDENTIFIER)*;
