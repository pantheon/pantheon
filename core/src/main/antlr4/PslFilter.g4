grammar PslFilter;

fragment DIGIT : [0-9] ;

fragment LETTER
    : [A-Za-z$_-]
    | [\u0080-\uD7FF] // support subset of characters in BMP (no surrogates or private use, still, some of these may not be printable)
    ;
fragment LETTERORDIGIT
    : LETTER
    | DIGIT
    ;
fragment NULL : 'null';
fragment TIMENUMOFFSET : ('+' | '-') HOUR ':' MINUTE;
fragment TIMEOFFSET: 'Z' | TIMENUMOFFSET;
fragment PARTIALTIME : HOUR ':' MINUTE ':' SECOND SECFRAC?;
fragment HOUR : ([0-1][0-9])| ('2'[0-4]);
fragment MINUTE : ([0-5][0-9]);
fragment SECOND: ([0-5][0-9])|'60';
fragment SECFRAC : '.' DIGIT+;
fragment DATE_: DATEYEAR'-'DATEMONTH'-'DATEDAY;
fragment Q2STRINGSYMBOL: ~["];
// Q2STRINGSYMBOL at the end to disambiguate between empty Q2 string and closing trippleqote
fragment Q2STRING_LITERAL: '"' Q2STRINGSYMBOL* '"';
fragment Q3STRING_LITERAL: '""""""'|'"""'(Q2STRING_LITERAL | Q2STRINGSYMBOL)*? Q2STRINGSYMBOL'"""' ;
fragment DATEYEAR : DIGIT DIGIT DIGIT DIGIT;
fragment DATEMONTH : ('0' [1-9]) |('1' [0-2]);
fragment DATEDAY  : ('0'[1-9])| ([1-2] [0-9])|('3' [0,1]);

BOOL: 'true' | 'false';

NUMBER
    : DIGIT+('.' DIGIT+)?
    | DIGIT*'.' DIGIT+
    ;

TIMESTAMP: 'timestamp' ' '+ '\'' DATE_ ' ' PARTIALTIME TIMEOFFSET? '\'';
DATE: 'date' ' '+ '\'' DATE_ '\'';


// need these labels as they are transformed into parser tree methods(safer than pattern match on strings), bare tokens used in parsers are not mapped to methods
EQ: '=';
GT: '>';
GTEQ: '>=';
LT: '<';
LTEQ: '<=';
NEQ: '!=';
IsNull: 'is' ' '+ NULL;
IsNotNull: 'is' ' '+ 'not' ' '+ NULL;

NOTIN: 'not' ' '+ 'in';
IN: 'in';
LIKE: 'like';

AND: 'and';
OR: 'or';

LINE_COMMENT:   '//' ~[\r\n]* -> channel(HIDDEN);
BLOCK_COMMENT:   '/*' .*? '*/' -> channel(HIDDEN);
WS  :  [ \t\r\n\u000C]+ -> channel(HIDDEN);

SINGLEQUOTEDSTRINGLIT: '\''(~[']| ('\'\''))*'\'';

CHARS_WITH_DOT: LETTERORDIGIT+ ('.'LETTERORDIGIT+)+;
LETTER_CHARS: LETTER+;

ERRORCHARACTER : . ;


filter: expr EOF;

expr : expr AND expr
     | expr OR expr
     | op
     | '('expr')';

op: identifier (binaryOperation | multivalOperation | unaryOperation);
identifier: CHARS_WITH_DOT | LETTER_CHARS;

binaryOperation: binaryOperator value;
binaryOperator: EQ | GT | GTEQ | LT | LTEQ | NEQ;
value: queryParam | DATE | TIMESTAMP | BOOL | NUMBER | SINGLEQUOTEDSTRINGLIT;

multivalOperation: multivalOperator multival;
multivalOperator: IN | NOTIN | LIKE;
multival: '(' (multiNumber | multiString| multiBool |multiDate | multiTimeStamp)')';
// These patterns encode a list of items containing a possible null value
multiDate: DATE (',' DATE) *;
multiTimeStamp: TIMESTAMP  (',' TIMESTAMP )* ;
multiBool: BOOL (',' BOOL)* ;
multiNumber: NUMBER (',' NUMBER)* ;
multiString: SINGLEQUOTEDSTRINGLIT (',' SINGLEQUOTEDSTRINGLIT)* ;

unaryOperation: IsNull | IsNotNull ;

queryParam: ':'LETTER_CHARS;
