grammar Psl;


fragment DIGIT : [0-9] ;

fragment LETTER
    : [A-Za-z$_-]
    | [\u0080-\uD7FF] // support subset of characters in BMP (no surrogates or private use, still, some of these may not be printable)
    ;

fragment LETTER_OR_DIGIT : LETTER | DIGIT;

WILDCARDCOLUMN: 'columns' ' '+ '*';
BOOL: 'true' | 'false';

LINE_COMMENT:   '//' ~[\r\n]* -> channel(HIDDEN);
BLOCK_COMMENT:   '/*' .*? '*/' -> channel(HIDDEN);
WS  :  [ \t\r\n\u000C]+ -> channel(HIDDEN);

// Q3 string literal should have the same rules as in scala
fragment Q3STRING_LITERAL: '""""""'|'"""'('""'Q2STRINGSYMBOL)?('"'? Q2STRINGSYMBOL '"'?)*'"'*'"""' ;
fragment Q2STRING_LITERAL: '"' Q2STRINGSYMBOL* '"';
fragment Q2STRINGSYMBOL: '\\"' | ~'"' ;

STRINGLITERAL: Q3STRING_LITERAL | Q2STRING_LITERAL;

DECIMAL: DIGIT* '.' DIGIT+;

INTEGER: DIGIT+;

// MIXED_CHARS contains a DIGIT and a LETTER
MIXED_CHARS: (LETTER LETTER_OR_DIGIT* DIGIT LETTER*) | (DIGIT LETTER_OR_DIGIT* LETTER DIGIT*);

LETTER_CHARS: LETTER+;

ERRORCHARACTER : . ;

// Note: For consistency, all parser definitions should be declared BELOW their usage.
schema: 'schema' identifier schemaParams? schemaBody? EOF;
schemaParams: '(' (schemaParam (',' schemaParam)*)? ')' ;
schemaParam: dataSourceP | strictP;

/*
* The following rule may have looked like this: schemaBody: '{'schemaBodyItem* filter? schemaBodyItem*'}'.
* It detects that only one 'filter' entry is allowed in schema BUT
* it shows 'no viable alternative at input' in case if filter or other bodyItem is incomplete
* Current form fits better to UI autocompete needs.
* We need to show 'mismatched input '<EOF>' expecting IDENTIFIER' on incomplete but correct sentences
* ANYWAY there is no generic way to represent >1 unique items in a list with ANTLR grammar
* we use in-code validation for this in other places like 'measureParams', 'dimensionParams' etc.
*/
schemaBody:  '{' (measure | dimension | schemaImports | schemaExports | table | filter)* '}';

measure: 'measure' identifier measureParams? measureBody?;
measureParams: '(' (measureParam (',' measureParam)*)? ')' ;
measureParam: aggregateP | measureP | filterP | calculationP | columnRefP | columnRefsP;
measureBody: '{' metadata? conforms* '}';

filter : 'filter' STRINGLITERAL;

dimension: 'dimension' identifier dimensionParams? dimensionBody?;
dimensionParams: '(' (dimensionParam (',' dimensionParam)*)? ')';
dimensionParam: tableP;
dimensionBody: '{' metadata? conforms* dimensionHierarchies? '}';
dimensionHierarchies: (hierarchy hierarchy*) | (hierarchyLevel hierarchyLevel*) | (hierarchyLevelAttribute hierarchyLevelAttribute*);

hierarchy: 'hierarchy' identifier? hierarchyParams? hierarchyBody;
hierarchyParams: '(' (hierarchyParam (',' hierarchyParam)*)? ')' ;
hierarchyParam: tableP;
hierarchyBody: '{'metadata?  hierarchyLevel* '}';
hierarchyLevel:'level' identifier hierarchyLevelParams? hierarchyLevelBody? ;
hierarchyLevelBody: '{'metadata? conforms* hierarchyLevelAttribute* '}';
hierarchyLevelAttribute: 'attribute' identifier hierarchyLevelParams? hierarchyLevelAttributeBody?;
hierarchyLevelAttributeBody: '{'metadata? conforms*'}';
hierarchyLevelParams: '(' (hierarchyLevelParam (',' hierarchyLevelParam)*)? ')' ;
hierarchyLevelParam: tableP | columnRefP | columnRefsP | expressionP | castTypeP;

schemaExports: 'export' importExportBody;
schemaImports: 'import' importExportBody;
importExportBody: wildcardNameBlock | aliasedNamesBlock;
wildcardNameBlock:  identifier '._';
aliasedNamesBlock: aliasedName | '{' aliasedName(',' aliasedName)*'}';
aliasedName: identifier alias?;
alias: '=>' identifier;

table: 'table' identifier tableParams? tableBody?;
tableParams: '(' (tableParam (',' tableParam)*)? ')';
tableParam: dataSourceP | physicalTableP | sqlP;
tableBody:'{' (tableColumn | WILDCARDCOLUMN)* '}';

tableColumn: 'column' quotedIdentifier tableColumnParams? tableColumnBody?;
tableColumnParams: '(' (tableColumnParam (',' tableColumnParam)*)? ')';
tableColumnParam: expressionP | tableRefP | tableRefsP ;
tableColumnBody: '{' tableRef* '}';

tableRef: 'tableRef' tableRefName tableRefParams?;
tableRefName: identifier '.' quotedIdentifier;
tableRefParams: '(' joinTypeP? ')';

conforms: 'conforms' identWithDot;

metadata: 'metadata' metadataItems;
metadataItems: '(' (metadataItem (',' metadataItem)*)? ')' ;
metadataItem: metaDataItemName '=' (primitiveValue | arrayValue);
metaDataItemName: anyParamName | identifier;

identWithDot: identifier('.'identifier)*;
arrayValue: '[' primitiveValue(',' primitiveValue)* ']';

primitiveValue: BOOL| INTEGER| DECIMAL| STRINGLITERAL;

aggregateP: 'aggregate' '=' STRINGLITERAL;
expressionP: 'expression' '=' STRINGLITERAL;
calculationP: 'calculation' '=' STRINGLITERAL;
dataSourceP: 'dataSource' '=' STRINGLITERAL;
strictP: 'strict' '=' BOOL;
tableP: 'table' '=' STRINGLITERAL ;
physicalTableP: 'physicalTable' '=' STRINGLITERAL ;
sqlP: 'sql' '=' STRINGLITERAL ;
columnRefP: 'column' '=' STRINGLITERAL;
columnRefsP: 'columns' '=' multiStringValue;
castTypeP: 'castType' '=' STRINGLITERAL;
measureP: 'measure' '=' STRINGLITERAL;
filterP: 'filter' '=' STRINGLITERAL;
tableRefP: 'tableRef' '=' STRINGLITERAL;
tableRefsP: 'tableRefs' '=' multiStringValue;
joinTypeP: 'joinType' '=' STRINGLITERAL;

multiStringValue: '['STRINGLITERAL(',' STRINGLITERAL)* ']';

// include keyword & anyParamName
identifier: keyword | anyParamName | MIXED_CHARS | LETTER_CHARS | INTEGER;

quotedIdentifier: STRINGLITERAL | identifier;

// param names
anyParamName:
    'aggregate'
    | 'expression'
    | 'dataSource'
    | 'table'
    | 'physicalTable'
    | 'sql'
    | 'column'
    | 'columns'
    | 'castType'
    | 'measure'
    | 'tableRef'
    | 'tableRefs'
    | 'filter'
    | 'calculation';

// keywords not covered by anyParamName
keyword:
    'attribute'
    | 'dimension'
    | 'export'
    | 'hierarchy'
    | 'import'
    | 'level'
    | 'measure'
    | 'schema';
