import {CommonTokenStream, Parser, ParserRuleContext, Token} from 'antlr4';
import {TerminalNode} from 'antlr4/tree/Tree';


export declare class FilterContext extends ParserRuleContext {
    
    expr(): ExprContext;
    
}

export declare class OpContext extends ParserRuleContext {
    
    identifier(): IdentifierContext;
    
    binaryOperation(): BinaryOperationContext;
    
    multivalOperation(): MultivalOperationContext;
    
    unaryOperation(): UnaryOperationContext;
    
}

export declare class IdentifierContext extends ParserRuleContext {
    
    CHARS_WITH_DOT(): TerminalNode;
    
    LETTER_CHARS(): TerminalNode;
    
}

export declare class BinaryOperationContext extends ParserRuleContext {
    
    binaryOperator(): BinaryOperatorContext;
    
    value(): ValueContext;
    
}

export declare class BinaryOperatorContext extends ParserRuleContext {
    
    EQ(): TerminalNode;
    
    GT(): TerminalNode;
    
    GTEQ(): TerminalNode;
    
    LT(): TerminalNode;
    
    LTEQ(): TerminalNode;
    
    NEQ(): TerminalNode;
    
}

export declare class ValueContext extends ParserRuleContext {
    
    queryParam(): QueryParamContext;
    
    DATE(): TerminalNode;
    
    TIMESTAMP(): TerminalNode;
    
    BOOL(): TerminalNode;
    
    NUMBER(): TerminalNode;
    
    SINGLEQUOTEDSTRINGLIT(): TerminalNode;
    
}

export declare class MultivalOperationContext extends ParserRuleContext {
    
    multivalOperator(): MultivalOperatorContext;
    
    multival(): MultivalContext;
    
}

export declare class MultivalOperatorContext extends ParserRuleContext {
    
    IN(): TerminalNode;
    
    NOTIN(): TerminalNode;
    
    LIKE(): TerminalNode;
    
}

export declare class MultivalContext extends ParserRuleContext {
    
    multiNumber(): MultiNumberContext;
    
    multiString(): MultiStringContext;
    
    multiBool(): MultiBoolContext;
    
    multiDate(): MultiDateContext;
    
    multiTimeStamp(): MultiTimeStampContext;
    
}

export declare class MultiDateContext extends ParserRuleContext {
    
}

export declare class MultiTimeStampContext extends ParserRuleContext {
    
}

export declare class MultiBoolContext extends ParserRuleContext {
    
}

export declare class MultiNumberContext extends ParserRuleContext {
    
}

export declare class MultiStringContext extends ParserRuleContext {
    
}

export declare class UnaryOperationContext extends ParserRuleContext {
    
    IsNull(): TerminalNode;
    
    IsNotNull(): TerminalNode;
    
}

export declare class QueryParamContext extends ParserRuleContext {
    
    LETTER_CHARS(): TerminalNode;
    
}

export declare class ExprContext extends ParserRuleContext {
    
    op(): OpContext;
    
    AND(): TerminalNode;
    
    OR(): TerminalNode;
    
}


export declare class PslFilterParser extends Parser {
    readonly ruleNames: string[];
    readonly literalNames: string[];
    readonly symbolicNames: string[];

    constructor(input: CommonTokenStream);
    
    filter(): FilterContext;

    op(): OpContext;

    identifier(): IdentifierContext;

    binaryOperation(): BinaryOperationContext;

    binaryOperator(): BinaryOperatorContext;

    value(): ValueContext;

    multivalOperation(): MultivalOperationContext;

    multivalOperator(): MultivalOperatorContext;

    multival(): MultivalContext;

    multiDate(): MultiDateContext;

    multiTimeStamp(): MultiTimeStampContext;

    multiBool(): MultiBoolContext;

    multiNumber(): MultiNumberContext;

    multiString(): MultiStringContext;

    unaryOperation(): UnaryOperationContext;

    queryParam(): QueryParamContext;

}
