import {CommonTokenStream, ParserRuleContext, Token} from 'antlr4';
import {ErrorNode, ParseTreeListener, TerminalNode} from 'antlr4/tree/Tree';

import {FilterContext} from './PslFilterParser';

import {ExprContext} from './PslFilterParser';

import {OpContext} from './PslFilterParser';

import {IdentifierContext} from './PslFilterParser';

import {BinaryOperationContext} from './PslFilterParser';

import {BinaryOperatorContext} from './PslFilterParser';

import {ValueContext} from './PslFilterParser';

import {MultivalOperationContext} from './PslFilterParser';

import {MultivalOperatorContext} from './PslFilterParser';

import {MultivalContext} from './PslFilterParser';

import {MultiDateContext} from './PslFilterParser';

import {MultiTimeStampContext} from './PslFilterParser';

import {MultiBoolContext} from './PslFilterParser';

import {MultiNumberContext} from './PslFilterParser';

import {MultiStringContext} from './PslFilterParser';

import {UnaryOperationContext} from './PslFilterParser';

import {QueryParamContext} from './PslFilterParser';


export declare class PslFilterListener implements ParseTreeListener {
    constructor();
    
    enterFilter(ctx: FilterContext): void;
    
    exitFilter(ctx: FilterContext): void;
    
    enterExpr(ctx: ExprContext): void;
    
    exitExpr(ctx: ExprContext): void;
    
    enterOp(ctx: OpContext): void;
    
    exitOp(ctx: OpContext): void;
    
    enterIdentifier(ctx: IdentifierContext): void;
    
    exitIdentifier(ctx: IdentifierContext): void;
    
    enterBinaryOperation(ctx: BinaryOperationContext): void;
    
    exitBinaryOperation(ctx: BinaryOperationContext): void;
    
    enterBinaryOperator(ctx: BinaryOperatorContext): void;
    
    exitBinaryOperator(ctx: BinaryOperatorContext): void;
    
    enterValue(ctx: ValueContext): void;
    
    exitValue(ctx: ValueContext): void;
    
    enterMultivalOperation(ctx: MultivalOperationContext): void;
    
    exitMultivalOperation(ctx: MultivalOperationContext): void;
    
    enterMultivalOperator(ctx: MultivalOperatorContext): void;
    
    exitMultivalOperator(ctx: MultivalOperatorContext): void;
    
    enterMultival(ctx: MultivalContext): void;
    
    exitMultival(ctx: MultivalContext): void;
    
    enterMultiDate(ctx: MultiDateContext): void;
    
    exitMultiDate(ctx: MultiDateContext): void;
    
    enterMultiTimeStamp(ctx: MultiTimeStampContext): void;
    
    exitMultiTimeStamp(ctx: MultiTimeStampContext): void;
    
    enterMultiBool(ctx: MultiBoolContext): void;
    
    exitMultiBool(ctx: MultiBoolContext): void;
    
    enterMultiNumber(ctx: MultiNumberContext): void;
    
    exitMultiNumber(ctx: MultiNumberContext): void;
    
    enterMultiString(ctx: MultiStringContext): void;
    
    exitMultiString(ctx: MultiStringContext): void;
    
    enterUnaryOperation(ctx: UnaryOperationContext): void;
    
    exitUnaryOperation(ctx: UnaryOperationContext): void;
    
    enterQueryParam(ctx: QueryParamContext): void;
    
    exitQueryParam(ctx: QueryParamContext): void;
    
    visitTerminal(node: TerminalNode): void;

    visitErrorNode(node: ErrorNode): void;

    enterEveryRule(node: ParserRuleContext): void;

    exitEveryRule(node: ParserRuleContext): void;
}
