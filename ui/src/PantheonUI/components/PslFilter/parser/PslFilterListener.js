// Generated from PslFilter.g4 by ANTLR 4.7.1
// jshint ignore: start
var antlr4 = require("antlr4/index");

// This class defines a complete listener for a parse tree produced by PslFilterParser.
function PslFilterListener() {
  antlr4.tree.ParseTreeListener.call(this);
  return this;
}

PslFilterListener.prototype = Object.create(antlr4.tree.ParseTreeListener.prototype);
PslFilterListener.prototype.constructor = PslFilterListener;

// Enter a parse tree produced by PslFilterParser#filter.
PslFilterListener.prototype.enterFilter = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#filter.
PslFilterListener.prototype.exitFilter = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#expr.
PslFilterListener.prototype.enterExpr = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#expr.
PslFilterListener.prototype.exitExpr = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#op.
PslFilterListener.prototype.enterOp = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#op.
PslFilterListener.prototype.exitOp = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#identifier.
PslFilterListener.prototype.enterIdentifier = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#identifier.
PslFilterListener.prototype.exitIdentifier = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#binaryOperation.
PslFilterListener.prototype.enterBinaryOperation = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#binaryOperation.
PslFilterListener.prototype.exitBinaryOperation = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#binaryOperator.
PslFilterListener.prototype.enterBinaryOperator = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#binaryOperator.
PslFilterListener.prototype.exitBinaryOperator = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#value.
PslFilterListener.prototype.enterValue = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#value.
PslFilterListener.prototype.exitValue = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multivalOperation.
PslFilterListener.prototype.enterMultivalOperation = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multivalOperation.
PslFilterListener.prototype.exitMultivalOperation = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multivalOperator.
PslFilterListener.prototype.enterMultivalOperator = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multivalOperator.
PslFilterListener.prototype.exitMultivalOperator = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multival.
PslFilterListener.prototype.enterMultival = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multival.
PslFilterListener.prototype.exitMultival = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multiDate.
PslFilterListener.prototype.enterMultiDate = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multiDate.
PslFilterListener.prototype.exitMultiDate = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multiTimeStamp.
PslFilterListener.prototype.enterMultiTimeStamp = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multiTimeStamp.
PslFilterListener.prototype.exitMultiTimeStamp = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multiBool.
PslFilterListener.prototype.enterMultiBool = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multiBool.
PslFilterListener.prototype.exitMultiBool = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multiNumber.
PslFilterListener.prototype.enterMultiNumber = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multiNumber.
PslFilterListener.prototype.exitMultiNumber = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#multiString.
PslFilterListener.prototype.enterMultiString = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#multiString.
PslFilterListener.prototype.exitMultiString = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#unaryOperation.
PslFilterListener.prototype.enterUnaryOperation = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#unaryOperation.
PslFilterListener.prototype.exitUnaryOperation = function(ctx) {};

// Enter a parse tree produced by PslFilterParser#queryParam.
PslFilterListener.prototype.enterQueryParam = function(ctx) {};

// Exit a parse tree produced by PslFilterParser#queryParam.
PslFilterListener.prototype.exitQueryParam = function(ctx) {};

exports.PslFilterListener = PslFilterListener;
