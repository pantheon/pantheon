// Generated from PslFilter.g4 by ANTLR 4.7.1
// jshint ignore: start
var antlr4 = require("antlr4/index");

// This class defines a complete generic visitor for a parse tree produced by PslFilterParser.

function PslFilterVisitor() {
  antlr4.tree.ParseTreeVisitor.call(this);
  return this;
}

PslFilterVisitor.prototype = Object.create(antlr4.tree.ParseTreeVisitor.prototype);
PslFilterVisitor.prototype.constructor = PslFilterVisitor;

// Visit a parse tree produced by PslFilterParser#filter.
PslFilterVisitor.prototype.visitFilter = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#expr.
PslFilterVisitor.prototype.visitExpr = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#op.
PslFilterVisitor.prototype.visitOp = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#identifier.
PslFilterVisitor.prototype.visitIdentifier = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#binaryOperation.
PslFilterVisitor.prototype.visitBinaryOperation = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#binaryOperator.
PslFilterVisitor.prototype.visitBinaryOperator = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#value.
PslFilterVisitor.prototype.visitValue = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multivalOperation.
PslFilterVisitor.prototype.visitMultivalOperation = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multivalOperator.
PslFilterVisitor.prototype.visitMultivalOperator = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multival.
PslFilterVisitor.prototype.visitMultival = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multiDate.
PslFilterVisitor.prototype.visitMultiDate = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multiTimeStamp.
PslFilterVisitor.prototype.visitMultiTimeStamp = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multiBool.
PslFilterVisitor.prototype.visitMultiBool = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multiNumber.
PslFilterVisitor.prototype.visitMultiNumber = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#multiString.
PslFilterVisitor.prototype.visitMultiString = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#unaryOperation.
PslFilterVisitor.prototype.visitUnaryOperation = function(ctx) {
  return this.visitChildren(ctx);
};

// Visit a parse tree produced by PslFilterParser#queryParam.
PslFilterVisitor.prototype.visitQueryParam = function(ctx) {
  return this.visitChildren(ctx);
};

exports.PslFilterVisitor = PslFilterVisitor;
