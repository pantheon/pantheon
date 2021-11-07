// Generated from PslFilter.g4 by ANTLR 4.7.1
// jshint ignore: start
var antlr4 = require("antlr4/index");
var PslFilterListener = require("./PslFilterListener").PslFilterListener;
var PslFilterVisitor = require("./PslFilterVisitor").PslFilterVisitor;

var grammarFileName = "PslFilter.g4";

var serializedATN = [
  "\u0003\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964",
  "\u0003\u001e\u008c\u0004\u0002\t\u0002\u0004\u0003\t\u0003\u0004\u0004",
  "\t\u0004\u0004\u0005\t\u0005\u0004\u0006\t\u0006\u0004\u0007\t\u0007",
  "\u0004\b\t\b\u0004\t\t\t\u0004\n\t\n\u0004\u000b\t\u000b\u0004\f\t\f",
  "\u0004\r\t\r\u0004\u000e\t\u000e\u0004\u000f\t\u000f\u0004\u0010\t\u0010",
  "\u0004\u0011\t\u0011\u0004\u0012\t\u0012\u0003\u0002\u0003\u0002\u0003",
  "\u0002\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003",
  "\u0003\u0005\u0003.\n\u0003\u0003\u0003\u0003\u0003\u0003\u0003\u0003",
  "\u0003\u0003\u0003\u0003\u0003\u0007\u00036\n\u0003\f\u0003\u000e\u0003",
  "9\u000b\u0003\u0003\u0004\u0003\u0004\u0003\u0004\u0003\u0004\u0005",
  "\u0004?\n\u0004\u0003\u0005\u0003\u0005\u0003\u0006\u0003\u0006\u0003",
  "\u0006\u0003\u0007\u0003\u0007\u0003\b\u0003\b\u0003\b\u0003\b\u0003",
  "\b\u0003\b\u0005\bN\n\b\u0003\t\u0003\t\u0003\t\u0003\n\u0003\n\u0003",
  "\u000b\u0003\u000b\u0003\u000b\u0003\u000b\u0003\u000b\u0003\u000b\u0005",
  "\u000b[\n\u000b\u0003\u000b\u0003\u000b\u0003\f\u0003\f\u0003\f\u0007",
  "\fb\n\f\f\f\u000e\fe\u000b\f\u0003\r\u0003\r\u0003\r\u0007\rj\n\r\f",
  "\r\u000e\rm\u000b\r\u0003\u000e\u0003\u000e\u0003\u000e\u0007\u000e",
  "r\n\u000e\f\u000e\u000e\u000eu\u000b\u000e\u0003\u000f\u0003\u000f\u0003",
  "\u000f\u0007\u000fz\n\u000f\f\u000f\u000e\u000f}\u000b\u000f\u0003\u0010",
  "\u0003\u0010\u0003\u0010\u0007\u0010\u0082\n\u0010\f\u0010\u000e\u0010",
  "\u0085\u000b\u0010\u0003\u0011\u0003\u0011\u0003\u0012\u0003\u0012\u0003",
  "\u0012\u0003\u0012\u0002\u0003\u0004\u0013\u0002\u0004\u0006\b\n\f\u000e",
  '\u0010\u0012\u0014\u0016\u0018\u001a\u001c\u001e "\u0002\u0006\u0003',
  "\u0002\u001c\u001d\u0003\u0002\u000b\u0010\u0003\u0002\u0013\u0015\u0003",
  "\u0002\u0011\u0012\u0002\u008d\u0002$\u0003\u0002\u0002\u0002\u0004",
  "-\u0003\u0002\u0002\u0002\u0006:\u0003\u0002\u0002\u0002\b@\u0003\u0002",
  "\u0002\u0002\nB\u0003\u0002\u0002\u0002\fE\u0003\u0002\u0002\u0002\u000e",
  "M\u0003\u0002\u0002\u0002\u0010O\u0003\u0002\u0002\u0002\u0012R\u0003",
  "\u0002\u0002\u0002\u0014T\u0003\u0002\u0002\u0002\u0016^\u0003\u0002",
  "\u0002\u0002\u0018f\u0003\u0002\u0002\u0002\u001an\u0003\u0002\u0002",
  "\u0002\u001cv\u0003\u0002\u0002\u0002\u001e~\u0003\u0002\u0002\u0002",
  ' \u0086\u0003\u0002\u0002\u0002"\u0088\u0003\u0002\u0002\u0002$%\u0005',
  "\u0004\u0003\u0002%&\u0007\u0002\u0002\u0003&\u0003\u0003\u0002\u0002",
  "\u0002'(\b\u0003\u0001\u0002(.\u0005\u0006\u0004\u0002)*\u0007\u0003",
  "\u0002\u0002*+\u0005\u0004\u0003\u0002+,\u0007\u0004\u0002\u0002,.\u0003",
  "\u0002\u0002\u0002-'\u0003\u0002\u0002\u0002-)\u0003\u0002\u0002\u0002",
  ".7\u0003\u0002\u0002\u0002/0\f\u0006\u0002\u000201\u0007\u0016\u0002",
  "\u000216\u0005\u0004\u0003\u000723\f\u0005\u0002\u000234\u0007\u0017",
  "\u0002\u000246\u0005\u0004\u0003\u00065/\u0003\u0002\u0002\u000252\u0003",
  "\u0002\u0002\u000269\u0003\u0002\u0002\u000275\u0003\u0002\u0002\u0002",
  "78\u0003\u0002\u0002\u00028\u0005\u0003\u0002\u0002\u000297\u0003\u0002",
  "\u0002\u0002:>\u0005\b\u0005\u0002;?\u0005\n\u0006\u0002<?\u0005\u0010",
  "\t\u0002=?\u0005 \u0011\u0002>;\u0003\u0002\u0002\u0002><\u0003\u0002",
  "\u0002\u0002>=\u0003\u0002\u0002\u0002?\u0007\u0003\u0002\u0002\u0002",
  "@A\t\u0002\u0002\u0002A\t\u0003\u0002\u0002\u0002BC\u0005\f\u0007\u0002",
  "CD\u0005\u000e\b\u0002D\u000b\u0003\u0002\u0002\u0002EF\t\u0003\u0002",
  '\u0002F\r\u0003\u0002\u0002\u0002GN\u0005"\u0012\u0002HN\u0007\n\u0002',
  "\u0002IN\u0007\t\u0002\u0002JN\u0007\u0007\u0002\u0002KN\u0007\b\u0002",
  "\u0002LN\u0007\u001b\u0002\u0002MG\u0003\u0002\u0002\u0002MH\u0003\u0002",
  "\u0002\u0002MI\u0003\u0002\u0002\u0002MJ\u0003\u0002\u0002\u0002MK\u0003",
  "\u0002\u0002\u0002ML\u0003\u0002\u0002\u0002N\u000f\u0003\u0002\u0002",
  "\u0002OP\u0005\u0012\n\u0002PQ\u0005\u0014\u000b\u0002Q\u0011\u0003",
  "\u0002\u0002\u0002RS\t\u0004\u0002\u0002S\u0013\u0003\u0002\u0002\u0002",
  "TZ\u0007\u0003\u0002\u0002U[\u0005\u001c\u000f\u0002V[\u0005\u001e\u0010",
  "\u0002W[\u0005\u001a\u000e\u0002X[\u0005\u0016\f\u0002Y[\u0005\u0018",
  "\r\u0002ZU\u0003\u0002\u0002\u0002ZV\u0003\u0002\u0002\u0002ZW\u0003",
  "\u0002\u0002\u0002ZX\u0003\u0002\u0002\u0002ZY\u0003\u0002\u0002\u0002",
  "[\\\u0003\u0002\u0002\u0002\\]\u0007\u0004\u0002\u0002]\u0015\u0003",
  "\u0002\u0002\u0002^c\u0007\n\u0002\u0002_`\u0007\u0005\u0002\u0002`",
  "b\u0007\n\u0002\u0002a_\u0003\u0002\u0002\u0002be\u0003\u0002\u0002",
  "\u0002ca\u0003\u0002\u0002\u0002cd\u0003\u0002\u0002\u0002d\u0017\u0003",
  "\u0002\u0002\u0002ec\u0003\u0002\u0002\u0002fk\u0007\t\u0002\u0002g",
  "h\u0007\u0005\u0002\u0002hj\u0007\t\u0002\u0002ig\u0003\u0002\u0002",
  "\u0002jm\u0003\u0002\u0002\u0002ki\u0003\u0002\u0002\u0002kl\u0003\u0002",
  "\u0002\u0002l\u0019\u0003\u0002\u0002\u0002mk\u0003\u0002\u0002\u0002",
  "ns\u0007\u0007\u0002\u0002op\u0007\u0005\u0002\u0002pr\u0007\u0007\u0002",
  "\u0002qo\u0003\u0002\u0002\u0002ru\u0003\u0002\u0002\u0002sq\u0003\u0002",
  "\u0002\u0002st\u0003\u0002\u0002\u0002t\u001b\u0003\u0002\u0002\u0002",
  "us\u0003\u0002\u0002\u0002v{\u0007\b\u0002\u0002wx\u0007\u0005\u0002",
  "\u0002xz\u0007\b\u0002\u0002yw\u0003\u0002\u0002\u0002z}\u0003\u0002",
  "\u0002\u0002{y\u0003\u0002\u0002\u0002{|\u0003\u0002\u0002\u0002|\u001d",
  "\u0003\u0002\u0002\u0002}{\u0003\u0002\u0002\u0002~\u0083\u0007\u001b",
  "\u0002\u0002\u007f\u0080\u0007\u0005\u0002\u0002\u0080\u0082\u0007\u001b",
  "\u0002\u0002\u0081\u007f\u0003\u0002\u0002\u0002\u0082\u0085\u0003\u0002",
  "\u0002\u0002\u0083\u0081\u0003\u0002\u0002\u0002\u0083\u0084\u0003\u0002",
  "\u0002\u0002\u0084\u001f\u0003\u0002\u0002\u0002\u0085\u0083\u0003\u0002",
  "\u0002\u0002\u0086\u0087\t\u0005\u0002\u0002\u0087!\u0003\u0002\u0002",
  "\u0002\u0088\u0089\u0007\u0006\u0002\u0002\u0089\u008a\u0007\u001d\u0002",
  "\u0002\u008a#\u0003\u0002\u0002\u0002\r-57>MZcks{\u0083",
].join("");

var atn = new antlr4.atn.ATNDeserializer().deserialize(serializedATN);

var decisionsToDFA = atn.decisionToState.map(function(ds, index) {
  return new antlr4.dfa.DFA(ds, index);
});

var sharedContextCache = new antlr4.PredictionContextCache();

var literalNames = [
  null,
  "'('",
  "')'",
  "','",
  "':'",
  null,
  null,
  null,
  null,
  "'='",
  "'>'",
  "'>='",
  "'<'",
  "'<='",
  "'!='",
  null,
  null,
  null,
  "'in'",
  "'like'",
  "'and'",
  "'or'",
];

var symbolicNames = [
  null,
  null,
  null,
  null,
  null,
  "BOOL",
  "NUMBER",
  "TIMESTAMP",
  "DATE",
  "EQ",
  "GT",
  "GTEQ",
  "LT",
  "LTEQ",
  "NEQ",
  "IsNull",
  "IsNotNull",
  "NOTIN",
  "IN",
  "LIKE",
  "AND",
  "OR",
  "LINE_COMMENT",
  "BLOCK_COMMENT",
  "WS",
  "SINGLEQUOTEDSTRINGLIT",
  "CHARS_WITH_DOT",
  "LETTER_CHARS",
  "ERRORCHARACTER",
];

var ruleNames = [
  "filter",
  "expr",
  "op",
  "identifier",
  "binaryOperation",
  "binaryOperator",
  "value",
  "multivalOperation",
  "multivalOperator",
  "multival",
  "multiDate",
  "multiTimeStamp",
  "multiBool",
  "multiNumber",
  "multiString",
  "unaryOperation",
  "queryParam",
];

function PslFilterParser(input) {
  antlr4.Parser.call(this, input);
  this._interp = new antlr4.atn.ParserATNSimulator(this, atn, decisionsToDFA, sharedContextCache);
  this.ruleNames = ruleNames;
  this.literalNames = literalNames;
  this.symbolicNames = symbolicNames;
  return this;
}

PslFilterParser.prototype = Object.create(antlr4.Parser.prototype);
PslFilterParser.prototype.constructor = PslFilterParser;

Object.defineProperty(PslFilterParser.prototype, "atn", {
  get: function() {
    return atn;
  },
});

PslFilterParser.EOF = antlr4.Token.EOF;
PslFilterParser.T__0 = 1;
PslFilterParser.T__1 = 2;
PslFilterParser.T__2 = 3;
PslFilterParser.T__3 = 4;
PslFilterParser.BOOL = 5;
PslFilterParser.NUMBER = 6;
PslFilterParser.TIMESTAMP = 7;
PslFilterParser.DATE = 8;
PslFilterParser.EQ = 9;
PslFilterParser.GT = 10;
PslFilterParser.GTEQ = 11;
PslFilterParser.LT = 12;
PslFilterParser.LTEQ = 13;
PslFilterParser.NEQ = 14;
PslFilterParser.IsNull = 15;
PslFilterParser.IsNotNull = 16;
PslFilterParser.NOTIN = 17;
PslFilterParser.IN = 18;
PslFilterParser.LIKE = 19;
PslFilterParser.AND = 20;
PslFilterParser.OR = 21;
PslFilterParser.LINE_COMMENT = 22;
PslFilterParser.BLOCK_COMMENT = 23;
PslFilterParser.WS = 24;
PslFilterParser.SINGLEQUOTEDSTRINGLIT = 25;
PslFilterParser.CHARS_WITH_DOT = 26;
PslFilterParser.LETTER_CHARS = 27;
PslFilterParser.ERRORCHARACTER = 28;

PslFilterParser.RULE_filter = 0;
PslFilterParser.RULE_expr = 1;
PslFilterParser.RULE_op = 2;
PslFilterParser.RULE_identifier = 3;
PslFilterParser.RULE_binaryOperation = 4;
PslFilterParser.RULE_binaryOperator = 5;
PslFilterParser.RULE_value = 6;
PslFilterParser.RULE_multivalOperation = 7;
PslFilterParser.RULE_multivalOperator = 8;
PslFilterParser.RULE_multival = 9;
PslFilterParser.RULE_multiDate = 10;
PslFilterParser.RULE_multiTimeStamp = 11;
PslFilterParser.RULE_multiBool = 12;
PslFilterParser.RULE_multiNumber = 13;
PslFilterParser.RULE_multiString = 14;
PslFilterParser.RULE_unaryOperation = 15;
PslFilterParser.RULE_queryParam = 16;

function FilterContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_filter;
  return this;
}

FilterContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
FilterContext.prototype.constructor = FilterContext;

FilterContext.prototype.expr = function() {
  return this.getTypedRuleContext(ExprContext, 0);
};

FilterContext.prototype.EOF = function() {
  return this.getToken(PslFilterParser.EOF, 0);
};

FilterContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterFilter(this);
  }
};

FilterContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitFilter(this);
  }
};

FilterContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitFilter(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.FilterContext = FilterContext;

PslFilterParser.prototype.filter = function() {
  var localctx = new FilterContext(this, this._ctx, this.state);
  this.enterRule(localctx, 0, PslFilterParser.RULE_filter);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 34;
    this.expr(0);
    this.state = 35;
    this.match(PslFilterParser.EOF);
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function ExprContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_expr;
  return this;
}

ExprContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
ExprContext.prototype.constructor = ExprContext;

ExprContext.prototype.op = function() {
  return this.getTypedRuleContext(OpContext, 0);
};

ExprContext.prototype.expr = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTypedRuleContexts(ExprContext);
  } else {
    return this.getTypedRuleContext(ExprContext, i);
  }
};

ExprContext.prototype.AND = function() {
  return this.getToken(PslFilterParser.AND, 0);
};

ExprContext.prototype.OR = function() {
  return this.getToken(PslFilterParser.OR, 0);
};

ExprContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterExpr(this);
  }
};

ExprContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitExpr(this);
  }
};

ExprContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitExpr(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.prototype.expr = function(_p) {
  if (_p === undefined) {
    _p = 0;
  }
  var _parentctx = this._ctx;
  var _parentState = this.state;
  var localctx = new ExprContext(this, this._ctx, _parentState);
  var _prevctx = localctx;
  var _startState = 2;
  this.enterRecursionRule(localctx, 2, PslFilterParser.RULE_expr, _p);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 43;
    this._errHandler.sync(this);
    switch (this._input.LA(1)) {
      case PslFilterParser.CHARS_WITH_DOT:
      case PslFilterParser.LETTER_CHARS:
        this.state = 38;
        this.op();
        break;
      case PslFilterParser.T__0:
        this.state = 39;
        this.match(PslFilterParser.T__0);
        this.state = 40;
        this.expr(0);
        this.state = 41;
        this.match(PslFilterParser.T__1);
        break;
      default:
        throw new antlr4.error.NoViableAltException(this);
    }
    this._ctx.stop = this._input.LT(-1);
    this.state = 53;
    this._errHandler.sync(this);
    var _alt = this._interp.adaptivePredict(this._input, 2, this._ctx);
    while (_alt != 2 && _alt != antlr4.atn.ATN.INVALID_ALT_NUMBER) {
      if (_alt === 1) {
        if (this._parseListeners !== null) {
          this.triggerExitRuleEvent();
        }
        _prevctx = localctx;
        this.state = 51;
        this._errHandler.sync(this);
        var la_ = this._interp.adaptivePredict(this._input, 1, this._ctx);
        switch (la_) {
          case 1:
            localctx = new ExprContext(this, _parentctx, _parentState);
            this.pushNewRecursionContext(localctx, _startState, PslFilterParser.RULE_expr);
            this.state = 45;
            if (!this.precpred(this._ctx, 4)) {
              throw new antlr4.error.FailedPredicateException(this, "this.precpred(this._ctx, 4)");
            }
            this.state = 46;
            this.match(PslFilterParser.AND);
            this.state = 47;
            this.expr(5);
            break;

          case 2:
            localctx = new ExprContext(this, _parentctx, _parentState);
            this.pushNewRecursionContext(localctx, _startState, PslFilterParser.RULE_expr);
            this.state = 48;
            if (!this.precpred(this._ctx, 3)) {
              throw new antlr4.error.FailedPredicateException(this, "this.precpred(this._ctx, 3)");
            }
            this.state = 49;
            this.match(PslFilterParser.OR);
            this.state = 50;
            this.expr(4);
            break;
        }
      }
      this.state = 55;
      this._errHandler.sync(this);
      _alt = this._interp.adaptivePredict(this._input, 2, this._ctx);
    }
  } catch (error) {
    if (error instanceof antlr4.error.RecognitionException) {
      localctx.exception = error;
      this._errHandler.reportError(this, error);
      this._errHandler.recover(this, error);
    } else {
      throw error;
    }
  } finally {
    this.unrollRecursionContexts(_parentctx);
  }
  return localctx;
};

function OpContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_op;
  return this;
}

OpContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
OpContext.prototype.constructor = OpContext;

OpContext.prototype.identifier = function() {
  return this.getTypedRuleContext(IdentifierContext, 0);
};

OpContext.prototype.binaryOperation = function() {
  return this.getTypedRuleContext(BinaryOperationContext, 0);
};

OpContext.prototype.multivalOperation = function() {
  return this.getTypedRuleContext(MultivalOperationContext, 0);
};

OpContext.prototype.unaryOperation = function() {
  return this.getTypedRuleContext(UnaryOperationContext, 0);
};

OpContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterOp(this);
  }
};

OpContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitOp(this);
  }
};

OpContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitOp(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.OpContext = OpContext;

PslFilterParser.prototype.op = function() {
  var localctx = new OpContext(this, this._ctx, this.state);
  this.enterRule(localctx, 4, PslFilterParser.RULE_op);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 56;
    this.identifier();
    this.state = 60;
    this._errHandler.sync(this);
    switch (this._input.LA(1)) {
      case PslFilterParser.EQ:
      case PslFilterParser.GT:
      case PslFilterParser.GTEQ:
      case PslFilterParser.LT:
      case PslFilterParser.LTEQ:
      case PslFilterParser.NEQ:
        this.state = 57;
        this.binaryOperation();
        break;
      case PslFilterParser.NOTIN:
      case PslFilterParser.IN:
      case PslFilterParser.LIKE:
        this.state = 58;
        this.multivalOperation();
        break;
      case PslFilterParser.IsNull:
      case PslFilterParser.IsNotNull:
        this.state = 59;
        this.unaryOperation();
        break;
      default:
        throw new antlr4.error.NoViableAltException(this);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function IdentifierContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_identifier;
  return this;
}

IdentifierContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
IdentifierContext.prototype.constructor = IdentifierContext;

IdentifierContext.prototype.CHARS_WITH_DOT = function() {
  return this.getToken(PslFilterParser.CHARS_WITH_DOT, 0);
};

IdentifierContext.prototype.LETTER_CHARS = function() {
  return this.getToken(PslFilterParser.LETTER_CHARS, 0);
};

IdentifierContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterIdentifier(this);
  }
};

IdentifierContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitIdentifier(this);
  }
};

IdentifierContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitIdentifier(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.IdentifierContext = IdentifierContext;

PslFilterParser.prototype.identifier = function() {
  var localctx = new IdentifierContext(this, this._ctx, this.state);
  this.enterRule(localctx, 6, PslFilterParser.RULE_identifier);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 62;
    _la = this._input.LA(1);
    if (!(_la === PslFilterParser.CHARS_WITH_DOT || _la === PslFilterParser.LETTER_CHARS)) {
      this._errHandler.recoverInline(this);
    } else {
      this._errHandler.reportMatch(this);
      this.consume();
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function BinaryOperationContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_binaryOperation;
  return this;
}

BinaryOperationContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
BinaryOperationContext.prototype.constructor = BinaryOperationContext;

BinaryOperationContext.prototype.binaryOperator = function() {
  return this.getTypedRuleContext(BinaryOperatorContext, 0);
};

BinaryOperationContext.prototype.value = function() {
  return this.getTypedRuleContext(ValueContext, 0);
};

BinaryOperationContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterBinaryOperation(this);
  }
};

BinaryOperationContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitBinaryOperation(this);
  }
};

BinaryOperationContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitBinaryOperation(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.BinaryOperationContext = BinaryOperationContext;

PslFilterParser.prototype.binaryOperation = function() {
  var localctx = new BinaryOperationContext(this, this._ctx, this.state);
  this.enterRule(localctx, 8, PslFilterParser.RULE_binaryOperation);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 64;
    this.binaryOperator();
    this.state = 65;
    this.value();
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function BinaryOperatorContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_binaryOperator;
  return this;
}

BinaryOperatorContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
BinaryOperatorContext.prototype.constructor = BinaryOperatorContext;

BinaryOperatorContext.prototype.EQ = function() {
  return this.getToken(PslFilterParser.EQ, 0);
};

BinaryOperatorContext.prototype.GT = function() {
  return this.getToken(PslFilterParser.GT, 0);
};

BinaryOperatorContext.prototype.GTEQ = function() {
  return this.getToken(PslFilterParser.GTEQ, 0);
};

BinaryOperatorContext.prototype.LT = function() {
  return this.getToken(PslFilterParser.LT, 0);
};

BinaryOperatorContext.prototype.LTEQ = function() {
  return this.getToken(PslFilterParser.LTEQ, 0);
};

BinaryOperatorContext.prototype.NEQ = function() {
  return this.getToken(PslFilterParser.NEQ, 0);
};

BinaryOperatorContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterBinaryOperator(this);
  }
};

BinaryOperatorContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitBinaryOperator(this);
  }
};

BinaryOperatorContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitBinaryOperator(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.BinaryOperatorContext = BinaryOperatorContext;

PslFilterParser.prototype.binaryOperator = function() {
  var localctx = new BinaryOperatorContext(this, this._ctx, this.state);
  this.enterRule(localctx, 10, PslFilterParser.RULE_binaryOperator);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 67;
    _la = this._input.LA(1);
    if (
      !(
        (_la & ~0x1f) == 0 &&
        ((1 << _la) &
          ((1 << PslFilterParser.EQ) |
            (1 << PslFilterParser.GT) |
            (1 << PslFilterParser.GTEQ) |
            (1 << PslFilterParser.LT) |
            (1 << PslFilterParser.LTEQ) |
            (1 << PslFilterParser.NEQ))) !==
          0
      )
    ) {
      this._errHandler.recoverInline(this);
    } else {
      this._errHandler.reportMatch(this);
      this.consume();
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function ValueContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_value;
  return this;
}

ValueContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
ValueContext.prototype.constructor = ValueContext;

ValueContext.prototype.queryParam = function() {
  return this.getTypedRuleContext(QueryParamContext, 0);
};

ValueContext.prototype.DATE = function() {
  return this.getToken(PslFilterParser.DATE, 0);
};

ValueContext.prototype.TIMESTAMP = function() {
  return this.getToken(PslFilterParser.TIMESTAMP, 0);
};

ValueContext.prototype.BOOL = function() {
  return this.getToken(PslFilterParser.BOOL, 0);
};

ValueContext.prototype.NUMBER = function() {
  return this.getToken(PslFilterParser.NUMBER, 0);
};

ValueContext.prototype.SINGLEQUOTEDSTRINGLIT = function() {
  return this.getToken(PslFilterParser.SINGLEQUOTEDSTRINGLIT, 0);
};

ValueContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterValue(this);
  }
};

ValueContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitValue(this);
  }
};

ValueContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitValue(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.ValueContext = ValueContext;

PslFilterParser.prototype.value = function() {
  var localctx = new ValueContext(this, this._ctx, this.state);
  this.enterRule(localctx, 12, PslFilterParser.RULE_value);
  try {
    this.state = 75;
    this._errHandler.sync(this);
    switch (this._input.LA(1)) {
      case PslFilterParser.T__3:
        this.enterOuterAlt(localctx, 1);
        this.state = 69;
        this.queryParam();
        break;
      case PslFilterParser.DATE:
        this.enterOuterAlt(localctx, 2);
        this.state = 70;
        this.match(PslFilterParser.DATE);
        break;
      case PslFilterParser.TIMESTAMP:
        this.enterOuterAlt(localctx, 3);
        this.state = 71;
        this.match(PslFilterParser.TIMESTAMP);
        break;
      case PslFilterParser.BOOL:
        this.enterOuterAlt(localctx, 4);
        this.state = 72;
        this.match(PslFilterParser.BOOL);
        break;
      case PslFilterParser.NUMBER:
        this.enterOuterAlt(localctx, 5);
        this.state = 73;
        this.match(PslFilterParser.NUMBER);
        break;
      case PslFilterParser.SINGLEQUOTEDSTRINGLIT:
        this.enterOuterAlt(localctx, 6);
        this.state = 74;
        this.match(PslFilterParser.SINGLEQUOTEDSTRINGLIT);
        break;
      default:
        throw new antlr4.error.NoViableAltException(this);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultivalOperationContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multivalOperation;
  return this;
}

MultivalOperationContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultivalOperationContext.prototype.constructor = MultivalOperationContext;

MultivalOperationContext.prototype.multivalOperator = function() {
  return this.getTypedRuleContext(MultivalOperatorContext, 0);
};

MultivalOperationContext.prototype.multival = function() {
  return this.getTypedRuleContext(MultivalContext, 0);
};

MultivalOperationContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultivalOperation(this);
  }
};

MultivalOperationContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultivalOperation(this);
  }
};

MultivalOperationContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultivalOperation(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultivalOperationContext = MultivalOperationContext;

PslFilterParser.prototype.multivalOperation = function() {
  var localctx = new MultivalOperationContext(this, this._ctx, this.state);
  this.enterRule(localctx, 14, PslFilterParser.RULE_multivalOperation);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 77;
    this.multivalOperator();
    this.state = 78;
    this.multival();
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultivalOperatorContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multivalOperator;
  return this;
}

MultivalOperatorContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultivalOperatorContext.prototype.constructor = MultivalOperatorContext;

MultivalOperatorContext.prototype.IN = function() {
  return this.getToken(PslFilterParser.IN, 0);
};

MultivalOperatorContext.prototype.NOTIN = function() {
  return this.getToken(PslFilterParser.NOTIN, 0);
};

MultivalOperatorContext.prototype.LIKE = function() {
  return this.getToken(PslFilterParser.LIKE, 0);
};

MultivalOperatorContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultivalOperator(this);
  }
};

MultivalOperatorContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultivalOperator(this);
  }
};

MultivalOperatorContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultivalOperator(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultivalOperatorContext = MultivalOperatorContext;

PslFilterParser.prototype.multivalOperator = function() {
  var localctx = new MultivalOperatorContext(this, this._ctx, this.state);
  this.enterRule(localctx, 16, PslFilterParser.RULE_multivalOperator);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 80;
    _la = this._input.LA(1);
    if (
      !(
        (_la & ~0x1f) == 0 &&
        ((1 << _la) & ((1 << PslFilterParser.NOTIN) | (1 << PslFilterParser.IN) | (1 << PslFilterParser.LIKE))) !== 0
      )
    ) {
      this._errHandler.recoverInline(this);
    } else {
      this._errHandler.reportMatch(this);
      this.consume();
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultivalContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multival;
  return this;
}

MultivalContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultivalContext.prototype.constructor = MultivalContext;

MultivalContext.prototype.multiNumber = function() {
  return this.getTypedRuleContext(MultiNumberContext, 0);
};

MultivalContext.prototype.multiString = function() {
  return this.getTypedRuleContext(MultiStringContext, 0);
};

MultivalContext.prototype.multiBool = function() {
  return this.getTypedRuleContext(MultiBoolContext, 0);
};

MultivalContext.prototype.multiDate = function() {
  return this.getTypedRuleContext(MultiDateContext, 0);
};

MultivalContext.prototype.multiTimeStamp = function() {
  return this.getTypedRuleContext(MultiTimeStampContext, 0);
};

MultivalContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultival(this);
  }
};

MultivalContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultival(this);
  }
};

MultivalContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultival(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultivalContext = MultivalContext;

PslFilterParser.prototype.multival = function() {
  var localctx = new MultivalContext(this, this._ctx, this.state);
  this.enterRule(localctx, 18, PslFilterParser.RULE_multival);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 82;
    this.match(PslFilterParser.T__0);
    this.state = 88;
    this._errHandler.sync(this);
    switch (this._input.LA(1)) {
      case PslFilterParser.NUMBER:
        this.state = 83;
        this.multiNumber();
        break;
      case PslFilterParser.SINGLEQUOTEDSTRINGLIT:
        this.state = 84;
        this.multiString();
        break;
      case PslFilterParser.BOOL:
        this.state = 85;
        this.multiBool();
        break;
      case PslFilterParser.DATE:
        this.state = 86;
        this.multiDate();
        break;
      case PslFilterParser.TIMESTAMP:
        this.state = 87;
        this.multiTimeStamp();
        break;
      default:
        throw new antlr4.error.NoViableAltException(this);
    }
    this.state = 90;
    this.match(PslFilterParser.T__1);
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultiDateContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multiDate;
  return this;
}

MultiDateContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultiDateContext.prototype.constructor = MultiDateContext;

MultiDateContext.prototype.DATE = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTokens(PslFilterParser.DATE);
  } else {
    return this.getToken(PslFilterParser.DATE, i);
  }
};

MultiDateContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultiDate(this);
  }
};

MultiDateContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultiDate(this);
  }
};

MultiDateContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultiDate(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultiDateContext = MultiDateContext;

PslFilterParser.prototype.multiDate = function() {
  var localctx = new MultiDateContext(this, this._ctx, this.state);
  this.enterRule(localctx, 20, PslFilterParser.RULE_multiDate);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 92;
    this.match(PslFilterParser.DATE);
    this.state = 97;
    this._errHandler.sync(this);
    _la = this._input.LA(1);
    while (_la === PslFilterParser.T__2) {
      this.state = 93;
      this.match(PslFilterParser.T__2);
      this.state = 94;
      this.match(PslFilterParser.DATE);
      this.state = 99;
      this._errHandler.sync(this);
      _la = this._input.LA(1);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultiTimeStampContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multiTimeStamp;
  return this;
}

MultiTimeStampContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultiTimeStampContext.prototype.constructor = MultiTimeStampContext;

MultiTimeStampContext.prototype.TIMESTAMP = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTokens(PslFilterParser.TIMESTAMP);
  } else {
    return this.getToken(PslFilterParser.TIMESTAMP, i);
  }
};

MultiTimeStampContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultiTimeStamp(this);
  }
};

MultiTimeStampContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultiTimeStamp(this);
  }
};

MultiTimeStampContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultiTimeStamp(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultiTimeStampContext = MultiTimeStampContext;

PslFilterParser.prototype.multiTimeStamp = function() {
  var localctx = new MultiTimeStampContext(this, this._ctx, this.state);
  this.enterRule(localctx, 22, PslFilterParser.RULE_multiTimeStamp);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 100;
    this.match(PslFilterParser.TIMESTAMP);
    this.state = 105;
    this._errHandler.sync(this);
    _la = this._input.LA(1);
    while (_la === PslFilterParser.T__2) {
      this.state = 101;
      this.match(PslFilterParser.T__2);
      this.state = 102;
      this.match(PslFilterParser.TIMESTAMP);
      this.state = 107;
      this._errHandler.sync(this);
      _la = this._input.LA(1);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultiBoolContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multiBool;
  return this;
}

MultiBoolContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultiBoolContext.prototype.constructor = MultiBoolContext;

MultiBoolContext.prototype.BOOL = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTokens(PslFilterParser.BOOL);
  } else {
    return this.getToken(PslFilterParser.BOOL, i);
  }
};

MultiBoolContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultiBool(this);
  }
};

MultiBoolContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultiBool(this);
  }
};

MultiBoolContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultiBool(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultiBoolContext = MultiBoolContext;

PslFilterParser.prototype.multiBool = function() {
  var localctx = new MultiBoolContext(this, this._ctx, this.state);
  this.enterRule(localctx, 24, PslFilterParser.RULE_multiBool);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 108;
    this.match(PslFilterParser.BOOL);
    this.state = 113;
    this._errHandler.sync(this);
    _la = this._input.LA(1);
    while (_la === PslFilterParser.T__2) {
      this.state = 109;
      this.match(PslFilterParser.T__2);
      this.state = 110;
      this.match(PslFilterParser.BOOL);
      this.state = 115;
      this._errHandler.sync(this);
      _la = this._input.LA(1);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultiNumberContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multiNumber;
  return this;
}

MultiNumberContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultiNumberContext.prototype.constructor = MultiNumberContext;

MultiNumberContext.prototype.NUMBER = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTokens(PslFilterParser.NUMBER);
  } else {
    return this.getToken(PslFilterParser.NUMBER, i);
  }
};

MultiNumberContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultiNumber(this);
  }
};

MultiNumberContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultiNumber(this);
  }
};

MultiNumberContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultiNumber(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultiNumberContext = MultiNumberContext;

PslFilterParser.prototype.multiNumber = function() {
  var localctx = new MultiNumberContext(this, this._ctx, this.state);
  this.enterRule(localctx, 26, PslFilterParser.RULE_multiNumber);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 116;
    this.match(PslFilterParser.NUMBER);
    this.state = 121;
    this._errHandler.sync(this);
    _la = this._input.LA(1);
    while (_la === PslFilterParser.T__2) {
      this.state = 117;
      this.match(PslFilterParser.T__2);
      this.state = 118;
      this.match(PslFilterParser.NUMBER);
      this.state = 123;
      this._errHandler.sync(this);
      _la = this._input.LA(1);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function MultiStringContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_multiString;
  return this;
}

MultiStringContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
MultiStringContext.prototype.constructor = MultiStringContext;

MultiStringContext.prototype.SINGLEQUOTEDSTRINGLIT = function(i) {
  if (i === undefined) {
    i = null;
  }
  if (i === null) {
    return this.getTokens(PslFilterParser.SINGLEQUOTEDSTRINGLIT);
  } else {
    return this.getToken(PslFilterParser.SINGLEQUOTEDSTRINGLIT, i);
  }
};

MultiStringContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterMultiString(this);
  }
};

MultiStringContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitMultiString(this);
  }
};

MultiStringContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitMultiString(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.MultiStringContext = MultiStringContext;

PslFilterParser.prototype.multiString = function() {
  var localctx = new MultiStringContext(this, this._ctx, this.state);
  this.enterRule(localctx, 28, PslFilterParser.RULE_multiString);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 124;
    this.match(PslFilterParser.SINGLEQUOTEDSTRINGLIT);
    this.state = 129;
    this._errHandler.sync(this);
    _la = this._input.LA(1);
    while (_la === PslFilterParser.T__2) {
      this.state = 125;
      this.match(PslFilterParser.T__2);
      this.state = 126;
      this.match(PslFilterParser.SINGLEQUOTEDSTRINGLIT);
      this.state = 131;
      this._errHandler.sync(this);
      _la = this._input.LA(1);
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function UnaryOperationContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_unaryOperation;
  return this;
}

UnaryOperationContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
UnaryOperationContext.prototype.constructor = UnaryOperationContext;

UnaryOperationContext.prototype.IsNull = function() {
  return this.getToken(PslFilterParser.IsNull, 0);
};

UnaryOperationContext.prototype.IsNotNull = function() {
  return this.getToken(PslFilterParser.IsNotNull, 0);
};

UnaryOperationContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterUnaryOperation(this);
  }
};

UnaryOperationContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitUnaryOperation(this);
  }
};

UnaryOperationContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitUnaryOperation(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.UnaryOperationContext = UnaryOperationContext;

PslFilterParser.prototype.unaryOperation = function() {
  var localctx = new UnaryOperationContext(this, this._ctx, this.state);
  this.enterRule(localctx, 30, PslFilterParser.RULE_unaryOperation);
  var _la = 0; // Token type
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 132;
    _la = this._input.LA(1);
    if (!(_la === PslFilterParser.IsNull || _la === PslFilterParser.IsNotNull)) {
      this._errHandler.recoverInline(this);
    } else {
      this._errHandler.reportMatch(this);
      this.consume();
    }
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

function QueryParamContext(parser, parent, invokingState) {
  if (parent === undefined) {
    parent = null;
  }
  if (invokingState === undefined || invokingState === null) {
    invokingState = -1;
  }
  antlr4.ParserRuleContext.call(this, parent, invokingState);
  this.parser = parser;
  this.ruleIndex = PslFilterParser.RULE_queryParam;
  return this;
}

QueryParamContext.prototype = Object.create(antlr4.ParserRuleContext.prototype);
QueryParamContext.prototype.constructor = QueryParamContext;

QueryParamContext.prototype.LETTER_CHARS = function() {
  return this.getToken(PslFilterParser.LETTER_CHARS, 0);
};

QueryParamContext.prototype.enterRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.enterQueryParam(this);
  }
};

QueryParamContext.prototype.exitRule = function(listener) {
  if (listener instanceof PslFilterListener) {
    listener.exitQueryParam(this);
  }
};

QueryParamContext.prototype.accept = function(visitor) {
  if (visitor instanceof PslFilterVisitor) {
    return visitor.visitQueryParam(this);
  } else {
    return visitor.visitChildren(this);
  }
};

PslFilterParser.QueryParamContext = QueryParamContext;

PslFilterParser.prototype.queryParam = function() {
  var localctx = new QueryParamContext(this, this._ctx, this.state);
  this.enterRule(localctx, 32, PslFilterParser.RULE_queryParam);
  try {
    this.enterOuterAlt(localctx, 1);
    this.state = 134;
    this.match(PslFilterParser.T__3);
    this.state = 135;
    this.match(PslFilterParser.LETTER_CHARS);
  } catch (re) {
    if (re instanceof antlr4.error.RecognitionException) {
      localctx.exception = re;
      this._errHandler.reportError(this, re);
      this._errHandler.recover(this, re);
    } else {
      throw re;
    }
  } finally {
    this.exitRule();
  }
  return localctx;
};

PslFilterParser.prototype.sempred = function(localctx, ruleIndex, predIndex) {
  switch (ruleIndex) {
    case 1:
      return this.expr_sempred(localctx, predIndex);
    default:
      throw "No predicate with index:" + ruleIndex;
  }
};

PslFilterParser.prototype.expr_sempred = function(localctx, predIndex) {
  switch (predIndex) {
    case 0:
      return this.precpred(this._ctx, 4);
    case 1:
      return this.precpred(this._ctx, 3);
    default:
      throw "No predicate with index:" + predIndex;
  }
};

exports.PslFilterParser = PslFilterParser;
exports.FilterContext = FilterContext;
PslFilterParser.FilterContext = FilterContext;
exports.ExprContext = ExprContext;
PslFilterParser.ExprContext = ExprContext;
exports.OpContext = OpContext;
PslFilterParser.OpContext = OpContext;
exports.IdentifierContext = IdentifierContext;
PslFilterParser.IdentifierContext = IdentifierContext;
exports.BinaryOperationContext = BinaryOperationContext;
PslFilterParser.BinaryOperationContext = BinaryOperationContext;
exports.BinaryOperatorContext = BinaryOperatorContext;
PslFilterParser.BinaryOperatorContext = BinaryOperatorContext;
exports.ValueContext = ValueContext;
PslFilterParser.ValueContext = ValueContext;
exports.MultivalOperationContext = MultivalOperationContext;
PslFilterParser.MultivalOperationContext = MultivalOperationContext;
exports.MultivalOperatorContext = MultivalOperatorContext;
PslFilterParser.MultivalOperatorContext = MultivalOperatorContext;
exports.MultivalContext = MultivalContext;
PslFilterParser.MultivalContext = MultivalContext;
exports.MultiDateContext = MultiDateContext;
PslFilterParser.MultiDateContext = MultiDateContext;
exports.MultiTimeStampContext = MultiTimeStampContext;
PslFilterParser.MultiTimeStampContext = MultiTimeStampContext;
exports.MultiBoolContext = MultiBoolContext;
PslFilterParser.MultiBoolContext = MultiBoolContext;
exports.MultiNumberContext = MultiNumberContext;
PslFilterParser.MultiNumberContext = MultiNumberContext;
exports.MultiStringContext = MultiStringContext;
PslFilterParser.MultiStringContext = MultiStringContext;
exports.UnaryOperationContext = UnaryOperationContext;
PslFilterParser.UnaryOperationContext = UnaryOperationContext;
exports.QueryParamContext = QueryParamContext;
PslFilterParser.QueryParamContext = QueryParamContext;
