import { CommonTokenStream, InputStream } from "antlr4";
import memoize from "lodash/memoize";
import { PslFilterLexer } from "./parser/PslFilterLexer";
import { PslFilterListener } from "./parser/PslFilterListener";
import { ExprContext, OpContext, PslFilterParser } from "./parser/PslFilterParser";
import { PslFilterErrorListener } from "./parserFilter.errorListener";

export const operators = {
  "=": true,
  ">": true,
  ">=": true,
  "<": true,
  "<=": true,
  "!=": true,
  "is null": true,
  "is not null": true,
  "not in": true,
  in: true,
  like: true,
};

export type Operator = keyof typeof operators;

export interface FilterTreePredicateNode {
  type: "predicate";
  dimension: string;
  operator: Operator;
  value?: string;
}

export const unmatchedParenthesis = NaN;
export const isUnmatchedParenthesis = Number.isNaN;

export interface FilterTreeOperatorNode {
  type: "operator";
  closeParentheses: number[]; // parenthesis id or unmatchedParenthesis
  operator?: "and" | "or";
  openParentheses: number[]; // parenthesis id or unmatchedParenthesis
}

type FilterTreeNode = FilterTreePredicateNode | FilterTreeOperatorNode;

export type FilterTree = FilterTreeNode[];

export const isOperator = (node: FilterTreeNode): node is FilterTreeOperatorNode => {
  return node.type === "operator";
};

export const isPredicate = (node: FilterTreeNode): node is FilterTreePredicateNode => {
  return node.type === "predicate";
};

class TreeExtrator extends PslFilterListener {
  public tree: FilterTreeNode[] = [{ type: "operator", closeParentheses: [], openParentheses: [] }];

  private exprStartIndices: number[] = [];
  private parenthesisId = 0;

  public enterExpr() {
    this.exprStartIndices.push(this.tree.length - 1);
  }

  public exitExpr(node: ExprContext) {
    const startIndex = this.exprStartIndices.pop();

    // Add "and" operator
    if (node.AND()) {
      const operatorNode = this.tree[startIndex || this.tree.length - 3];
      if (operatorNode.type === "operator") {
        operatorNode.operator = "and";
      }
    }

    // Add "or" operator
    if (node.OR()) {
      const operatorNode = this.tree[startIndex || this.tree.length - 3];
      if (operatorNode.type === "operator") {
        operatorNode.operator = "or";
      }
    }

    // Deal with parentheses
    if (node.getChildCount() === 3 && node.getChild(0).getText() === "(" && node.getChild(2).getText() === ")") {
      const startOperatorNode = this.tree[startIndex || 0];
      if (startOperatorNode.type === "operator") {
        startOperatorNode.openParentheses.unshift(this.parenthesisId);
      }
      const endOperatorNode = this.tree[this.tree.length - 1];
      if (endOperatorNode.type === "operator") {
        endOperatorNode.closeParentheses.push(this.parenthesisId);
      }
      this.parenthesisId++;
    }
  }

  public exitOp(node: OpContext) {
    this.tree.push(
      {
        type: "predicate",
        dimension: node.identifier().getText(),
        operator: this.getPredicateOperator(node),
        value: this.getPredicateValue(node),
      },
      { type: "operator", openParentheses: [], closeParentheses: [] },
    );
  }

  private getPredicateOperator(node: OpContext) {
    if (node.binaryOperation()) {
      return node
        .binaryOperation()
        .binaryOperator()
        .getText() as Operator;
    }

    if (node.multivalOperation()) {
      return node
        .multivalOperation()
        .multivalOperator()
        .getText() as Operator;
    }

    return node.unaryOperation().getText() as Operator;
  }

  private getPredicateValue(node: OpContext) {
    if (node.binaryOperation()) {
      return node
        .binaryOperation()
        .value()
        .getText();
    }

    if (node.multivalOperation()) {
      return node
        .multivalOperation()
        .multival()
        .getText()
        .slice(1, -1) // remove start and end parentheses
        .split(",")
        .join(", "); // pretiffy the list
    }

    return undefined;
  }
}

/**
 * Return an easy to consume object from filter.
 *
 * @param input filter string
 */
export const parseFilter = (input: string): FilterTree => {
  if (input === "") {
    return [];
  }

  const inputStream = new InputStream(input);
  const lexer = new PslFilterLexer(inputStream);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new PslFilterParser(tokenStream);
  const treeExtractor = new TreeExtrator();
  parser.removeErrorListeners();
  parser.addParseListener(treeExtractor);
  parser.filter();
  return treeExtractor.tree;
};

/**
 * Return the filter as a string.
 *
 * @param input filter tree
 */
export const printFilter = (input: FilterTree): string =>
  input
    .map(item => {
      if (item.type === "operator") {
        return (
          ")".repeat(item.closeParentheses.length) +
          (item.operator ? ` ${item.operator} ` : "") +
          "(".repeat(item.openParentheses.length)
        );
      } else {
        if (["in", "not in", "like"].includes(item.operator)) {
          return `${item.dimension} ${item.operator} ${item.value ? `(${item.value})` : ""}`;
        } else {
          return `${item.dimension} ${item.operator} ${item.value ? `${item.value}` : ""}`;
        }
      }
    })
    .join("")
    .trim();

/**
 * Report errors on the given filter.
 *
 * @param input filter string
 */
export const validateFilter = (input: string) => {
  const inputStream = new InputStream(input);
  const lexer = new PslFilterLexer(inputStream);
  const tokenStream = new CommonTokenStream(lexer);
  const parser = new PslFilterParser(tokenStream);
  const errorListener = new PslFilterErrorListener();
  parser.removeErrorListeners();
  parser.addErrorListener(errorListener);
  parser.filter();
  return errorListener.errors.length ? errorListener.errors : false;
};

export default {
  stringify: memoize(printFilter),
  parse: memoize(parseFilter),
};

/**
 * PslFilter value auto-fixer
 *
 * @param value filter value
 */
export const fixFilterValue = (value?: string): string | undefined => {
  if (value === undefined) {
    return value;
  } else if (value.includes("'") && value.split("").filter(i => i === "'").length % 2 === 0) {
    return value; // value correctly quoted
  } else if (value.split("").filter(i => i === "'").length === 1) {
    return `'${value.replace("'", "")}'`; // missing quote
  } else if (value.includes('"') && value.split("").filter(i => i === '"').length % 2 === 0) {
    return value.replace(/"/g, "'"); // double quote instead of single
  } else if (value.includes(",")) {
    return value
      .replace(/ /g, "")
      .split(",")
      .map(fixFilterValue)
      .join(", ");
  } else if (value !== "true" && value !== "false" && Number.isNaN(+value)) {
    return `'${value}'`; // string without quotes
  } else {
    return value;
  }
};
