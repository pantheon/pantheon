import { Recognizer, Token } from "antlr4";
import { ErrorListener } from "antlr4/error/ErrorListener";

export interface Error {
  message: string;
  startLineNumber: number;
  endLineNumber: number;
  startColumn: number;
  endColumn: number;
}

export class PslFilterErrorListener extends ErrorListener {
  public errors: Error[] = [];

  public syntaxError(_: Recognizer, offendingSymbol: Token, line: number, column: number, msg: string) {
    this.errors.push({
      message: msg,
      startLineNumber: line,
      endLineNumber: line,
      startColumn: column + 1,
      endColumn: column + offendingSymbol.stop - offendingSymbol.start + 2, // Assume that we have only one line error
    });
  }
}
