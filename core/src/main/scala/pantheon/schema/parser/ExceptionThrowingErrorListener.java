package pantheon.schema.parser;

import org.antlr.v4.runtime.ANTLRErrorListener;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.ATNConfigSet;
import org.antlr.v4.runtime.dfa.DFA;
import pantheon.schema.parser.SyntaxError;

import java.util.BitSet;

// Extending this class in Java because of https://issues.scala-lang.org/browse/SI-10155
public class ExceptionThrowingErrorListener implements ANTLRErrorListener {

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        throw SyntaxError.apply(line, charPositionInLine, msg + " at line:" +  line + ", charPosition:" + charPositionInLine);
    }

    @Override
    public void reportAmbiguity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, boolean exact, BitSet ambigAlts, ATNConfigSet configs) {
        // TODO: IMPLEMENT ME IF NEEDED
    }
    public Exception a = new Exception();
    @Override
    public void reportAttemptingFullContext(Parser recognizer, DFA dfa, int startIndex, int stopIndex, BitSet conflictingAlts, ATNConfigSet configs) {
        // TODO: IMPLEMENT ME IF NEEDED
    }

    @Override
    public void reportContextSensitivity(Parser recognizer, DFA dfa, int startIndex, int stopIndex, int prediction, ATNConfigSet configs) {
        // TODO: IMPLEMENT ME IF NEEDED
    }
}