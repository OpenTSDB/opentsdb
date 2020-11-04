// Generated from /Users/chiruvol/Desktop/Desktop/TechOtros/codebases/new_yamas/opentsdb/core/src/main/antlr4/MetricExpression.g4 by ANTLR 4.7
import org.antlr.v4.runtime.Lexer;
import org.antlr.v4.runtime.CharStream;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.misc.*;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MetricExpressionLexer extends Lexer {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, TRUE=19, FALSE=20, REX=21, QUOTE=22, NUM=23, WS=24, BACKSLASH=25;
	public static String[] channelNames = {
		"DEFAULT_TOKEN_CHANNEL", "HIDDEN"
	};

	public static String[] modeNames = {
		"DEFAULT_MODE"
	};

	public static final String[] ruleNames = {
		"T__0", "T__1", "T__2", "T__3", "T__4", "T__5", "T__6", "T__7", "T__8", 
		"T__9", "T__10", "T__11", "T__12", "T__13", "T__14", "T__15", "T__16", 
		"T__17", "TRUE", "FALSE", "REX", "QUOTE", "NUM", "WS", "BACKSLASH", "A", 
		"B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", 
		"P", "Q", "R", "S", "T", "U", "V", "W", "X", "Y", "Z"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'%'", "'/'", "'*'", "'-'", "'+'", "'<'", "'>'", "'=='", 
		"'<='", "'>='", "'!='", "'?'", "':'", "'&&'", "'||'", "'!'", null, null, 
		null, "'(\"|')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, "TRUE", "FALSE", "REX", "QUOTE", 
		"NUM", "WS", "BACKSLASH"
	};
	public static final Vocabulary VOCABULARY = new VocabularyImpl(_LITERAL_NAMES, _SYMBOLIC_NAMES);

	/**
	 * @deprecated Use {@link #VOCABULARY} instead.
	 */
	@Deprecated
	public static final String[] tokenNames;
	static {
		tokenNames = new String[_SYMBOLIC_NAMES.length];
		for (int i = 0; i < tokenNames.length; i++) {
			tokenNames[i] = VOCABULARY.getLiteralName(i);
			if (tokenNames[i] == null) {
				tokenNames[i] = VOCABULARY.getSymbolicName(i);
			}

			if (tokenNames[i] == null) {
				tokenNames[i] = "<INVALID>";
			}
		}
	}

	@Override
	@Deprecated
	public String[] getTokenNames() {
		return tokenNames;
	}

	@Override

	public Vocabulary getVocabulary() {
		return VOCABULARY;
	}


	public MetricExpressionLexer(CharStream input) {
		super(input);
		_interp = new LexerATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}

	@Override
	public String getGrammarFileName() { return "MetricExpression.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public String[] getChannelNames() { return channelNames; }

	@Override
	public String[] getModeNames() { return modeNames; }

	@Override
	public ATN getATN() { return _ATN; }

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\2\33\u00fe\b\1\4\2"+
		"\t\2\4\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4"+
		"\13\t\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22"+
		"\t\22\4\23\t\23\4\24\t\24\4\25\t\25\4\26\t\26\4\27\t\27\4\30\t\30\4\31"+
		"\t\31\4\32\t\32\4\33\t\33\4\34\t\34\4\35\t\35\4\36\t\36\4\37\t\37\4 \t"+
		" \4!\t!\4\"\t\"\4#\t#\4$\t$\4%\t%\4&\t&\4\'\t\'\4(\t(\4)\t)\4*\t*\4+\t"+
		"+\4,\t,\4-\t-\4.\t.\4/\t/\4\60\t\60\4\61\t\61\4\62\t\62\4\63\t\63\4\64"+
		"\t\64\3\2\3\2\3\3\3\3\3\4\3\4\3\5\3\5\3\6\3\6\3\7\3\7\3\b\3\b\3\t\3\t"+
		"\3\n\3\n\3\13\3\13\3\13\3\f\3\f\3\f\3\r\3\r\3\r\3\16\3\16\3\16\3\17\3"+
		"\17\3\20\3\20\3\21\3\21\3\21\3\22\3\22\3\22\3\23\3\23\3\24\3\24\3\24\3"+
		"\24\3\24\3\25\3\25\3\25\3\25\3\25\3\25\3\26\3\26\7\26\u00a1\n\26\f\26"+
		"\16\26\u00a4\13\26\3\27\3\27\3\27\3\27\3\27\3\27\3\30\5\30\u00ad\n\30"+
		"\3\30\6\30\u00b0\n\30\r\30\16\30\u00b1\3\30\5\30\u00b5\n\30\3\30\7\30"+
		"\u00b8\n\30\f\30\16\30\u00bb\13\30\3\31\6\31\u00be\n\31\r\31\16\31\u00bf"+
		"\3\31\3\31\3\32\6\32\u00c5\n\32\r\32\16\32\u00c6\3\32\3\32\3\33\3\33\3"+
		"\34\3\34\3\35\3\35\3\36\3\36\3\37\3\37\3 \3 \3!\3!\3\"\3\"\3#\3#\3$\3"+
		"$\3%\3%\3&\3&\3\'\3\'\3(\3(\3)\3)\3*\3*\3+\3+\3,\3,\3-\3-\3.\3.\3/\3/"+
		"\3\60\3\60\3\61\3\61\3\62\3\62\3\63\3\63\3\64\3\64\2\2\65\3\3\5\4\7\5"+
		"\t\6\13\7\r\b\17\t\21\n\23\13\25\f\27\r\31\16\33\17\35\20\37\21!\22#\23"+
		"%\24\'\25)\26+\27-\30/\31\61\32\63\33\65\2\67\29\2;\2=\2?\2A\2C\2E\2G"+
		"\2I\2K\2M\2O\2Q\2S\2U\2W\2Y\2[\2]\2_\2a\2c\2e\2g\2\3\2#\5\2C\\aac|\7\2"+
		"/\60\62;C\\aac|\3\2//\3\2\62;\3\2\60\60\4\2\13\13\"\"\5\2\f\f\"\"^^\4"+
		"\2CCcc\4\2DDdd\4\2EEee\4\2FFff\4\2GGgg\4\2HHhh\4\2IIii\4\2JJjj\4\2KKk"+
		"k\4\2LLll\4\2MMmm\4\2NNnn\4\2OOoo\4\2PPpp\4\2QQqq\4\2RRrr\4\2SSss\4\2"+
		"TTtt\4\2UUuu\4\2VVvv\4\2WWww\4\2XXxx\4\2YYyy\4\2ZZzz\4\2[[{{\4\2\\\\|"+
		"|\2\u00ea\2\3\3\2\2\2\2\5\3\2\2\2\2\7\3\2\2\2\2\t\3\2\2\2\2\13\3\2\2\2"+
		"\2\r\3\2\2\2\2\17\3\2\2\2\2\21\3\2\2\2\2\23\3\2\2\2\2\25\3\2\2\2\2\27"+
		"\3\2\2\2\2\31\3\2\2\2\2\33\3\2\2\2\2\35\3\2\2\2\2\37\3\2\2\2\2!\3\2\2"+
		"\2\2#\3\2\2\2\2%\3\2\2\2\2\'\3\2\2\2\2)\3\2\2\2\2+\3\2\2\2\2-\3\2\2\2"+
		"\2/\3\2\2\2\2\61\3\2\2\2\2\63\3\2\2\2\3i\3\2\2\2\5k\3\2\2\2\7m\3\2\2\2"+
		"\to\3\2\2\2\13q\3\2\2\2\rs\3\2\2\2\17u\3\2\2\2\21w\3\2\2\2\23y\3\2\2\2"+
		"\25{\3\2\2\2\27~\3\2\2\2\31\u0081\3\2\2\2\33\u0084\3\2\2\2\35\u0087\3"+
		"\2\2\2\37\u0089\3\2\2\2!\u008b\3\2\2\2#\u008e\3\2\2\2%\u0091\3\2\2\2\'"+
		"\u0093\3\2\2\2)\u0098\3\2\2\2+\u009e\3\2\2\2-\u00a5\3\2\2\2/\u00ac\3\2"+
		"\2\2\61\u00bd\3\2\2\2\63\u00c4\3\2\2\2\65\u00ca\3\2\2\2\67\u00cc\3\2\2"+
		"\29\u00ce\3\2\2\2;\u00d0\3\2\2\2=\u00d2\3\2\2\2?\u00d4\3\2\2\2A\u00d6"+
		"\3\2\2\2C\u00d8\3\2\2\2E\u00da\3\2\2\2G\u00dc\3\2\2\2I\u00de\3\2\2\2K"+
		"\u00e0\3\2\2\2M\u00e2\3\2\2\2O\u00e4\3\2\2\2Q\u00e6\3\2\2\2S\u00e8\3\2"+
		"\2\2U\u00ea\3\2\2\2W\u00ec\3\2\2\2Y\u00ee\3\2\2\2[\u00f0\3\2\2\2]\u00f2"+
		"\3\2\2\2_\u00f4\3\2\2\2a\u00f6\3\2\2\2c\u00f8\3\2\2\2e\u00fa\3\2\2\2g"+
		"\u00fc\3\2\2\2ij\7*\2\2j\4\3\2\2\2kl\7+\2\2l\6\3\2\2\2mn\7\'\2\2n\b\3"+
		"\2\2\2op\7\61\2\2p\n\3\2\2\2qr\7,\2\2r\f\3\2\2\2st\7/\2\2t\16\3\2\2\2"+
		"uv\7-\2\2v\20\3\2\2\2wx\7>\2\2x\22\3\2\2\2yz\7@\2\2z\24\3\2\2\2{|\7?\2"+
		"\2|}\7?\2\2}\26\3\2\2\2~\177\7>\2\2\177\u0080\7?\2\2\u0080\30\3\2\2\2"+
		"\u0081\u0082\7@\2\2\u0082\u0083\7?\2\2\u0083\32\3\2\2\2\u0084\u0085\7"+
		"#\2\2\u0085\u0086\7?\2\2\u0086\34\3\2\2\2\u0087\u0088\7A\2\2\u0088\36"+
		"\3\2\2\2\u0089\u008a\7<\2\2\u008a \3\2\2\2\u008b\u008c\7(\2\2\u008c\u008d"+
		"\7(\2\2\u008d\"\3\2\2\2\u008e\u008f\7~\2\2\u008f\u0090\7~\2\2\u0090$\3"+
		"\2\2\2\u0091\u0092\7#\2\2\u0092&\3\2\2\2\u0093\u0094\5[.\2\u0094\u0095"+
		"\5W,\2\u0095\u0096\5]/\2\u0096\u0097\5=\37\2\u0097(\3\2\2\2\u0098\u0099"+
		"\5? \2\u0099\u009a\5\65\33\2\u009a\u009b\5K&\2\u009b\u009c\5Y-\2\u009c"+
		"\u009d\5=\37\2\u009d*\3\2\2\2\u009e\u00a2\t\2\2\2\u009f\u00a1\t\3\2\2"+
		"\u00a0\u009f\3\2\2\2\u00a1\u00a4\3\2\2\2\u00a2\u00a0\3\2\2\2\u00a2\u00a3"+
		"\3\2\2\2\u00a3,\3\2\2\2\u00a4\u00a2\3\2\2\2\u00a5\u00a6\7*\2\2\u00a6\u00a7"+
		"\7$\2\2\u00a7\u00a8\7~\2\2\u00a8\u00a9\7)\2\2\u00a9\u00aa\7+\2\2\u00aa"+
		".\3\2\2\2\u00ab\u00ad\t\4\2\2\u00ac\u00ab\3\2\2\2\u00ac\u00ad\3\2\2\2"+
		"\u00ad\u00af\3\2\2\2\u00ae\u00b0\t\5\2\2\u00af\u00ae\3\2\2\2\u00b0\u00b1"+
		"\3\2\2\2\u00b1\u00af\3\2\2\2\u00b1\u00b2\3\2\2\2\u00b2\u00b4\3\2\2\2\u00b3"+
		"\u00b5\t\6\2\2\u00b4\u00b3\3\2\2\2\u00b4\u00b5\3\2\2\2\u00b5\u00b9\3\2"+
		"\2\2\u00b6\u00b8\t\5\2\2\u00b7\u00b6\3\2\2\2\u00b8\u00bb\3\2\2\2\u00b9"+
		"\u00b7\3\2\2\2\u00b9\u00ba\3\2\2\2\u00ba\60\3\2\2\2\u00bb\u00b9\3\2\2"+
		"\2\u00bc\u00be\t\7\2\2\u00bd\u00bc\3\2\2\2\u00be\u00bf\3\2\2\2\u00bf\u00bd"+
		"\3\2\2\2\u00bf\u00c0\3\2\2\2\u00c0\u00c1\3\2\2\2\u00c1\u00c2\b\31\2\2"+
		"\u00c2\62\3\2\2\2\u00c3\u00c5\t\b\2\2\u00c4\u00c3\3\2\2\2\u00c5\u00c6"+
		"\3\2\2\2\u00c6\u00c4\3\2\2\2\u00c6\u00c7\3\2\2\2\u00c7\u00c8\3\2\2\2\u00c8"+
		"\u00c9\b\32\2\2\u00c9\64\3\2\2\2\u00ca\u00cb\t\t\2\2\u00cb\66\3\2\2\2"+
		"\u00cc\u00cd\t\n\2\2\u00cd8\3\2\2\2\u00ce\u00cf\t\13\2\2\u00cf:\3\2\2"+
		"\2\u00d0\u00d1\t\f\2\2\u00d1<\3\2\2\2\u00d2\u00d3\t\r\2\2\u00d3>\3\2\2"+
		"\2\u00d4\u00d5\t\16\2\2\u00d5@\3\2\2\2\u00d6\u00d7\t\17\2\2\u00d7B\3\2"+
		"\2\2\u00d8\u00d9\t\20\2\2\u00d9D\3\2\2\2\u00da\u00db\t\21\2\2\u00dbF\3"+
		"\2\2\2\u00dc\u00dd\t\22\2\2\u00ddH\3\2\2\2\u00de\u00df\t\23\2\2\u00df"+
		"J\3\2\2\2\u00e0\u00e1\t\24\2\2\u00e1L\3\2\2\2\u00e2\u00e3\t\25\2\2\u00e3"+
		"N\3\2\2\2\u00e4\u00e5\t\26\2\2\u00e5P\3\2\2\2\u00e6\u00e7\t\27\2\2\u00e7"+
		"R\3\2\2\2\u00e8\u00e9\t\30\2\2\u00e9T\3\2\2\2\u00ea\u00eb\t\31\2\2\u00eb"+
		"V\3\2\2\2\u00ec\u00ed\t\32\2\2\u00edX\3\2\2\2\u00ee\u00ef\t\33\2\2\u00ef"+
		"Z\3\2\2\2\u00f0\u00f1\t\34\2\2\u00f1\\\3\2\2\2\u00f2\u00f3\t\35\2\2\u00f3"+
		"^\3\2\2\2\u00f4\u00f5\t\36\2\2\u00f5`\3\2\2\2\u00f6\u00f7\t\37\2\2\u00f7"+
		"b\3\2\2\2\u00f8\u00f9\t \2\2\u00f9d\3\2\2\2\u00fa\u00fb\t!\2\2\u00fbf"+
		"\3\2\2\2\u00fc\u00fd\t\"\2\2\u00fdh\3\2\2\2\n\2\u00a2\u00ac\u00b1\u00b4"+
		"\u00b9\u00bf\u00c6\3\b\2\2";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}