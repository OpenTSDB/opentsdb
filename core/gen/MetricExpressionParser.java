// Generated from /Users/chiruvol/Desktop/Desktop/TechOtros/codebases/new_yamas/opentsdb/core/src/main/antlr4/MetricExpression.g4 by ANTLR 4.7
import org.antlr.v4.runtime.atn.*;
import org.antlr.v4.runtime.dfa.DFA;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.misc.*;
import org.antlr.v4.runtime.tree.*;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

@SuppressWarnings({"all", "warnings", "unchecked", "unused", "cast"})
public class MetricExpressionParser extends Parser {
	static { RuntimeMetaData.checkVersion("4.7", RuntimeMetaData.VERSION); }

	protected static final DFA[] _decisionToDFA;
	protected static final PredictionContextCache _sharedContextCache =
		new PredictionContextCache();
	public static final int
		T__0=1, T__1=2, T__2=3, T__3=4, T__4=5, T__5=6, T__6=7, T__7=8, T__8=9, 
		T__9=10, T__10=11, T__11=12, T__12=13, T__13=14, T__14=15, T__15=16, T__16=17, 
		T__17=18, TRUE=19, FALSE=20, REX=21, QUOTE=22, NUM=23, WS=24, BACKSLASH=25, 
		A=26, N=27, D=28, O=29, R=30, T=31;
	public static final int
		RULE_prog = 0, RULE_expression = 1, RULE_logicalExpression = 2, RULE_logicalOperands = 3, 
		RULE_arthmeticExpression = 4, RULE_arithmeticOperands = 5, RULE_relationalExpression = 6, 
		RULE_relationalOperands = 7, RULE_logicop = 8, RULE_relationalop = 9, 
		RULE_ternaryExpression = 10, RULE_ternaryOperands = 11, RULE_and = 12, 
		RULE_or = 13, RULE_not = 14, RULE_modulo = 15, RULE_metric = 16;
	public static final String[] ruleNames = {
		"prog", "expression", "logicalExpression", "logicalOperands", "arthmeticExpression", 
		"arithmeticOperands", "relationalExpression", "relationalOperands", "logicop", 
		"relationalop", "ternaryExpression", "ternaryOperands", "and", "or", "not", 
		"modulo", "metric"
	};

	private static final String[] _LITERAL_NAMES = {
		null, "'('", "')'", "'%'", "'/'", "'*'", "'-'", "'+'", "'<'", "'>'", "'=='", 
		"'<='", "'>='", "'!='", "'?'", "':'", "'&&'", "'||'", "'!'", null, null, 
		null, "'(\"|')'"
	};
	private static final String[] _SYMBOLIC_NAMES = {
		null, null, null, null, null, null, null, null, null, null, null, null, 
		null, null, null, null, null, null, null, "TRUE", "FALSE", "REX", "QUOTE", 
		"NUM", "WS", "BACKSLASH", "A", "N", "D", "O", "R", "T"
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

	@Override
	public String getGrammarFileName() { return "MetricExpression.g4"; }

	@Override
	public String[] getRuleNames() { return ruleNames; }

	@Override
	public String getSerializedATN() { return _serializedATN; }

	@Override
	public ATN getATN() { return _ATN; }

	public MetricExpressionParser(TokenStream input) {
		super(input);
		_interp = new ParserATNSimulator(this,_ATN,_decisionToDFA,_sharedContextCache);
	}
	public static class ProgContext extends ParserRuleContext {
		public ExpressionContext expression() {
			return getRuleContext(ExpressionContext.class,0);
		}
		public ProgContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_prog; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterProg(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitProg(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitProg(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ProgContext prog() throws RecognitionException {
		ProgContext _localctx = new ProgContext(_ctx, getState());
		enterRule(_localctx, 0, RULE_prog);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(34);
			expression();
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ExpressionContext extends ParserRuleContext {
		public ExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_expression; }
	 
		public ExpressionContext() { }
		public void copyFrom(ExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class ArithmeticContext extends ExpressionContext {
		public ArthmeticExpressionContext arthmeticExpression() {
			return getRuleContext(ArthmeticExpressionContext.class,0);
		}
		public ArithmeticContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterArithmetic(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitArithmetic(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitArithmetic(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class RelationalContext extends ExpressionContext {
		public RelationalExpressionContext relationalExpression() {
			return getRuleContext(RelationalExpressionContext.class,0);
		}
		public RelationalContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterRelational(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitRelational(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitRelational(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class TernaryContext extends ExpressionContext {
		public TernaryExpressionContext ternaryExpression() {
			return getRuleContext(TernaryExpressionContext.class,0);
		}
		public TernaryContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterTernary(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitTernary(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitTernary(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class LogicalContext extends ExpressionContext {
		public LogicalExpressionContext logicalExpression() {
			return getRuleContext(LogicalExpressionContext.class,0);
		}
		public LogicalContext(ExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogical(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogical(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogical(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ExpressionContext expression() throws RecognitionException {
		ExpressionContext _localctx = new ExpressionContext(_ctx, getState());
		enterRule(_localctx, 2, RULE_expression);
		try {
			setState(40);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,0,_ctx) ) {
			case 1:
				_localctx = new ArithmeticContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(36);
				arthmeticExpression(0);
				}
				break;
			case 2:
				_localctx = new LogicalContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(37);
				logicalExpression(0);
				}
				break;
			case 3:
				_localctx = new RelationalContext(_localctx);
				enterOuterAlt(_localctx, 3);
				{
				setState(38);
				relationalExpression();
				}
				break;
			case 4:
				_localctx = new TernaryContext(_localctx);
				enterOuterAlt(_localctx, 4);
				{
				setState(39);
				ternaryExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogicalExpressionContext extends ParserRuleContext {
		public LogicalExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicalExpression; }
	 
		public LogicalExpressionContext() { }
		public void copyFrom(LogicalExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Paren_logical_ruleContext extends LogicalExpressionContext {
		public LogicalExpressionContext logicalExpression() {
			return getRuleContext(LogicalExpressionContext.class,0);
		}
		public Paren_logical_ruleContext(LogicalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterParen_logical_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitParen_logical_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitParen_logical_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Logical_operands_ruleContext extends LogicalExpressionContext {
		public LogicalOperandsContext logicalOperands() {
			return getRuleContext(LogicalOperandsContext.class,0);
		}
		public Logical_operands_ruleContext(LogicalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogical_operands_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogical_operands_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogical_operands_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Logical_expr_and_ruleContext extends LogicalExpressionContext {
		public List<LogicalExpressionContext> logicalExpression() {
			return getRuleContexts(LogicalExpressionContext.class);
		}
		public LogicalExpressionContext logicalExpression(int i) {
			return getRuleContext(LogicalExpressionContext.class,i);
		}
		public AndContext and() {
			return getRuleContext(AndContext.class,0);
		}
		public Logical_expr_and_ruleContext(LogicalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogical_expr_and_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogical_expr_and_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogical_expr_and_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Logical_expr_or_ruleContext extends LogicalExpressionContext {
		public List<LogicalExpressionContext> logicalExpression() {
			return getRuleContexts(LogicalExpressionContext.class);
		}
		public LogicalExpressionContext logicalExpression(int i) {
			return getRuleContext(LogicalExpressionContext.class,i);
		}
		public OrContext or() {
			return getRuleContext(OrContext.class,0);
		}
		public Logical_expr_or_ruleContext(LogicalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogical_expr_or_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogical_expr_or_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogical_expr_or_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Logical_expr_not_ruleContext extends LogicalExpressionContext {
		public NotContext not() {
			return getRuleContext(NotContext.class,0);
		}
		public LogicalExpressionContext logicalExpression() {
			return getRuleContext(LogicalExpressionContext.class,0);
		}
		public Logical_expr_not_ruleContext(LogicalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogical_expr_not_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogical_expr_not_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogical_expr_not_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicalExpressionContext logicalExpression() throws RecognitionException {
		return logicalExpression(0);
	}

	private LogicalExpressionContext logicalExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		LogicalExpressionContext _localctx = new LogicalExpressionContext(_ctx, _parentState);
		LogicalExpressionContext _prevctx = _localctx;
		int _startState = 4;
		enterRecursionRule(_localctx, 4, RULE_logicalExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(51);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,1,_ctx) ) {
			case 1:
				{
				_localctx = new Paren_logical_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(43);
				match(T__0);
				setState(44);
				logicalExpression(0);
				setState(45);
				match(T__1);
				}
				break;
			case 2:
				{
				_localctx = new Logical_expr_not_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(47);
				not();
				setState(48);
				logicalExpression(4);
				}
				break;
			case 3:
				{
				_localctx = new Logical_operands_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(50);
				logicalOperands();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(63);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(61);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,2,_ctx) ) {
					case 1:
						{
						_localctx = new Logical_expr_and_ruleContext(new LogicalExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_logicalExpression);
						setState(53);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(54);
						and();
						setState(55);
						logicalExpression(4);
						}
						break;
					case 2:
						{
						_localctx = new Logical_expr_or_ruleContext(new LogicalExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_logicalExpression);
						setState(57);
						if (!(precpred(_ctx, 2))) throw new FailedPredicateException(this, "precpred(_ctx, 2)");
						setState(58);
						or();
						setState(59);
						logicalExpression(3);
						}
						break;
					}
					} 
				}
				setState(65);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,3,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class LogicalOperandsContext extends ParserRuleContext {
		public RelationalExpressionContext relationalExpression() {
			return getRuleContext(RelationalExpressionContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(MetricExpressionParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(MetricExpressionParser.FALSE, 0); }
		public LogicalOperandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicalOperands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogicalOperands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogicalOperands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogicalOperands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicalOperandsContext logicalOperands() throws RecognitionException {
		LogicalOperandsContext _localctx = new LogicalOperandsContext(_ctx, getState());
		enterRule(_localctx, 6, RULE_logicalOperands);
		try {
			setState(69);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,4,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(66);
				relationalExpression();
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(67);
				match(TRUE);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(68);
				match(FALSE);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ArthmeticExpressionContext extends ParserRuleContext {
		public ArthmeticExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arthmeticExpression; }
	 
		public ArthmeticExpressionContext() { }
		public void copyFrom(ArthmeticExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Arith_operands_ruleContext extends ArthmeticExpressionContext {
		public ArithmeticOperandsContext arithmeticOperands() {
			return getRuleContext(ArithmeticOperandsContext.class,0);
		}
		public Arith_operands_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterArith_operands_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitArith_operands_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitArith_operands_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Paren_arith_ruleContext extends ArthmeticExpressionContext {
		public ArthmeticExpressionContext arthmeticExpression() {
			return getRuleContext(ArthmeticExpressionContext.class,0);
		}
		public Paren_arith_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterParen_arith_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitParen_arith_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitParen_arith_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Minus_metric_ruleContext extends ArthmeticExpressionContext {
		public MetricContext metric() {
			return getRuleContext(MetricContext.class,0);
		}
		public Minus_metric_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterMinus_metric_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitMinus_metric_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitMinus_metric_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Mod_arith_ruleContext extends ArthmeticExpressionContext {
		public List<ArthmeticExpressionContext> arthmeticExpression() {
			return getRuleContexts(ArthmeticExpressionContext.class);
		}
		public ArthmeticExpressionContext arthmeticExpression(int i) {
			return getRuleContext(ArthmeticExpressionContext.class,i);
		}
		public Mod_arith_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterMod_arith_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitMod_arith_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitMod_arith_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Addsub_arith_ruleContext extends ArthmeticExpressionContext {
		public List<ArthmeticExpressionContext> arthmeticExpression() {
			return getRuleContexts(ArthmeticExpressionContext.class);
		}
		public ArthmeticExpressionContext arthmeticExpression(int i) {
			return getRuleContext(ArthmeticExpressionContext.class,i);
		}
		public Addsub_arith_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterAddsub_arith_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitAddsub_arith_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitAddsub_arith_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Divmul_arith_ruleContext extends ArthmeticExpressionContext {
		public List<ArthmeticExpressionContext> arthmeticExpression() {
			return getRuleContexts(ArthmeticExpressionContext.class);
		}
		public ArthmeticExpressionContext arthmeticExpression(int i) {
			return getRuleContext(ArthmeticExpressionContext.class,i);
		}
		public Divmul_arith_ruleContext(ArthmeticExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterDivmul_arith_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitDivmul_arith_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitDivmul_arith_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArthmeticExpressionContext arthmeticExpression() throws RecognitionException {
		return arthmeticExpression(0);
	}

	private ArthmeticExpressionContext arthmeticExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		ArthmeticExpressionContext _localctx = new ArthmeticExpressionContext(_ctx, _parentState);
		ArthmeticExpressionContext _prevctx = _localctx;
		int _startState = 8;
		enterRecursionRule(_localctx, 8, RULE_arthmeticExpression, _p);
		int _la;
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(79);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
				{
				_localctx = new Paren_arith_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(72);
				match(T__0);
				setState(73);
				arthmeticExpression(0);
				setState(74);
				match(T__1);
				}
				break;
			case T__5:
				{
				_localctx = new Minus_metric_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(76);
				match(T__5);
				setState(77);
				metric();
				}
				break;
			case REX:
			case NUM:
				{
				_localctx = new Arith_operands_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(78);
				arithmeticOperands();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			_ctx.stop = _input.LT(-1);
			setState(92);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					setState(90);
					_errHandler.sync(this);
					switch ( getInterpreter().adaptivePredict(_input,6,_ctx) ) {
					case 1:
						{
						_localctx = new Mod_arith_ruleContext(new ArthmeticExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_arthmeticExpression);
						setState(81);
						if (!(precpred(_ctx, 5))) throw new FailedPredicateException(this, "precpred(_ctx, 5)");
						setState(82);
						match(T__2);
						setState(83);
						arthmeticExpression(6);
						}
						break;
					case 2:
						{
						_localctx = new Divmul_arith_ruleContext(new ArthmeticExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_arthmeticExpression);
						setState(84);
						if (!(precpred(_ctx, 4))) throw new FailedPredicateException(this, "precpred(_ctx, 4)");
						setState(85);
						_la = _input.LA(1);
						if ( !(_la==T__3 || _la==T__4) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(86);
						arthmeticExpression(5);
						}
						break;
					case 3:
						{
						_localctx = new Addsub_arith_ruleContext(new ArthmeticExpressionContext(_parentctx, _parentState));
						pushNewRecursionContext(_localctx, _startState, RULE_arthmeticExpression);
						setState(87);
						if (!(precpred(_ctx, 3))) throw new FailedPredicateException(this, "precpred(_ctx, 3)");
						setState(88);
						_la = _input.LA(1);
						if ( !(_la==T__5 || _la==T__6) ) {
						_errHandler.recoverInline(this);
						}
						else {
							if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
							_errHandler.reportMatch(this);
							consume();
						}
						setState(89);
						arthmeticExpression(4);
						}
						break;
					}
					} 
				}
				setState(94);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,7,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class ArithmeticOperandsContext extends ParserRuleContext {
		public ArithmeticOperandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_arithmeticOperands; }
	 
		public ArithmeticOperandsContext() { }
		public void copyFrom(ArithmeticOperandsContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Arithmetic_operands_ruleContext extends ArithmeticOperandsContext {
		public MetricContext metric() {
			return getRuleContext(MetricContext.class,0);
		}
		public TerminalNode NUM() { return getToken(MetricExpressionParser.NUM, 0); }
		public Arithmetic_operands_ruleContext(ArithmeticOperandsContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterArithmetic_operands_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitArithmetic_operands_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitArithmetic_operands_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ArithmeticOperandsContext arithmeticOperands() throws RecognitionException {
		ArithmeticOperandsContext _localctx = new ArithmeticOperandsContext(_ctx, getState());
		enterRule(_localctx, 10, RULE_arithmeticOperands);
		try {
			_localctx = new Arithmetic_operands_ruleContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(97);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case REX:
				{
				setState(95);
				metric();
				}
				break;
			case NUM:
				{
				setState(96);
				match(NUM);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationalExpressionContext extends ParserRuleContext {
		public RelationalExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationalExpression; }
	 
		public RelationalExpressionContext() { }
		public void copyFrom(RelationalExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Main_relational_ruleContext extends RelationalExpressionContext {
		public List<RelationalOperandsContext> relationalOperands() {
			return getRuleContexts(RelationalOperandsContext.class);
		}
		public RelationalOperandsContext relationalOperands(int i) {
			return getRuleContext(RelationalOperandsContext.class,i);
		}
		public RelationalopContext relationalop() {
			return getRuleContext(RelationalopContext.class,0);
		}
		public Main_relational_ruleContext(RelationalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterMain_relational_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitMain_relational_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitMain_relational_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Paren_relational_ruleContext extends RelationalExpressionContext {
		public RelationalExpressionContext relationalExpression() {
			return getRuleContext(RelationalExpressionContext.class,0);
		}
		public Paren_relational_ruleContext(RelationalExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterParen_relational_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitParen_relational_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitParen_relational_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationalExpressionContext relationalExpression() throws RecognitionException {
		RelationalExpressionContext _localctx = new RelationalExpressionContext(_ctx, getState());
		enterRule(_localctx, 12, RULE_relationalExpression);
		try {
			setState(107);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,9,_ctx) ) {
			case 1:
				_localctx = new Paren_relational_ruleContext(_localctx);
				enterOuterAlt(_localctx, 1);
				{
				setState(99);
				match(T__0);
				setState(100);
				relationalExpression();
				setState(101);
				match(T__1);
				}
				break;
			case 2:
				_localctx = new Main_relational_ruleContext(_localctx);
				enterOuterAlt(_localctx, 2);
				{
				setState(103);
				relationalOperands();
				setState(104);
				relationalop();
				setState(105);
				relationalOperands();
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationalOperandsContext extends ParserRuleContext {
		public RelationalOperandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationalOperands; }
	 
		public RelationalOperandsContext() { }
		public void copyFrom(RelationalOperandsContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Relational_operands_ruleContext extends RelationalOperandsContext {
		public ArthmeticExpressionContext arthmeticExpression() {
			return getRuleContext(ArthmeticExpressionContext.class,0);
		}
		public TerminalNode TRUE() { return getToken(MetricExpressionParser.TRUE, 0); }
		public TerminalNode FALSE() { return getToken(MetricExpressionParser.FALSE, 0); }
		public Relational_operands_ruleContext(RelationalOperandsContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterRelational_operands_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitRelational_operands_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitRelational_operands_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationalOperandsContext relationalOperands() throws RecognitionException {
		RelationalOperandsContext _localctx = new RelationalOperandsContext(_ctx, getState());
		enterRule(_localctx, 14, RULE_relationalOperands);
		try {
			_localctx = new Relational_operands_ruleContext(_localctx);
			enterOuterAlt(_localctx, 1);
			{
			setState(112);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__0:
			case T__5:
			case REX:
			case NUM:
				{
				setState(109);
				arthmeticExpression(0);
				}
				break;
			case TRUE:
				{
				setState(110);
				match(TRUE);
				}
				break;
			case FALSE:
				{
				setState(111);
				match(FALSE);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class LogicopContext extends ParserRuleContext {
		public AndContext and() {
			return getRuleContext(AndContext.class,0);
		}
		public OrContext or() {
			return getRuleContext(OrContext.class,0);
		}
		public NotContext not() {
			return getRuleContext(NotContext.class,0);
		}
		public LogicopContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_logicop; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterLogicop(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitLogicop(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitLogicop(this);
			else return visitor.visitChildren(this);
		}
	}

	public final LogicopContext logicop() throws RecognitionException {
		LogicopContext _localctx = new LogicopContext(_ctx, getState());
		enterRule(_localctx, 16, RULE_logicop);
		try {
			setState(117);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case T__15:
			case A:
				enterOuterAlt(_localctx, 1);
				{
				setState(114);
				and();
				}
				break;
			case T__16:
			case O:
				enterOuterAlt(_localctx, 2);
				{
				setState(115);
				or();
				}
				break;
			case T__17:
			case N:
				enterOuterAlt(_localctx, 3);
				{
				setState(116);
				not();
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class RelationalopContext extends ParserRuleContext {
		public RelationalopContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_relationalop; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterRelationalop(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitRelationalop(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitRelationalop(this);
			else return visitor.visitChildren(this);
		}
	}

	public final RelationalopContext relationalop() throws RecognitionException {
		RelationalopContext _localctx = new RelationalopContext(_ctx, getState());
		enterRule(_localctx, 18, RULE_relationalop);
		int _la;
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(119);
			_la = _input.LA(1);
			if ( !((((_la) & ~0x3f) == 0 && ((1L << _la) & ((1L << T__7) | (1L << T__8) | (1L << T__9) | (1L << T__10) | (1L << T__11) | (1L << T__12))) != 0)) ) {
			_errHandler.recoverInline(this);
			}
			else {
				if ( _input.LA(1)==Token.EOF ) matchedEOF = true;
				_errHandler.reportMatch(this);
				consume();
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class TernaryExpressionContext extends ParserRuleContext {
		public TernaryExpressionContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ternaryExpression; }
	 
		public TernaryExpressionContext() { }
		public void copyFrom(TernaryExpressionContext ctx) {
			super.copyFrom(ctx);
		}
	}
	public static class Main_ternary_ruleContext extends TernaryExpressionContext {
		public List<TernaryOperandsContext> ternaryOperands() {
			return getRuleContexts(TernaryOperandsContext.class);
		}
		public TernaryOperandsContext ternaryOperands(int i) {
			return getRuleContext(TernaryOperandsContext.class,i);
		}
		public LogicalExpressionContext logicalExpression() {
			return getRuleContext(LogicalExpressionContext.class,0);
		}
		public RelationalExpressionContext relationalExpression() {
			return getRuleContext(RelationalExpressionContext.class,0);
		}
		public MetricContext metric() {
			return getRuleContext(MetricContext.class,0);
		}
		public Main_ternary_ruleContext(TernaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterMain_ternary_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitMain_ternary_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitMain_ternary_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Paren_ternary_ruleContext extends TernaryExpressionContext {
		public TernaryExpressionContext ternaryExpression() {
			return getRuleContext(TernaryExpressionContext.class,0);
		}
		public Paren_ternary_ruleContext(TernaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterParen_ternary_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitParen_ternary_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitParen_ternary_rule(this);
			else return visitor.visitChildren(this);
		}
	}
	public static class Nested_ternary_ruleContext extends TernaryExpressionContext {
		public TernaryExpressionContext ternaryExpression() {
			return getRuleContext(TernaryExpressionContext.class,0);
		}
		public List<TernaryOperandsContext> ternaryOperands() {
			return getRuleContexts(TernaryOperandsContext.class);
		}
		public TernaryOperandsContext ternaryOperands(int i) {
			return getRuleContext(TernaryOperandsContext.class,i);
		}
		public Nested_ternary_ruleContext(TernaryExpressionContext ctx) { copyFrom(ctx); }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterNested_ternary_rule(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitNested_ternary_rule(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitNested_ternary_rule(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TernaryExpressionContext ternaryExpression() throws RecognitionException {
		return ternaryExpression(0);
	}

	private TernaryExpressionContext ternaryExpression(int _p) throws RecognitionException {
		ParserRuleContext _parentctx = _ctx;
		int _parentState = getState();
		TernaryExpressionContext _localctx = new TernaryExpressionContext(_ctx, _parentState);
		TernaryExpressionContext _prevctx = _localctx;
		int _startState = 20;
		enterRecursionRule(_localctx, 20, RULE_ternaryExpression, _p);
		try {
			int _alt;
			enterOuterAlt(_localctx, 1);
			{
			setState(136);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,13,_ctx) ) {
			case 1:
				{
				_localctx = new Paren_ternary_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;

				setState(122);
				match(T__0);
				setState(123);
				ternaryExpression(0);
				setState(124);
				match(T__1);
				}
				break;
			case 2:
				{
				_localctx = new Main_ternary_ruleContext(_localctx);
				_ctx = _localctx;
				_prevctx = _localctx;
				setState(129);
				_errHandler.sync(this);
				switch ( getInterpreter().adaptivePredict(_input,12,_ctx) ) {
				case 1:
					{
					setState(126);
					logicalExpression(0);
					}
					break;
				case 2:
					{
					setState(127);
					relationalExpression();
					}
					break;
				case 3:
					{
					setState(128);
					metric();
					}
					break;
				}
				setState(131);
				match(T__13);
				setState(132);
				ternaryOperands();
				setState(133);
				match(T__14);
				setState(134);
				ternaryOperands();
				}
				break;
			}
			_ctx.stop = _input.LT(-1);
			setState(146);
			_errHandler.sync(this);
			_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			while ( _alt!=2 && _alt!=org.antlr.v4.runtime.atn.ATN.INVALID_ALT_NUMBER ) {
				if ( _alt==1 ) {
					if ( _parseListeners!=null ) triggerExitRuleEvent();
					_prevctx = _localctx;
					{
					{
					_localctx = new Nested_ternary_ruleContext(new TernaryExpressionContext(_parentctx, _parentState));
					pushNewRecursionContext(_localctx, _startState, RULE_ternaryExpression);
					setState(138);
					if (!(precpred(_ctx, 1))) throw new FailedPredicateException(this, "precpred(_ctx, 1)");
					setState(139);
					match(T__13);
					setState(140);
					ternaryOperands();
					setState(141);
					match(T__14);
					setState(142);
					ternaryOperands();
					}
					} 
				}
				setState(148);
				_errHandler.sync(this);
				_alt = getInterpreter().adaptivePredict(_input,14,_ctx);
			}
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			unrollRecursionContexts(_parentctx);
		}
		return _localctx;
	}

	public static class TernaryOperandsContext extends ParserRuleContext {
		public TernaryOperandsContext ternaryOperands() {
			return getRuleContext(TernaryOperandsContext.class,0);
		}
		public ArthmeticExpressionContext arthmeticExpression() {
			return getRuleContext(ArthmeticExpressionContext.class,0);
		}
		public LogicalExpressionContext logicalExpression() {
			return getRuleContext(LogicalExpressionContext.class,0);
		}
		public TernaryExpressionContext ternaryExpression() {
			return getRuleContext(TernaryExpressionContext.class,0);
		}
		public TernaryOperandsContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_ternaryOperands; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterTernaryOperands(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitTernaryOperands(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitTernaryOperands(this);
			else return visitor.visitChildren(this);
		}
	}

	public final TernaryOperandsContext ternaryOperands() throws RecognitionException {
		TernaryOperandsContext _localctx = new TernaryOperandsContext(_ctx, getState());
		enterRule(_localctx, 22, RULE_ternaryOperands);
		try {
			setState(156);
			_errHandler.sync(this);
			switch ( getInterpreter().adaptivePredict(_input,15,_ctx) ) {
			case 1:
				enterOuterAlt(_localctx, 1);
				{
				setState(149);
				match(T__0);
				setState(150);
				ternaryOperands();
				setState(151);
				match(T__1);
				}
				break;
			case 2:
				enterOuterAlt(_localctx, 2);
				{
				setState(153);
				arthmeticExpression(0);
				}
				break;
			case 3:
				enterOuterAlt(_localctx, 3);
				{
				setState(154);
				logicalExpression(0);
				}
				break;
			case 4:
				enterOuterAlt(_localctx, 4);
				{
				setState(155);
				ternaryExpression(0);
				}
				break;
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class AndContext extends ParserRuleContext {
		public TerminalNode A() { return getToken(MetricExpressionParser.A, 0); }
		public TerminalNode N() { return getToken(MetricExpressionParser.N, 0); }
		public TerminalNode D() { return getToken(MetricExpressionParser.D, 0); }
		public AndContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_and; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterAnd(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitAnd(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitAnd(this);
			else return visitor.visitChildren(this);
		}
	}

	public final AndContext and() throws RecognitionException {
		AndContext _localctx = new AndContext(_ctx, getState());
		enterRule(_localctx, 24, RULE_and);
		try {
			setState(162);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case A:
				enterOuterAlt(_localctx, 1);
				{
				setState(158);
				match(A);
				setState(159);
				match(N);
				setState(160);
				match(D);
				}
				break;
			case T__15:
				enterOuterAlt(_localctx, 2);
				{
				setState(161);
				match(T__15);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class OrContext extends ParserRuleContext {
		public TerminalNode O() { return getToken(MetricExpressionParser.O, 0); }
		public TerminalNode R() { return getToken(MetricExpressionParser.R, 0); }
		public OrContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_or; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterOr(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitOr(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitOr(this);
			else return visitor.visitChildren(this);
		}
	}

	public final OrContext or() throws RecognitionException {
		OrContext _localctx = new OrContext(_ctx, getState());
		enterRule(_localctx, 26, RULE_or);
		try {
			setState(167);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case O:
				enterOuterAlt(_localctx, 1);
				{
				setState(164);
				match(O);
				setState(165);
				match(R);
				}
				break;
			case T__16:
				enterOuterAlt(_localctx, 2);
				{
				setState(166);
				match(T__16);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class NotContext extends ParserRuleContext {
		public TerminalNode N() { return getToken(MetricExpressionParser.N, 0); }
		public TerminalNode O() { return getToken(MetricExpressionParser.O, 0); }
		public TerminalNode T() { return getToken(MetricExpressionParser.T, 0); }
		public NotContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_not; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterNot(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitNot(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitNot(this);
			else return visitor.visitChildren(this);
		}
	}

	public final NotContext not() throws RecognitionException {
		NotContext _localctx = new NotContext(_ctx, getState());
		enterRule(_localctx, 28, RULE_not);
		try {
			setState(173);
			_errHandler.sync(this);
			switch (_input.LA(1)) {
			case N:
				enterOuterAlt(_localctx, 1);
				{
				setState(169);
				match(N);
				setState(170);
				match(O);
				setState(171);
				match(T);
				}
				break;
			case T__17:
				enterOuterAlt(_localctx, 2);
				{
				setState(172);
				match(T__17);
				}
				break;
			default:
				throw new NoViableAltException(this);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class ModuloContext extends ParserRuleContext {
		public ModuloContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_modulo; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterModulo(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitModulo(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitModulo(this);
			else return visitor.visitChildren(this);
		}
	}

	public final ModuloContext modulo() throws RecognitionException {
		ModuloContext _localctx = new ModuloContext(_ctx, getState());
		enterRule(_localctx, 30, RULE_modulo);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(175);
			match(T__2);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public static class MetricContext extends ParserRuleContext {
		public TerminalNode REX() { return getToken(MetricExpressionParser.REX, 0); }
		public MetricContext(ParserRuleContext parent, int invokingState) {
			super(parent, invokingState);
		}
		@Override public int getRuleIndex() { return RULE_metric; }
		@Override
		public void enterRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).enterMetric(this);
		}
		@Override
		public void exitRule(ParseTreeListener listener) {
			if ( listener instanceof MetricExpressionListener ) ((MetricExpressionListener)listener).exitMetric(this);
		}
		@Override
		public <T> T accept(ParseTreeVisitor<? extends T> visitor) {
			if ( visitor instanceof MetricExpressionVisitor ) return ((MetricExpressionVisitor<? extends T>)visitor).visitMetric(this);
			else return visitor.visitChildren(this);
		}
	}

	public final MetricContext metric() throws RecognitionException {
		MetricContext _localctx = new MetricContext(_ctx, getState());
		enterRule(_localctx, 32, RULE_metric);
		try {
			enterOuterAlt(_localctx, 1);
			{
			setState(177);
			match(REX);
			}
		}
		catch (RecognitionException re) {
			_localctx.exception = re;
			_errHandler.reportError(this, re);
			_errHandler.recover(this, re);
		}
		finally {
			exitRule();
		}
		return _localctx;
	}

	public boolean sempred(RuleContext _localctx, int ruleIndex, int predIndex) {
		switch (ruleIndex) {
		case 2:
			return logicalExpression_sempred((LogicalExpressionContext)_localctx, predIndex);
		case 4:
			return arthmeticExpression_sempred((ArthmeticExpressionContext)_localctx, predIndex);
		case 10:
			return ternaryExpression_sempred((TernaryExpressionContext)_localctx, predIndex);
		}
		return true;
	}
	private boolean logicalExpression_sempred(LogicalExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 0:
			return precpred(_ctx, 3);
		case 1:
			return precpred(_ctx, 2);
		}
		return true;
	}
	private boolean arthmeticExpression_sempred(ArthmeticExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 2:
			return precpred(_ctx, 5);
		case 3:
			return precpred(_ctx, 4);
		case 4:
			return precpred(_ctx, 3);
		}
		return true;
	}
	private boolean ternaryExpression_sempred(TernaryExpressionContext _localctx, int predIndex) {
		switch (predIndex) {
		case 5:
			return precpred(_ctx, 1);
		}
		return true;
	}

	public static final String _serializedATN =
		"\3\u608b\ua72a\u8133\ub9ed\u417c\u3be7\u7786\u5964\3!\u00b6\4\2\t\2\4"+
		"\3\t\3\4\4\t\4\4\5\t\5\4\6\t\6\4\7\t\7\4\b\t\b\4\t\t\t\4\n\t\n\4\13\t"+
		"\13\4\f\t\f\4\r\t\r\4\16\t\16\4\17\t\17\4\20\t\20\4\21\t\21\4\22\t\22"+
		"\3\2\3\2\3\3\3\3\3\3\3\3\5\3+\n\3\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4"+
		"\5\4\66\n\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\3\4\7\4@\n\4\f\4\16\4C\13\4\3"+
		"\5\3\5\3\5\5\5H\n\5\3\6\3\6\3\6\3\6\3\6\3\6\3\6\3\6\5\6R\n\6\3\6\3\6\3"+
		"\6\3\6\3\6\3\6\3\6\3\6\3\6\7\6]\n\6\f\6\16\6`\13\6\3\7\3\7\5\7d\n\7\3"+
		"\b\3\b\3\b\3\b\3\b\3\b\3\b\3\b\5\bn\n\b\3\t\3\t\3\t\5\ts\n\t\3\n\3\n\3"+
		"\n\5\nx\n\n\3\13\3\13\3\f\3\f\3\f\3\f\3\f\3\f\3\f\3\f\5\f\u0084\n\f\3"+
		"\f\3\f\3\f\3\f\3\f\5\f\u008b\n\f\3\f\3\f\3\f\3\f\3\f\3\f\7\f\u0093\n\f"+
		"\f\f\16\f\u0096\13\f\3\r\3\r\3\r\3\r\3\r\3\r\3\r\5\r\u009f\n\r\3\16\3"+
		"\16\3\16\3\16\5\16\u00a5\n\16\3\17\3\17\3\17\5\17\u00aa\n\17\3\20\3\20"+
		"\3\20\3\20\5\20\u00b0\n\20\3\21\3\21\3\22\3\22\3\22\2\5\6\n\26\23\2\4"+
		"\6\b\n\f\16\20\22\24\26\30\32\34\36 \"\2\5\3\2\6\7\3\2\b\t\3\2\n\17\2"+
		"\u00c2\2$\3\2\2\2\4*\3\2\2\2\6\65\3\2\2\2\bG\3\2\2\2\nQ\3\2\2\2\fc\3\2"+
		"\2\2\16m\3\2\2\2\20r\3\2\2\2\22w\3\2\2\2\24y\3\2\2\2\26\u008a\3\2\2\2"+
		"\30\u009e\3\2\2\2\32\u00a4\3\2\2\2\34\u00a9\3\2\2\2\36\u00af\3\2\2\2 "+
		"\u00b1\3\2\2\2\"\u00b3\3\2\2\2$%\5\4\3\2%\3\3\2\2\2&+\5\n\6\2\'+\5\6\4"+
		"\2(+\5\16\b\2)+\5\26\f\2*&\3\2\2\2*\'\3\2\2\2*(\3\2\2\2*)\3\2\2\2+\5\3"+
		"\2\2\2,-\b\4\1\2-.\7\3\2\2./\5\6\4\2/\60\7\4\2\2\60\66\3\2\2\2\61\62\5"+
		"\36\20\2\62\63\5\6\4\6\63\66\3\2\2\2\64\66\5\b\5\2\65,\3\2\2\2\65\61\3"+
		"\2\2\2\65\64\3\2\2\2\66A\3\2\2\2\678\f\5\2\289\5\32\16\29:\5\6\4\6:@\3"+
		"\2\2\2;<\f\4\2\2<=\5\34\17\2=>\5\6\4\5>@\3\2\2\2?\67\3\2\2\2?;\3\2\2\2"+
		"@C\3\2\2\2A?\3\2\2\2AB\3\2\2\2B\7\3\2\2\2CA\3\2\2\2DH\5\16\b\2EH\7\25"+
		"\2\2FH\7\26\2\2GD\3\2\2\2GE\3\2\2\2GF\3\2\2\2H\t\3\2\2\2IJ\b\6\1\2JK\7"+
		"\3\2\2KL\5\n\6\2LM\7\4\2\2MR\3\2\2\2NO\7\b\2\2OR\5\"\22\2PR\5\f\7\2QI"+
		"\3\2\2\2QN\3\2\2\2QP\3\2\2\2R^\3\2\2\2ST\f\7\2\2TU\7\5\2\2U]\5\n\6\bV"+
		"W\f\6\2\2WX\t\2\2\2X]\5\n\6\7YZ\f\5\2\2Z[\t\3\2\2[]\5\n\6\6\\S\3\2\2\2"+
		"\\V\3\2\2\2\\Y\3\2\2\2]`\3\2\2\2^\\\3\2\2\2^_\3\2\2\2_\13\3\2\2\2`^\3"+
		"\2\2\2ad\5\"\22\2bd\7\31\2\2ca\3\2\2\2cb\3\2\2\2d\r\3\2\2\2ef\7\3\2\2"+
		"fg\5\16\b\2gh\7\4\2\2hn\3\2\2\2ij\5\20\t\2jk\5\24\13\2kl\5\20\t\2ln\3"+
		"\2\2\2me\3\2\2\2mi\3\2\2\2n\17\3\2\2\2os\5\n\6\2ps\7\25\2\2qs\7\26\2\2"+
		"ro\3\2\2\2rp\3\2\2\2rq\3\2\2\2s\21\3\2\2\2tx\5\32\16\2ux\5\34\17\2vx\5"+
		"\36\20\2wt\3\2\2\2wu\3\2\2\2wv\3\2\2\2x\23\3\2\2\2yz\t\4\2\2z\25\3\2\2"+
		"\2{|\b\f\1\2|}\7\3\2\2}~\5\26\f\2~\177\7\4\2\2\177\u008b\3\2\2\2\u0080"+
		"\u0084\5\6\4\2\u0081\u0084\5\16\b\2\u0082\u0084\5\"\22\2\u0083\u0080\3"+
		"\2\2\2\u0083\u0081\3\2\2\2\u0083\u0082\3\2\2\2\u0084\u0085\3\2\2\2\u0085"+
		"\u0086\7\20\2\2\u0086\u0087\5\30\r\2\u0087\u0088\7\21\2\2\u0088\u0089"+
		"\5\30\r\2\u0089\u008b\3\2\2\2\u008a{\3\2\2\2\u008a\u0083\3\2\2\2\u008b"+
		"\u0094\3\2\2\2\u008c\u008d\f\3\2\2\u008d\u008e\7\20\2\2\u008e\u008f\5"+
		"\30\r\2\u008f\u0090\7\21\2\2\u0090\u0091\5\30\r\2\u0091\u0093\3\2\2\2"+
		"\u0092\u008c\3\2\2\2\u0093\u0096\3\2\2\2\u0094\u0092\3\2\2\2\u0094\u0095"+
		"\3\2\2\2\u0095\27\3\2\2\2\u0096\u0094\3\2\2\2\u0097\u0098\7\3\2\2\u0098"+
		"\u0099\5\30\r\2\u0099\u009a\7\4\2\2\u009a\u009f\3\2\2\2\u009b\u009f\5"+
		"\n\6\2\u009c\u009f\5\6\4\2\u009d\u009f\5\26\f\2\u009e\u0097\3\2\2\2\u009e"+
		"\u009b\3\2\2\2\u009e\u009c\3\2\2\2\u009e\u009d\3\2\2\2\u009f\31\3\2\2"+
		"\2\u00a0\u00a1\7\34\2\2\u00a1\u00a2\7\35\2\2\u00a2\u00a5\7\36\2\2\u00a3"+
		"\u00a5\7\22\2\2\u00a4\u00a0\3\2\2\2\u00a4\u00a3\3\2\2\2\u00a5\33\3\2\2"+
		"\2\u00a6\u00a7\7\37\2\2\u00a7\u00aa\7 \2\2\u00a8\u00aa\7\23\2\2\u00a9"+
		"\u00a6\3\2\2\2\u00a9\u00a8\3\2\2\2\u00aa\35\3\2\2\2\u00ab\u00ac\7\35\2"+
		"\2\u00ac\u00ad\7\37\2\2\u00ad\u00b0\7!\2\2\u00ae\u00b0\7\24\2\2\u00af"+
		"\u00ab\3\2\2\2\u00af\u00ae\3\2\2\2\u00b0\37\3\2\2\2\u00b1\u00b2\7\5\2"+
		"\2\u00b2!\3\2\2\2\u00b3\u00b4\7\27\2\2\u00b4#\3\2\2\2\25*\65?AGQ\\^cm"+
		"rw\u0083\u008a\u0094\u009e\u00a4\u00a9\u00af";
	public static final ATN _ATN =
		new ATNDeserializer().deserialize(_serializedATN.toCharArray());
	static {
		_decisionToDFA = new DFA[_ATN.getNumberOfDecisions()];
		for (int i = 0; i < _ATN.getNumberOfDecisions(); i++) {
			_decisionToDFA[i] = new DFA(_ATN.getDecisionState(i), i);
		}
	}
}