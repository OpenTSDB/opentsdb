// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.data.prometheus;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.FailedPredicateException;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.ErrorNode;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;
import org.antlr.v4.runtime.tree.TerminalNode;

import net.opentsdb.prometheus.grammar.PromQLLexer;
import net.opentsdb.prometheus.grammar.PromQLParserVisitor;
import net.opentsdb.prometheus.grammar.PromQLParser.AddOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.AggregationContext;
import net.opentsdb.prometheus.grammar.PromQLParser.AndUnlessOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ByContext;
import net.opentsdb.prometheus.grammar.PromQLParser.CompareOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ExpressionContext;
import net.opentsdb.prometheus.grammar.PromQLParser.FunctionContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupLeftContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupRightContext;
import net.opentsdb.prometheus.grammar.PromQLParser.GroupingContext;
import net.opentsdb.prometheus.grammar.PromQLParser.IgnoringContext;
import net.opentsdb.prometheus.grammar.PromQLParser.InstantSelectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.KeywordContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelMatcherOperatorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelNameContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LabelNameListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.LiteralContext;
import net.opentsdb.prometheus.grammar.PromQLParser.MatrixSelectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.MultOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OffsetContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OnContext;
import net.opentsdb.prometheus.grammar.PromQLParser.OrOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParameterContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParameterListContext;
import net.opentsdb.prometheus.grammar.PromQLParser.ParensContext;
import net.opentsdb.prometheus.grammar.PromQLParser.PowOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.UnaryOpContext;
import net.opentsdb.prometheus.grammar.PromQLParser.VectorContext;
import net.opentsdb.prometheus.grammar.PromQLParser.VectorOperationContext;
import net.opentsdb.prometheus.grammar.PromQLParser.WithoutContext;
import net.opentsdb.query.SemanticQuery;

/**
 * Just a debugger.
 * 
 * @since 3.0
 */
public class PromQLParserDebug extends DefaultErrorStrategy  implements PromQLParserVisitor {


  static public void main(final String[] args) {
    
  //String incoming = "http_request_duration_seconds_sum";
  //String incoming = "http.request\\.duration.second.sum";
//  String incoming = "http_requests_total{group!~\"canary\"}";
//  String incoming = "http_requests_total{job=\"prometheus\",group!~\"canary\"}";
//  String incoming = "rate(http_request_duration_seconds_sum)";
//  String incoming = "http_request_duration_seconds_sum[5m]";
//  String incoming = "http_requests_total{job=\"prometheus\",group!~\"canary\"}[5m]";
//  String incoming = "rate(http_requests_total[5m])[30m:1m]";
//  String incoming = "sum(http_request_duration_Seconds_sum)";
//    String incoming = "topk(10, http_request_duration_Seconds_sum)";
//    String incoming = "avg_over_time(mymetric)";
    String incoming = "metric1 or metric2";
//  String incoming = "(\n" + 
//      "  sum(rate(http_request_duration_seconds_bucket{le=\"0.3\"}[5m])) by (job)\n" + 
//      "+\n" + 
//      "  sum(rate(http_request_duration_seconds_bucket{le=\"1.2\"}[5m])) by (job)\n" + 
//      ") / 2 / sum(rate(http_request_duration_seconds_count[5m])) by (job)";
//  String incoming = "http_requests_total{status!~\"4..\"}";
  // WARNING* FRom examples page bug doesn't parse!
//  String incoming = "max_over_time(deriv(rate(distance_covered_total[5s])[30s:5s])[10m:])";
//  String incoming = "sum by (job) (\n" + 
//      "  rate(http_requests_total[5m])\n" + 
//      ")";
//String incoming = "sum by (job) (http_requests_total[5m:5m])";
//  String incoming = "sum by (job) (http_requests_total[5m:5m] offset 5m)";
  
  
    new PromQLParserDebug().run(incoming);
  }
 
  int level = 0;
  

  public SemanticQuery run(final String incoming) {
    PromQLLexer lexer = new PromQLLexer(new ANTLRInputStream(incoming));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    net.opentsdb.prometheus.grammar.PromQLParser parser = 
        new net.opentsdb.prometheus.grammar.PromQLParser(tokens);
    parser.removeErrorListeners(); // suppress logging to stderr.
    parser.setErrorHandler(this);
    
    Object obj = parser.expression().accept(this);
    return null;
  }

  @Override
  public Object visit(ParseTree tree) {
    return tree.accept(this);
  }

  @Override
  public Object visitChildren(RuleNode node) {
    level += 2;
    System.out.println(pad("RULE NODE: " + node));
    level -= 2;
    return null;
  }

  @Override
  public Object visitTerminal(TerminalNode node) {
    level += 2;
    System.out.println(pad("TERMINAL: " + node));
    level -= 2;
    return node.getText();
  }

  @Override
  public Object visitErrorNode(ErrorNode node) {
    level += 2;
    System.out.println(pad("ERROR NODE: " + node));
    level -= 2;
    return null;
  }

  @Override
  public Object visitExpression(ExpressionContext ctx) {
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println("visitExpression[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i));
      
      Object child = ctx.getChild(i).accept(this);
      if (!child.equals("<EOF>")) {
        last = child;
      }
    }
    return last;
  }

  @Override
  public Object visitVectorOperation(VectorOperationContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitVectorOperation[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitUnaryOp(UnaryOpContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitUnaryOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitPowOp(PowOpContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitPowOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitMultOp(MultOpContext ctx) {
    // can be * or / depending on text I believe.
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitMultOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitAddOp(AddOpContext ctx) {
    // Add two values.
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitAddOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitCompareOp(CompareOpContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitCompareOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitAndUnlessOp(AndUnlessOpContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitAndUnlessOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitOrOp(OrOpContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitOrOp[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitVector(VectorContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitVector[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitParens(ParensContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitParens[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitInstantSelector(InstantSelectorContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitInstantSelector[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLabelMatcher(LabelMatcherContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLabelMatcher[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLabelMatcherOperator(LabelMatcherOperatorContext ctx) {
    // an op like `=` or `~` or `!~`
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLabelMatcherOperator[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLabelMatcherList(LabelMatcherListContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLabelMatcherList[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitMatrixSelector(MatrixSelectorContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitMatrixSelector[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitOffset(OffsetContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitOffset[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitFunction(FunctionContext ctx) {
    // function names, e.g. `rate`
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("FunctionContext[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitParameter(ParameterContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("ParameterContext[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitParameterList(ParameterListContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("ParameterListContext[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitAggregation(AggregationContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitAggregation[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitBy(ByContext ctx) {
    // yup, the group by context:
    // sum(rate(http_request_duration_seconds_bucket{le="0.3"}[5m])) by (job)
    // 0 == 'by' keyword
    // 1 == LabelNameListContext
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitBy[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitWithout(WithoutContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitWithout[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitGrouping(GroupingContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitGrouping[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitOn(OnContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitOn[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitIgnoring(IgnoringContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitIgnoring[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitGroupLeft(GroupLeftContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitGroupLeft[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitGroupRight(GroupRightContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitGroupRight[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLabelName(LabelNameContext ctx) {
    // came here after the group by for the tag key. Could be tag key?
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLabelName[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLabelNameList(LabelNameListContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLabelNameList[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitKeyword(KeywordContext ctx) {
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitKeyword[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public Object visitLiteral(LiteralContext ctx) {
    // here's where we get something like `2` as a literal.
    level += 2;
    Object last = null;
    for (int i = 0; i < ctx.getChildCount(); i++) {
      System.out.println(pad("visitLiteral[" + i + "] class: " + ctx.getChild(i).getClass() + " => " + ctx.getChild(i)));
      last = ctx.getChild(i).accept(this);
    }
    level -= 2;
    return last;
  }

  @Override
  public void recover(final Parser recognizer, final RecognitionException e) {
    final StringBuilder buf = new StringBuilder()
        .append("Error at line (")
        .append(e.getOffendingToken().getLine())
        .append(":")
        .append(e.getOffendingToken().getCharPositionInLine())
        .append(") ");
    if (e instanceof NoViableAltException) {
      final TokenStream tokens = recognizer.getInputStream();
      final NoViableAltException ex = (NoViableAltException) e;
      buf.append("Expression may not be complete. '");
      if (tokens != null) {
        if (ex.getStartToken().getType() == Token.EOF) {
          buf.append("<EOF>");
        } else {
          buf.append(tokens.getText(ex.getStartToken(), ex.getOffendingToken()));
        }
      } else {
        buf.append("TODO");
      }
      buf.append("'");
    } else if (e instanceof InputMismatchException) {
      final InputMismatchException ex = (InputMismatchException) e;
      buf.append("Missmatched input. Got '")
         .append(getTokenErrorDisplay(ex.getOffendingToken()))
         .append("' but expected '")
         .append(ex.getExpectedTokens().toString(recognizer.getVocabulary()))
         .append("'");
    } else if (e instanceof FailedPredicateException) {
      final FailedPredicateException ex = (FailedPredicateException) e;
      final String rule_name = recognizer
          .getRuleNames()[recognizer.getContext().getRuleIndex()];
      buf.append(" Predicate error with rule '")
         .append(rule_name)
         .append("' ")
         .append(ex.getMessage());
    } else {
      buf.append("Unexpected error at token '")
         .append(getTokenErrorDisplay(e.getOffendingToken()))
         .append("'");
    }

    for (ParserRuleContext context = recognizer.getContext();
        context != null; context = context.getParent()) {
      context.exception = e;
    }
    throw new ParseCancellationException(buf.toString(), e);
  }

  /** Make sure we don't attempt to recover inline; if the parser
   *  successfully recovers, it won't throw an exception.
   */
  @Override
  public Token recoverInline(final Parser recognizer)
      throws RecognitionException {
    final InputMismatchException e = new InputMismatchException(recognizer);
    recover(recognizer, e);
    return null;
  }

  /** Make sure we don't attempt to recover from problems in subrules. */
  @Override
  public void sync(final Parser recognizer) { }

  String pad(String string) {
    StringBuilder buf = new StringBuilder();
    for (int i = 0; i < level; i++) {
      buf.append(" ");
    }
    return buf.append(string).toString();
  }

}
