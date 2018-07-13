// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.expressions;

import java.util.List;
import java.util.Objects;
import java.util.Set;

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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.opentsdb.expressions.parser.MetricExpressionLexer;
import net.opentsdb.expressions.parser.MetricExpressionParser;
import net.opentsdb.expressions.parser.MetricExpressionParser.Addsub_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.AndContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Arith_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ArithmeticContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Arithmetic_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Divmul_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicalContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicalOperandsContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_and_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_not_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_expr_or_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Logical_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.LogicopContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Main_relational_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.MetricContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Minus_metric_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Mod_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ModuloContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.NotContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.OrContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_arith_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_logical_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Paren_relational_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.ProgContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.RelationalContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.Relational_operands_ruleContext;
import net.opentsdb.expressions.parser.MetricExpressionParser.RelationalopContext;
import net.opentsdb.expressions.parser.MetricExpressionVisitor;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;

/**
 * An Antlr4 visitor to build the expression sub-graph from the parsed
 * expression.
 * 
 * @since 3.0
 */
public class ExpressionParser extends DefaultErrorStrategy 
    implements MetricExpressionVisitor<Object> {

  /** The expression config. */
  private final ExpressionConfig config;
  
  /** The expression used for variable extraction. */
  private final String expression;
  
  /** A counter used to increment on nodes in the graph. */
  private int cntr = 0;
  
  /** The list of nodes generated after parsing. */
  private final List<ExpressionParseNode> nodes;
  
  /** The set of variables extracted when parsing the expression. */
  private final Set<String> variables;
  
  /**
   * Default ctor.
   * @param config The non-null expression config to parse.
   */
  public ExpressionParser(final ExpressionConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Expression config cannot "
          + "be null.");
    }
    this.config = config;
    nodes = Lists.newArrayList();
    expression = null;
    variables = null;
  }
  
  /**
   * Protected ctor used by {@link #parseVariables(String)}.
   * @param expression The non-null and non-empty expression.
   */
  protected ExpressionParser(final String expression) {
    config = null;
    nodes = null;
    this.expression = expression;
    variables = Sets.newHashSet();
  }
  
  /**
   * Parses the expression if successful.
   * @return A non-null and non-empty list of expression nodes.
   * @throws ParseCancellationException if parsing as unsuccessful.
   */
  public List<ExpressionParseNode> parse() {
    final MetricExpressionLexer lexer = new MetricExpressionLexer(
        new ANTLRInputStream(config.getExpression()));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final MetricExpressionParser parser = new MetricExpressionParser(tokens);
    parser.removeErrorListeners(); // suppress logging to stderr.
    parser.setErrorHandler(this);
    parser.prog().accept(this);
    
    if (nodes.size() < 1) {
      throw new ParseCancellationException("Unable to extract an "
          + "expression from '" + config.getExpression() + "'");
    }
    
    // reset the ID on the last node as it's the root
    nodes.get(nodes.size() - 1).overrideId(config.getId());
    nodes.get(nodes.size() - 1).overrideAs(config.getAs());
    return nodes;
  }
  
  /**
   * Parses the expression and returns the literal variable names.
   * @param expression The non-null and non-empty expression to parse.
   * @return The non-null set of 1 or more variables.
   * @throws IllegalArgumentException if the expression was null.
   */
  public static Set<String> parseVariables(final String expression) {
    if (Strings.isNullOrEmpty(expression)) {
      throw new IllegalArgumentException("Expression cannot be null "
          + "or empty.");
    }
    final MetricExpressionLexer lexer = new MetricExpressionLexer(
        new ANTLRInputStream(expression));
    final CommonTokenStream tokens = new CommonTokenStream(lexer);
    final MetricExpressionParser parser = new MetricExpressionParser(tokens);
    
    final ExpressionParser listener = new ExpressionParser(expression);
    parser.removeErrorListeners(); // suppress logging to stderr.
    parser.setErrorHandler(listener);
    parser.prog().accept(listener);
    
    if (listener.variables.size() < 1) {
      throw new ParseCancellationException("Unable to extract an "
          + "expression from '" + expression + "'");
    }
    
    return listener.variables;
  }
  
  /** Helper class. */
  protected class NumericLiteral {
    final boolean is_integer;
    final long number;
    
    NumericLiteral(final long number) {
      is_integer = true;
      this.number = number;
    }
    
    NumericLiteral(final double number) {
      is_integer = false;
      this.number = Double.doubleToLongBits(number);
    }
    
    public String toString() {
      return is_integer ? Long.toString(number) : 
        Double.toString(Double.longBitsToDouble(number));
    }
    
    public boolean isInteger() {
      return is_integer;
    }
    
    public long longValue() {
      return number;
    }
    
    public double doubleValue() {
      return Double.longBitsToDouble(number);
    }
  
    @Override
    public boolean equals(final Object o) {
      if (o == null) {
        return false;
      }
      if (o == this) {
        return true;
      }
      if (!(o instanceof NumericLiteral)) {
        return false;
      }
      return Objects.equals(is_integer, ((NumericLiteral) o).is_integer) &&
             Objects.equals(number, ((NumericLiteral) o).number);
    }
  }
  
  /** Helper class. */
  private class Null {
    public String toString() {
      return null;
    }
  }
  
  /**
   * Helper to construct a binary expression node.
   * @param op A non-null operator.
   * @param left The left operand.
   * @param right The right operand.
   * @return An ExpressionParseNode if applicable, possibly a NumericLiteral
   * or Null if both sides were of the same type.
   */
  @VisibleForTesting
  Object newBinary(final ExpressionOp op, 
                   final Object left, 
                   final Object right) {
    // if we're only parsing the variables, don't worry about instantiating
    // a node.
    if (config == null) {
      if (left instanceof String) {
        variables.add((String) left);
      }
      if (right instanceof String) {
        variables.add((String) right);
      }
      return null;
    }
    
    // shrug.
    if (left instanceof Null && right instanceof Null) {
      return left;
    }
    
    // if both sides are numerics then we just add em
    if (left instanceof NumericLiteral && right instanceof NumericLiteral) {
      if (((NumericLiteral) left).is_integer && ((NumericLiteral) right).is_integer) {
        switch (op) {
        case ADD:
          // TODO - overflow check
          return new NumericLiteral(
              ((NumericLiteral) left).number + ((NumericLiteral) right).number);
        case SUBTRACT:
          // TODO - underflow check
          return new NumericLiteral(
              ((NumericLiteral) left).number - ((NumericLiteral) right).number);
        case MULTIPLY:
          // TODO - overflow check
          return new NumericLiteral(
              ((NumericLiteral) left).number * ((NumericLiteral) right).number);
        case DIVIDE:
          if (((NumericLiteral) left).number % ((NumericLiteral) right).number == 0) {
            return new NumericLiteral(
                ((NumericLiteral) left).number / ((NumericLiteral) right).number);
          } else {
            return new NumericLiteral(
                (double) ((NumericLiteral) left).number / 
                (double) ((NumericLiteral) right).number);
          }
        case MOD:
          return new NumericLiteral(
              ((NumericLiteral) left).number % ((NumericLiteral) right).number);
        default:
          throw new ParseCancellationException("Logical and relational "
              + "operators cannot be applied to numerics. '" 
              + left + " op " + right + "'");
        }
      } else {
        final double dl = ((NumericLiteral) left).is_integer ? 
            (double) ((NumericLiteral) left).number : 
              Double.longBitsToDouble(((NumericLiteral) left).number);
        final double dr = ((NumericLiteral) right).is_integer ? 
            (double) ((NumericLiteral) right).number : 
              Double.longBitsToDouble(((NumericLiteral) right).number);
        switch (op) {
        case ADD:
          // TODO - overflow check
          return new NumericLiteral(dl + dr);
        case SUBTRACT:
          // TODO - underflow check
          return new NumericLiteral(dl - dr);
        case MULTIPLY:
          // TODO - overflow check
          return new NumericLiteral(dl * dr);
        case DIVIDE:
          return new NumericLiteral(dl / dr);
        case MOD:
          return new NumericLiteral(
              ((NumericLiteral) left).number % ((NumericLiteral) right).number);
        default:
          throw new ParseCancellationException("Logical operators cannot "
              + "be applied to numerics. '" + left + " op " + right + "'");
        }
      }
    }
    
    // here we can cleanup, e.g. merge numerics
    final ExpressionParseNode.Builder builder = ExpressionParseNode
        .newBuilder()
        .setExpressionOp(op);
    setBranch(builder, left, true);
    setBranch(builder, right, false);
    builder.setId(config.getId() + "_SubExp#" + cntr++);
    final ExpressionParseNode config = (ExpressionParseNode) builder.build();
    nodes.add(config);
    return config;
  }
  
  /**
   * Helper to build the binary expression.
   * @param builder A non-null builder.
   * @param obj A non-null object. (can be NULL though).
   * @param is_left Whether or not to populate the left or right branch.
   */
  private void setBranch(final ExpressionParseNode.Builder builder, 
                         final Object obj, 
                         final boolean is_left) {
    if (obj instanceof Null) {
      if (is_left) {
        builder.setLeftType(OperandType.NULL);
      } else {
        builder.setRightType(OperandType.NULL);
      }
    } else if (obj instanceof NumericLiteral) {
      if (is_left) {
        builder.setLeft(obj)
               .setLeftType(OperandType.LITERAL_NUMERIC);
      } else {
        builder.setRight(obj)
               .setRightType(OperandType.LITERAL_NUMERIC);
      }
    } else if (obj instanceof ExpressionParseNode) {
      if (is_left) {
        builder.setLeft(((ExpressionParseNode) obj).getId())
               .setLeftType(OperandType.SUB_EXP);
      } else {
        builder.setRight(((ExpressionParseNode) obj).getId())
               .setRightType(OperandType.SUB_EXP);
      }
    } else if (obj instanceof String) {
      // handle the funky "escape keywords" case. e.g. "sys.'if'.out"
      if (is_left) {
        builder.setLeft((String) obj)
               .setLeftType(OperandType.VARIABLE);
      } else {
        builder.setRight((String) obj)
               .setRightType(OperandType.VARIABLE);
      }
    } else {
      throw new RuntimeException("NEED TO HANDLE: " + obj.getClass());
    }
  }

  @Override
  public Object visit(ParseTree tree) {
    throw new UnsupportedOperationException("Can't visit " 
        + tree.getClass());
  }

  @Override
  public Object visitChildren(RuleNode node) {
    throw new UnsupportedOperationException("Can't visit " 
        + node.getClass());
  }

  @Override
  public Object visitTerminal(TerminalNode node) {
    // this is a variable or operand (or parens).
    final String text = node.getText();
    // TODO - break this into a numeric literal to avoid having to parse
    // here.
    if ((text.charAt(0) >= '0' &&
        text.charAt(0) <= '9') ||
        text.charAt(0) == '-') {
      // starts with a digit
      if (text.contains(".")) {
        try {
          final double v = Double.parseDouble(text);
          return new NumericLiteral(v);
        } catch (NumberFormatException e) { 
          return text;
        }
      } else {
        try {
          final long v = Long.parseLong(text);
          return new NumericLiteral(v);
        } catch (NumberFormatException e) { 
          return text;
        }
      }
    } else {
      return text;
    }
  }

  @Override
  public Object visitErrorNode(ErrorNode node) {
    throw new RuntimeException("Error parsing: " + node.getText());
  }

  @Override
  public Object visitProg(ProgContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitArithmetic(ArithmeticContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogical(LogicalContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitRelational(RelationalContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_logical_rule(Paren_logical_ruleContext ctx) {
    // catch exceptions
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitLogical_operands_rule(Logical_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogical_expr_and_rule(Logical_expr_and_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitLogical_expr_or_rule(Logical_expr_or_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitLogical_expr_not_rule(Logical_expr_not_ruleContext ctx) {
    final Object child = ctx.getChild(1).accept(this);
    if (child instanceof ExpressionParseNode) {
      ((ExpressionParseNode) child).setNot(true);
    } else {
      throw new RuntimeException("Response from the child of the not "
          + "was a " + child.getClass());
    }
    return child;
  }

  @Override
  public Object visitLogicalOperands(LogicalOperandsContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitArith_operands_rule(Arith_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_arith_rule(Paren_arith_ruleContext ctx) {
    // catch errors
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitMinus_metric_rule(Minus_metric_ruleContext ctx) {
    final Object child = ctx.getChild(1).accept(this);
    if (child instanceof ExpressionParseNode) {
      ((ExpressionParseNode) child).setNegate(true);
    } else {
      throw new ParseCancellationException("Response from the child of "
          + "the minus cannot be a metric.");
    }
    return child;
  }

  @Override
  public Object visitMod_arith_rule(Mod_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitAddsub_arith_rule(Addsub_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitDivmul_arith_rule(Divmul_arith_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitArithmetic_operands_rule(
      Arithmetic_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitParen_relational_rule(Paren_relational_ruleContext ctx) {
    // catch exceptions
    ctx.getChild(0).accept(this);
    ctx.getChild(2).accept(this);
    return ctx.getChild(1).accept(this);
  }

  @Override
  public Object visitMain_relational_rule(Main_relational_ruleContext ctx) {
    Object left = ctx.getChild(0).accept(this);
    for (int i = 2; i < ctx.getChildCount(); i += 2) {
      Object right = ctx.getChild(i).accept(this);
      Object op = ctx.getChild(i - 1).accept(this);
      left = newBinary(ExpressionOp.parse((String) op), left, right);
    }
    return left;
  }

  @Override
  public Object visitRelational_operands_rule(
      Relational_operands_ruleContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitLogicop(LogicopContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitRelationalop(RelationalopContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitAnd(AndContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitOr(OrContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitNot(NotContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitModulo(ModuloContext ctx) {
    return ctx.getChild(0).accept(this);
  }

  @Override
  public Object visitMetric(MetricContext ctx) {
    return ctx.getChild(0).accept(this);
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
        buf.append(config == null ? expression : config.getExpression());
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
  
}
