package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import net.opentsdb.core.DataPoints;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CommonTokenStream;
import org.antlr.runtime.RecognitionException;
import org.antlr.runtime.tree.CommonTreeNodeStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ArithmeticExpressionCalculator {
  private static final Logger LOG = LoggerFactory
      .getLogger(ArithmeticExpressionCalculator.class);

  private final String arithmeticExpression;
  private final ArithmeticNode rootNode;

  public ArithmeticExpressionCalculator(String arithmeticExpression) {
    this.arithmeticExpression = arithmeticExpression;
    rootNode = parseArithmeticExpression(arithmeticExpression);
  }

  public DataPoints calculateArithmeticExpression(
      final Map<String, DataPoints[]> queryResults) {
    DataPoints result = null;

    addDataPointsToMetricNodes(rootNode, queryResults);

    result = calculate(rootNode);

    return result;
  }

  private ArithmeticNode parseArithmeticExpression(
      final String arithmeticExpression) {
    ArithmeticNode result = null;

    if (arithmeticExpression != null && !arithmeticExpression.isEmpty()) {
      try {
        ANTLRStringStream input = new ANTLRStringStream(arithmeticExpression);
        MetricArithmeticExpressionLexer lexer = new MetricArithmeticExpressionLexer(
            input);
        CommonTokenStream tokens = new CommonTokenStream(lexer);
        MetricArithmeticExpressionParser parser = new MetricArithmeticExpressionParser(
            tokens);
        MetricArithmeticExpressionParser.parse_return parseReturn = parser
            .parse();
        CommonTreeNodeStream nodes = new CommonTreeNodeStream(parseReturn.tree);
        nodes.setTokenStream(tokens);

        MetricArithmeticExpressionTreeWalker walker = new MetricArithmeticExpressionTreeWalker(
            nodes);

        result = walker.parse();
      } catch (RecognitionException e) {
        throw new RuntimeException("error parsing arithmetic expression: "
            + arithmeticExpression, e);
      }
    }

    return result;
  }

  private void addDataPointsToMetricNodes(final ArithmeticNode arithmeticNode,
      final Map<String, DataPoints[]> queryResults) {
    if (arithmeticNode instanceof OperatorNode) {
      OperatorNode operatorNode = (OperatorNode) arithmeticNode;

      addDataPointsToMetricNodes(operatorNode.getOperandOne(), queryResults);
      addDataPointsToMetricNodes(operatorNode.getOperandTwo(), queryResults);
    } else {
      MetricNode metricNode = (MetricNode) arithmeticNode;
      DataPoints[] dataPoints = queryResults.get(metricNode.getName());

      metricNode.setDataPoints(dataPoints);
    }
  }

  private DataPoints calculate(ArithmeticNode rootNode) {
    List<TimestampValue> timestampValues = null;

    if (rootNode instanceof OperatorNode) {
      timestampValues = calculateOperation((OperatorNode) rootNode);
    } else {
      timestampValues = ((MetricNode) rootNode).getDataPointsValues();
    }

    return repackageTimestampValues(timestampValues);
  }

  private List<TimestampValue> calculateOperation(OperatorNode operator) {
    ArithmeticNode operandOne = operator.getOperandOne();
    ArithmeticNode operandTwo = operator.getOperandTwo();
    List<TimestampValue> operandOneTimestampValues;
    List<TimestampValue> operandTwoTimestampValues;

    if (operandOne instanceof OperatorNode) {
      operandOneTimestampValues = calculateOperation((OperatorNode) operandOne);
    } else {
      operandOneTimestampValues = ((MetricNode) operandOne)
          .getDataPointsValues();
    }

    if (operandTwo instanceof OperatorNode) {
      operandTwoTimestampValues = calculateOperation((OperatorNode) operandTwo);
    } else {
      operandTwoTimestampValues = ((MetricNode) operandTwo)
          .getDataPointsValues();
    }

    return calculateOperandValues(operandOneTimestampValues,
        operandTwoTimestampValues, operator.getOperator());
  }

  private List<TimestampValue> calculateOperandValues(
      List<TimestampValue> operandOneTimestampValues,
      List<TimestampValue> operandTwoTimestampValues, Operator operator) {
    final List<TimestampValue> result = new ArrayList<TimestampValue>();

    if (operandOneTimestampValues != null
        && operandOneTimestampValues.size() > 0
        && operandTwoTimestampValues != null
        && operandTwoTimestampValues.size() > 0) {
      final ListIterator<TimestampValue> iteratorOne = operandOneTimestampValues
          .listIterator();
      final ListIterator<TimestampValue> iteratorTwo = operandTwoTimestampValues
          .listIterator();
      TimestampValue timestampValueOne = iteratorOne.next();
      TimestampValue lastTimestampValueOne = null;
      TimestampValue timestampValueTwo = iteratorTwo.next();
      TimestampValue lastTimestampValueTwo = null;

      while (!timestampValueOne.equals(lastTimestampValueOne)
          && !timestampValueTwo.equals(lastTimestampValueTwo)) {
        if (timestampValueOne.getTimestamp() == timestampValueTwo
            .getTimestamp()) {
          // move on in both iterators
          lastTimestampValueOne = timestampValueOne;
          lastTimestampValueTwo = timestampValueTwo;

          timestampValueOne = iterate(iteratorOne, timestampValueOne);
          timestampValueTwo = iterate(iteratorTwo, timestampValueTwo);
        } else if (timestampValueOne.getTimestamp() < timestampValueTwo
            .getTimestamp()) {
          // move on only in iterator one
          lastTimestampValueOne = timestampValueOne;

          timestampValueOne = iterate(iteratorOne, timestampValueOne);
        } else {
          // move on only in iterator two
          lastTimestampValueTwo = timestampValueTwo;

          timestampValueTwo = iterate(iteratorTwo, timestampValueTwo);
        }

        if (lastTimestampValueOne != null && lastTimestampValueTwo != null) {
          result.add(calculateValues(lastTimestampValueOne,
              lastTimestampValueTwo, operator));
        }
      }
    }

    return result;
  }

  private DataPoints repackageTimestampValues(
      List<TimestampValue> timestampValues) {
    final ArithmeticExpressionResultDataPoints result = new ArithmeticExpressionResultDataPoints(
        arithmeticExpression);

    for (TimestampValue timestampValue : timestampValues) {
      result.add(new ArithmeticExpressionResultDataPoint(timestampValue
          .getTimestamp(), timestampValue.getValue()));
    }

    return result;
  }

  private TimestampValue calculateValues(TimestampValue timestampValueOne,
      TimestampValue timestampValueTwo, Operator operator) {
    TimestampValue result = null;

    long timestampOne = timestampValueOne.getTimestamp();
    double valueOne = timestampValueOne.getValue();
    long timestampTwo = timestampValueTwo.getTimestamp();
    double valueTwo = timestampValueTwo.getValue();
    long resultTimestamp = timestampOne < timestampTwo ? timestampOne
        : timestampTwo;
    double resultValue = 0.0;
    final char operatorValue = operator.getValue();

    switch (operatorValue) {
    case '+':
      resultValue = valueOne + valueTwo;
      break;
    case '-':
      resultValue = valueOne - valueTwo;
      break;
    case '*':
      resultValue = valueOne * valueTwo;
      break;
    case '/':
      resultValue = valueOne / valueTwo;
      break;
    }

    result = new TimestampValue(resultTimestamp, resultValue);

    return result;
  }

  private TimestampValue iterate(final Iterator<TimestampValue> iterator,
      final TimestampValue fallback) {
    TimestampValue result = fallback;

    if (iterator.hasNext()) {
      result = iterator.next();
    }

    return result;
  }
}