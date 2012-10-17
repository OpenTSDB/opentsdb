package net.opentsdb.expression;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
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

  private final Map<String, FunctionCalculator> FUNCTION_CALCULATORS;
  private final String arithmeticExpression;
  private final ArithmeticNode rootNode;

  public ArithmeticExpressionCalculator(String arithmeticExpression) {
    this.arithmeticExpression = arithmeticExpression;
    rootNode = parseArithmeticExpression(arithmeticExpression);

    FUNCTION_CALCULATORS = FunctionUtils.getFunctions();
  }

  public List<DataPoints> calculateArithmeticExpression(
      final Map<String, DataPoints[]> queryResults) {
    List<DataPoints> result = null;

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
    } else if (arithmeticNode instanceof FunctionNode) {
      FunctionNode functionNode = (FunctionNode) arithmeticNode;

      for (ArithmeticNode childNode : functionNode.getParameters()) {
        addDataPointsToMetricNodes(childNode, queryResults);
      }
    } else {
      MetricNode metricNode = (MetricNode) arithmeticNode;
      DataPoints[] dataPoints = queryResults.get(metricNode.getName());

      metricNode.setDataPoints(dataPoints);
    }
  }

  private List<DataPoints> calculate(ArithmeticNode rootNode) {
    return repackageTimestampValues(calculateInternal(rootNode));
  }

  private TimestampValues[] calculateInternal(ArithmeticNode node) {
    TimestampValues[] timestampValues = null;

    if (node instanceof OperatorNode) {
      timestampValues = calculateOperation((OperatorNode) node);
    } else if (node instanceof FunctionNode) {
      timestampValues = calculateFunction((FunctionNode) node);
    } else {
      timestampValues = ((MetricNode) node).getDataPointsValues();
    }

    return timestampValues;
  }

  private TimestampValues[] calculateOperation(OperatorNode operator) {
    List<TimestampValues> result = new ArrayList<TimestampValues>();
    ArithmeticNode operandOne = operator.getOperandOne();
    ArithmeticNode operandTwo = operator.getOperandTwo();
    TimestampValues[] operandOneTimestampValuesArray = calculateInternal(operandOne);
    TimestampValues[] operandTwoTimestampValuesArray = calculateInternal(operandTwo);

    for (TimestampValues operandOneTimestampValues : operandOneTimestampValuesArray) {
      for (TimestampValues operandTwoTimestampValues : operandTwoTimestampValuesArray) {
        result.add(calculateOperandValues(operandOneTimestampValues,
            operandTwoTimestampValues, operator.getOperator()));
      }
    }

    return result.toArray(new TimestampValues[] {});
  }

  private TimestampValues calculateOperandValues(
      TimestampValues operandOneTimestampValues,
      TimestampValues operandTwoTimestampValues, Operator operator) {
    final TimestampValues result = new TimestampValues();

    if (operandOneTimestampValues != null
        && !operandOneTimestampValues.isEmpty()
        && operandTwoTimestampValues != null
        && !operandTwoTimestampValues.isEmpty()) {
      final Iterator<TimestampValue> iteratorOne = operandOneTimestampValues
          .iterator();
      final Iterator<TimestampValue> iteratorTwo = operandTwoTimestampValues
          .iterator();
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
          try {
            result.add(calculateValues(lastTimestampValueOne,
                lastTimestampValueTwo, operator));
          } catch (MetricFormulaException e) {
            LOG.info(e.getMessage());
          }
        }
      }
    }

    return result;
  }

  private TimestampValues[] calculateFunction(FunctionNode function) {
    List<TimestampValues> result = new ArrayList<TimestampValues>();
    FunctionCalculator calculator = FUNCTION_CALCULATORS
        .get(function.getName());
    List<ArithmeticNode> parameterNodes = function.getParameters();
    List<TimestampValues[]> parameterNodesResults = calculateParameterNodesResults(parameterNodes);
    TimestampValues[][] cartesianParameters = getCartesianParameters(parameterNodesResults);

    for (TimestampValues[] parameters : cartesianParameters) {
      result.add(calculator.calculate(parameters));
    }

    return result.toArray(new TimestampValues[0]);
  }

  private List<DataPoints> repackageTimestampValues(
      TimestampValues[] timestampValuesArray) {
    final List<DataPoints> result = new ArrayList<DataPoints>();

    for (TimestampValues timestampValues : timestampValuesArray) {
      final ArithmeticExpressionResultDataPoints datapoints = new ArithmeticExpressionResultDataPoints(
          arithmeticExpression);

      for (TimestampValue timestampValue : timestampValues) {
        datapoints.add(new ArithmeticExpressionResultDataPoint(timestampValue
            .getTimestamp(), timestampValue.getValue()));
      }

      result.add(datapoints);
    }

    return result;
  }

  private TimestampValue calculateValues(TimestampValue timestampValueOne,
      TimestampValue timestampValueTwo, Operator operator)
      throws MetricFormulaException {
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
      if (valueTwo != 0.0) {
        resultValue = valueOne / valueTwo;
      } else {
        throw new MetricFormulaException("division by zero: " + valueOne + "/"
            + valueTwo);
      }
      break;
    }

    result = new TimestampValue(resultTimestamp, resultValue);

    return result;
  }

  private List<TimestampValues[]> calculateParameterNodesResults(
      List<ArithmeticNode> parameterNodes) {
    List<TimestampValues[]> result = new ArrayList<TimestampValues[]>();

    for (ArithmeticNode node : parameterNodes) {
      result.add(calculateInternal(node));
    }

    return result;
  }

  private TimestampValues[][] getCartesianParameters(
      List<TimestampValues[]> parameterNodesResults) {
    int n = parameterNodesResults.size();
    int solutions = 1;

    for (TimestampValues[] parameterValues : parameterNodesResults) {
      solutions *= parameterValues.length;
    }

    TimestampValues[][] allCombinations = new TimestampValues[solutions][];

    for (int i = 0; i < solutions; i++) {
      List<TimestampValues> combination = new ArrayList<TimestampValues>();
      int j = 1;

      for (TimestampValues[] parameterValues : parameterNodesResults) {
        combination.add(parameterValues[(i / j) % parameterValues.length]);
        j *= parameterValues.length;
      }

      allCombinations[i] = combination.toArray(new TimestampValues[n]);
    }

    return allCombinations;

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