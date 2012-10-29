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
  private final ArithmeticNode rootNode;

  public ArithmeticExpressionCalculator(String arithmeticExpression) {
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
    } else if (arithmeticNode instanceof MetricNode) {
      MetricNode metricNode = (MetricNode) arithmeticNode;
      DataPoints[] dataPoints = queryResults.get(metricNode.getName());

      metricNode.setDataPoints(dataPoints);
    }
  }

  private List<DataPoints> calculate(ArithmeticNode rootNode) {
    return repackageNodeResults(calculateInternal(rootNode));
  }

  private NodeResult[] calculateInternal(ArithmeticNode node) {
    NodeResult[] result = null;

    if (node instanceof OperatorNode) {
      result = calculateOperation((OperatorNode) node);
    } else if (node instanceof FunctionNode) {
      result = calculateFunction((FunctionNode) node);
    } else if (node instanceof MetricNode) {
      result = ((MetricNode) node).getDataPointsValues();
    } else if (node instanceof LiteralNode) {
      result = ((LiteralNode) node).getDataPointsValues();
    }

    return result;
  }

  private NodeResult[] calculateOperation(OperatorNode operator) {
    List<NodeResult> result = new ArrayList<NodeResult>();
    ArithmeticNode operandOne = operator.getOperandOne();
    ArithmeticNode operandTwo = operator.getOperandTwo();
    NodeResult[] operandOneNodeResults = calculateInternal(operandOne);
    NodeResult[] operandTwoNodeResults = calculateInternal(operandTwo);

    for (NodeResult operandOneNodeResult : operandOneNodeResults) {
      for (NodeResult operandTwoNodeResult : operandTwoNodeResults) {
        result.add(calculateOperandValues(operandOneNodeResult,
            operandTwoNodeResult, operator.getOperator()));
      }
    }

    return result.toArray(new NodeResult[result.size()]);
  }

  private NodeResult calculateOperandValues(NodeResult operandOneNodeResult,
      NodeResult operandTwoNodeResult, Operator operator) {
    NodeResult result = null;

    if (operandOneNodeResult != null && operandTwoNodeResult != null) {
      String label = operandOneNodeResult.getName() + operator.getValue()
          + operandTwoNodeResult.getName();

      if (operandOneNodeResult instanceof LiteralNodeResult
          && operandTwoNodeResult instanceof LiteralNodeResult) {
        result = calculateTwoLiteralNodeOperands(
            (LiteralNodeResult) operandOneNodeResult,
            (LiteralNodeResult) operandTwoNodeResult, operator);
      } else if (operandOneNodeResult instanceof LiteralNodeResult) {
        result = calculateLiteralNodeAndMetricNodeOperands(label,
            (ArithmeticNodeResult) operandTwoNodeResult,
            (LiteralNodeResult) operandOneNodeResult, operator);
      } else if (operandTwoNodeResult instanceof LiteralNodeResult) {
        result = calculateLiteralNodeAndMetricNodeOperands(label,
            (ArithmeticNodeResult) operandOneNodeResult,
            (LiteralNodeResult) operandTwoNodeResult, operator);
      } else {
        result = calculateTwoArithmeticNodeOperands(label,
            (ArithmeticNodeResult) operandOneNodeResult,
            (ArithmeticNodeResult) operandTwoNodeResult, operator);
      }
    }

    return result;
  }

  private LiteralNodeResult calculateTwoLiteralNodeOperands(
      LiteralNodeResult literalNodeResultOne,
      LiteralNodeResult literalNodeResultTwo, Operator operator) {
    LiteralNodeResult result = null;
    final double valueOne = literalNodeResultOne.getValue();
    final double valueTwo = literalNodeResultTwo.getValue();

    try {
      result = new LiteralNodeResult(calculateValues(valueOne, valueTwo,
          operator));
    } catch (MetricFormulaException e) {
      LOG.info(e.getMessage());
    }

    return result;
  }

  private ArithmeticNodeResult calculateLiteralNodeAndMetricNodeOperands(
      String label, ArithmeticNodeResult metricNodeResult,
      LiteralNodeResult literalNodeResult, Operator operator) {
    final ArithmeticNodeResult result = new ArithmeticNodeResult(label);
    final double operandOneValue = literalNodeResult.getValue();
    final Iterator<TimestampValue> iterator = metricNodeResult.iterator();

    while (iterator.hasNext()) {
      TimestampValue timestampValue = iterator.next();

      try {
        result.add(new TimestampValue(timestampValue.getTimestamp(),
            calculateValues(operandOneValue, timestampValue.getValue(),
                operator)));
      } catch (MetricFormulaException e) {
        LOG.info(e.getMessage());
      }
    }

    return result;
  }

  private ArithmeticNodeResult calculateTwoArithmeticNodeOperands(String label,
      ArithmeticNodeResult arithmeticNodeResultOne,
      ArithmeticNodeResult arithmeticNodeResultTwo, Operator operator) {
    final ArithmeticNodeResult result = new ArithmeticNodeResult(label);
    final Iterator<TimestampValue> iteratorOne = arithmeticNodeResultOne
        .iterator();
    final Iterator<TimestampValue> iteratorTwo = arithmeticNodeResultTwo
        .iterator();
    TimestampValue timestampValueOne = iteratorOne.next();
    TimestampValue lastTimestampValueOne = null;
    TimestampValue timestampValueTwo = iteratorTwo.next();
    TimestampValue lastTimestampValueTwo = null;

    while (!timestampValueOne.equals(lastTimestampValueOne)
        && !timestampValueTwo.equals(lastTimestampValueTwo)) {
      if (timestampValueOne.getTimestamp() == timestampValueTwo.getTimestamp()) {
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
          result.add(calculateTimestampValues(lastTimestampValueOne,
              lastTimestampValueTwo, operator));
        } catch (MetricFormulaException e) {
          LOG.info(e.getMessage());
        }
      }
    }

    return result;
  }

  private NodeResult[] calculateFunction(FunctionNode function) {
    List<NodeResult> result = new ArrayList<NodeResult>();
    FunctionCalculator calculator = FUNCTION_CALCULATORS
        .get(function.getName());
    List<ArithmeticNode> parameterNodes = function.getParameters();
    List<NodeResult[]> parameterNodesResults = calculateParameterNodesResults(parameterNodes);
    NodeResult[][] cartesianParameters = getCartesianParameters(parameterNodesResults);

    for (NodeResult[] parameters : cartesianParameters) {
      result.add(calculator.calculate(parameters));
    }

    return result.toArray(new NodeResult[result.size()]);
  }

  private List<DataPoints> repackageNodeResults(NodeResult[] nodeResults) {
    final List<DataPoints> result = new ArrayList<DataPoints>();

    for (NodeResult nodeResult : nodeResults) {
      if (nodeResult instanceof ArithmeticNodeResult) {
        final ArithmeticExpressionResultDataPoints datapoints = new ArithmeticExpressionResultDataPoints(
            nodeResult.getName());

        for (TimestampValue timestampValue : (ArithmeticNodeResult) nodeResult) {
          datapoints.add(new ArithmeticExpressionResultDataPoint(timestampValue
              .getTimestamp(), timestampValue.getValue()));
        }

        result.add(datapoints);
      }
    }

    return result;
  }

  private TimestampValue calculateTimestampValues(
      TimestampValue timestampValueOne, TimestampValue timestampValueTwo,
      Operator operator) throws MetricFormulaException {
    TimestampValue result = null;

    long timestampOne = timestampValueOne.getTimestamp();
    double valueOne = timestampValueOne.getValue();
    long timestampTwo = timestampValueTwo.getTimestamp();
    double valueTwo = timestampValueTwo.getValue();
    long resultTimestamp = timestampOne < timestampTwo ? timestampOne
        : timestampTwo;

    result = new TimestampValue(resultTimestamp, calculateValues(valueOne,
        valueTwo, operator));

    return result;
  }

  private double calculateValues(double valueOne, double valueTwo,
      Operator operator) throws MetricFormulaException {
    double result = 0.0;
    final char operatorValue = operator.getValue();

    switch (operatorValue) {
    case '+':
      result = valueOne + valueTwo;
      break;
    case '-':
      result = valueOne - valueTwo;
      break;
    case '*':
      result = valueOne * valueTwo;
      break;
    case '/':
      if (valueTwo != 0.0) {
        result = valueOne / valueTwo;
      } else {
        throw new MetricFormulaException("division by zero: " + valueOne + "/"
            + valueTwo);
      }
      break;
    }

    return result;
  }

  private List<NodeResult[]> calculateParameterNodesResults(
      List<ArithmeticNode> parameterNodes) {
    List<NodeResult[]> result = new ArrayList<NodeResult[]>();

    for (ArithmeticNode node : parameterNodes) {
      result.add(calculateInternal(node));
    }

    return result;
  }

  private NodeResult[][] getCartesianParameters(
      List<NodeResult[]> parameterNodesResults) {
    int n = parameterNodesResults.size();
    int solutions = 1;

    for (NodeResult[] parameterValues : parameterNodesResults) {
      solutions *= parameterValues.length;
    }

    NodeResult[][] allCombinations = new NodeResult[solutions][];

    for (int i = 0; i < solutions; i++) {
      List<NodeResult> combination = new ArrayList<NodeResult>();
      int j = 1;

      for (NodeResult[] parameterValues : parameterNodesResults) {
        combination.add(parameterValues[(i / j) % parameterValues.length]);
        j *= parameterValues.length;
      }

      allCombinations[i] = combination.toArray(new NodeResult[n]);
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