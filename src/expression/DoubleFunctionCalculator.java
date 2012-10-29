package net.opentsdb.expression;

public class DoubleFunctionCalculator extends FunctionCalculator {
  public DoubleFunctionCalculator(String name) {
    super(name);
  }

  @Override
  public NodeResult calculate(NodeResult... parameters) {
    NodeResult result = null;

    if (parameters.length == 1) {
      NodeResult parameter = parameters[0];

      if (parameter instanceof ArithmeticNodeResult) {
        ArithmeticNodeResult arithmeticNodeResult = new ArithmeticNodeResult(
            getLabel(parameters));
        for (TimestampValue value : (ArithmeticNodeResult) parameter) {
          arithmeticNodeResult.add(new TimestampValue(value.getTimestamp(),
              value.getValue() * 2));
        }
        result = arithmeticNodeResult;
      } else {
        result = new LiteralNodeResult(
            ((LiteralNodeResult) parameter).getValue() * 2);
      }
    }

    return result;
  }
}
