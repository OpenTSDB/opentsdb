package net.opentsdb.expression;

public class DoubleFunctionCalculator extends FunctionCalculator {
  public DoubleFunctionCalculator(String name) {
    super(name);
  }

  @Override
  public ArithmeticNodeResult calculate(ArithmeticNodeResult... parameters) {
    ArithmeticNodeResult result = new ArithmeticNodeResult(getLabel(parameters));

    if (parameters.length == 1) {
      for (TimestampValue value : parameters[0]) {
        result.add(new TimestampValue(value.getTimestamp(),
            value.getValue() * 2));
      }
    }

    return result;
  }
}
