package net.opentsdb.expression;

public class DoubleFunctionCalculator implements FunctionCalculator {
  @Override
  public TimestampValues calculate(TimestampValues... parameters) {
    TimestampValues result = new TimestampValues();

    if (parameters.length == 1) {
      for (TimestampValue value : parameters[0]) {
        result.add(new TimestampValue(value.getTimestamp(),
            value.getValue() * 2));
      }
    }

    return result;
  }
}
