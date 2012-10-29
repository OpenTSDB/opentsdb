package net.opentsdb.expression;

public class LiteralNodeResult extends NodeResult {
  private double value;

  public LiteralNodeResult(double value) {
    super(Double.toString(value));

    this.value = value;
  }

  public double getValue() {
    return value;
  }
}
