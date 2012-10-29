package net.opentsdb.expression;

public class LiteralNode extends ArithmeticNode {
  private static final long serialVersionUID = 6594055430692570904L;

  private double value;

  public LiteralNode(String value) {
    this.value = Double.parseDouble(value);
  }

  public double getValue() {
    return value;
  }

  public NodeResult[] getDataPointsValues() {
    return new NodeResult[] { new LiteralNodeResult(value) };
  }
}