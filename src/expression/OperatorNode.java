package net.opentsdb.expression;

public class OperatorNode extends ArithmeticNode {
  private static final long serialVersionUID = -2418157170737850517L;

  private ArithmeticNode operandOne;
  private ArithmeticNode operandTwo;
  private Operator operator;

  public OperatorNode(ArithmeticNode operandOne, Operator operator,
      ArithmeticNode operandTwo) {
    this.operandOne = operandOne;
    this.operator = operator;
    this.operandTwo = operandTwo;
  }

  public ArithmeticNode getOperandOne() {
    return operandOne;
  }

  public ArithmeticNode getOperandTwo() {
    return operandTwo;
  }

  public Operator getOperator() {
    return operator;
  }

  public String toString() {
    return new StringBuilder().append(operandOne).append(' ')
        .append(operator.getValue()).append(' ').append(operandTwo).toString();
  }
}