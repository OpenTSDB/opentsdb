package net.opentsdb.expression;

import java.util.List;

public class FunctionNode extends ArithmeticNode {
  private static final long serialVersionUID = 6594055430692570904L;

  private String name;
  private List<ArithmeticNode> parameters;

  public FunctionNode(String name, List<ArithmeticNode> parameters) {
    this.name = name;
    this.parameters = parameters;
  }

  public String getName() {
    return name;
  }

  public List<ArithmeticNode> getParameters() {
    return parameters;
  }
}