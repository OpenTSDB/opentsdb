package net.opentsdb.expression;

public abstract class NodeResult {
  private String name;
  
  public NodeResult(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
