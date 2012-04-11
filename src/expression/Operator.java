package net.opentsdb.expression;

public enum Operator {
  ADD('+'), SUBTRACT('-'), MULTIPLY('*'), DIVIDE('/');

  private char value;

  private Operator(char value) {
    this.value = value;
  }

  public char getValue() {
    return value;
  }
}