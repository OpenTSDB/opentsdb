package net.opentsdb.expression;

public class MetricFormulaException extends Exception {
  private static final long serialVersionUID = -5167700800460080581L;

  public MetricFormulaException(final String message) {
    super(message);
  }
}