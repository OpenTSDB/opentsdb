package net.opentsdb.expression;

/**
 * Interface for calculator classes that can calculate arbitrary functions for
 * arithmetic expression result values. Instances of this class have to provide
 * a default constructor, since they will be instantiated by reflection.
 * 
 * @author pgoetz
 */
public abstract class FunctionCalculator {
  private String name;

  public FunctionCalculator(String name) {
    this.name = name;
  }

  /**
   * Calculates the values of the parameters list and returns the resulting
   * {@link ArithmeticNodeResult}. This method has to be implemented to create
   * new functions for the {@link ArithmeticExpressionCalculator}.
   * 
   * @param parameters
   * @return function calculation result
   */
  public abstract NodeResult calculate(NodeResult... parameters);

  protected String getLabel(NodeResult... parameters) {
    StringBuilder result = new StringBuilder();

    result.append(name).append("(");

    for (NodeResult parameter : parameters) {
      result.append(parameter.getName()).append(", ");
    }

    if (result.length() > name.length() + 1) {
      result.delete(result.length() - 2, result.length());
    }

    result.append(")");

    return result.toString();
  }
}
