package net.opentsdb.expression;

/**
 * Interface for calculator classes that can calculate arbitrary functions for
 * arithmetic expression result values. Instances of this class have to provide
 * a default constructor, since they will be instantiated by reflection.
 * 
 * @author pgoetz
 */
public interface FunctionCalculator {
  /**
   * Calculates the values of the parameters list and returns the resulting
   * {@link TimestampValues}. This method has to be implemented to create new
   * functions for the {@link ArithmeticExpressionCalculator}.
   * 
   * @param parameters
   * @return function calculation result
   */
  TimestampValues calculate(TimestampValues... parameters);
}
