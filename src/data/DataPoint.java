package net.opentsdb.data;

public interface DataPoint {

  /**
   * Returns the timestamp (in milliseconds) associated with this data point.
   * @return A strictly positive, 32 bit integer.
   */
  long timestamp();

  /**
   * Tells whether or not the this data point is a value of integer type.
   * @return {@code true} if the {@code i}th value is of integer type,
   * {@code false} if it's of doubleing point type.
   */
  boolean isInteger();

  /**
   * Returns the value of the this data point as a {@code long}.
   * @throws ClassCastException if the {@code isInteger() == false}.
   */
  long longValue();

  /**
   * Returns the value of the this data point as a {@code double}.
   * @throws ClassCastException if the {@code isInteger() == true}.
   */
  double doubleValue();

  /**
   * Returns the value of the this data point as a {@code double}, even if
   * it's a {@code long}.
   * @return When {@code isInteger() == false}, this method returns the same
   * thing as {@link #doubleValue}.  Otherwise, it returns the same thing as
   * {@link #longValue}'s return value casted to a {@code double}.
   */
  double toDouble();

  long valueCount();
  
}
