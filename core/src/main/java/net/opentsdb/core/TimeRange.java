package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;

public class TimeRange {
  public static final long NOT_ENDED = -1;

  /**
   * Validates that the starTime timestamp is within valid bounds.
   *
   * @throws IllegalArgumentException if the timestamp isn't within bounds.
   */
  public static long checkStartTime(long startTime) {
    checkArgument(startTime >= 0,
        "The timestamp must be positive and greater than zero but was %s", startTime);

    return startTime;
  }

  /**
   * Validates that the endTime timestamp is within valid bounds.
   *
   * @throws IllegalArgumentException if the timestamp is negative or not equaling #NOT_ENDED.
   */
  public static long checkEndTime(long endTime) {
    checkArgument(endTime >= 0 || endTime == NOT_ENDED,
        "End time must be larger than 0 or equal to END_TIME but was %s", endTime);

    return endTime;
  }

  /**
   * Validates an explicit endTime that may not be set to #NOT_ENDED
   *
   * @param endTime endtime timestamp, must be greater or equal to zero
   *
   * @throws IllegalArgumentException if endTime is negative.
   */
  public static long checkFiniteEndTime(long endTime) {
    checkArgument(endTime >= 0,
        "End time must be larger than or equal to zero");

    return endTime;
  }

  /**
   * Validates that that a timespan is within correct bounderies. endTime can be set to #NOT_ENDED
   * indicating that it is a an open timespan begining at startTime.
   *
   * @param startTime startpoint timestamp, must be equal or greater than zero
   * @param endTime endpoint timestamp, can be #NOT_ENDED
   *
   * @throws IllegalArgumentException if either starTime or endTime is invalid or timespan
   * is negative. A timespan without an end is checked and allowed.
   */
  public static void checkTimespan(final long startTime, final long endTime) {
    checkStartTime(startTime);

    checkEndTime(endTime);

    checkArgument(endTime >= startTime || endTime == NOT_ENDED,
        "The end timestamp cannot be less than the start timestamp. Endtime %s, Startime %s",
        endTime, startTime);
  }

  /**
   * Validates that a timespan is finite and withing correct bounderiers.
   *
   * @param startTime startpoint timestamp, must be equal or greater than zero
   * @param endTime endpoint timestamp, must be be equal or greater than zero and
   * equal or greather than startTime
   *
   * @throws IllegalArgumentException if either startTime or endTime is invalid or
   * timespan is negative
   */
  public static void checkFiniteTimespan(long startTime, long endTime) {
    checkStartTime(startTime);

    checkFiniteEndTime(endTime);

    checkArgument(endTime >= startTime,
        "The end timestamp cannot be less than the start timestamp. Endtime %s, Startime %s",
        endTime, startTime);
  }
}
