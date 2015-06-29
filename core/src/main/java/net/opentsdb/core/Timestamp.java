package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by johannes on 2015-06-29.
 */
public class Timestamp {

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
   * @throws IllegalArgumentException if the timestamp isn't within bounds.
   */
  public static long checkEndTime(long endTime) {
    checkArgument(endTime >= 0 || endTime == NOT_ENDED,
        "End time must be larger than 0 or equal to Annotation.END_TIME but was %s", endTime);

    return endTime;
  }

  /**
   * Validates that that a timespan is within correct bounderies
   *
   * @param startTime startpoint timestamp
   * @param endTime endpoint timestamp
   *
   * @throws IllegalArgumentException if either starTime or endTime is invalid or timespan
   * is non positive. A timespan without an end is checked and allowed.
   */
  public static void checkTimeSpan(final long startTime, final long endTime) {
    checkStartTime(startTime);

    checkEndTime(endTime);

    checkArgument(endTime >= startTime || endTime == NOT_ENDED,
        "starttime was greater than endtime. Illegal negative timespan, endtime - startime %s",
        endTime - startTime);
  }
}
