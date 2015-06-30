package net.opentsdb.core;

import static com.google.common.base.Preconditions.checkArgument;

/**
 * Created by johannes on 2015-06-29.
 */
public class Timestamp {

  /**
   * Validates that a timestamp is llegal.
   *
   * @param timestamp a timestamp
   *
   * @throws IllegalArgumentException if timestamp is not equal to or greater than zero
   */
  public static long checkTimestamp(long timestamp) {

    checkArgument(timestamp >= 0, "The timestamp must be positive and greater than zero but was %s",
        timestamp);

    return timestamp;
  }
}
