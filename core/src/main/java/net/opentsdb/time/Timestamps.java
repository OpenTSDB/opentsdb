package net.opentsdb.time;

import static com.google.common.base.Preconditions.checkArgument;

public final class Timestamps {

  private Timestamps() {
  }

  /**
   * Validates that a timestamp is llegal.
   *
   * @param timestamp a timestamp
   *
   * @throws IllegalArgumentException if timestamp is not equal to or greater than zero
   */
  public static long checkTimestamp(final long timestamp) {

    checkArgument(timestamp >= 0, "The timestamp must be positive and greater than zero but was %s",
        timestamp);

    return timestamp;
  }
}
