package net.opentsdb.core;

/**
 * This is a utility class to work with OpenTSDBs internal representation of
 * timestamps.
 */
public class Timestamp {
  private Timestamp() {}

  /**
   * Convert the provided timestamp to milliseconds if needed,
   * otherwise just returns it.
   */
  static long inMilliseconds(final long timestamp) {
    return (timestamp & Const.SECOND_MASK) == 0 ? timestamp * 1000 : timestamp;
  }
}
