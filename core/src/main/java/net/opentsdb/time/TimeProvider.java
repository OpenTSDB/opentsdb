package net.opentsdb.time;

import java.util.Date;

/**
 * An implementation of a time provider provides methods to get the current time.
 */
public interface TimeProvider {
  /**
   * Get the current time expressed as a {@link java.util.Date} instance.
   *
   * @return The current time expressed as a date instance
   */
  Date now();

  /**
   * Get the current time expressed as the number of milliseconds since midnight on January 1, 1970
   * UTC.
   *
   * @return The current time expressed as the number of milliseconds since midnight on January 1,
   * 1970 UTC.
   */
  long currentTimeMillis();
}
