package net.opentsdb.time;

import java.util.Date;

/**
 * A {@link TimeProvider} that uses standard JDK methods to return the current
 * time.
 */
public class JdkTimeProvider implements TimeProvider {
  public Date now() {
    return new Date();
  }

  public long currentTimeMillis() {
    return System.currentTimeMillis();
  }
}
