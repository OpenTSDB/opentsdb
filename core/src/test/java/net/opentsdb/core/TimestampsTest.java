package net.opentsdb.core;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import net.opentsdb.time.Timestamps;

public class TimestampsTest {
  private final static long BAD_TIMESTAMP = -9L;
  private final static long GOOD_TIMESTAMP = 9L;

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimestampNegativeTimestamp() {
    Timestamps.checkTimestamp(BAD_TIMESTAMP);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_TIMESTAMP, Timestamps.checkTimestamp(GOOD_TIMESTAMP));
  }
}
