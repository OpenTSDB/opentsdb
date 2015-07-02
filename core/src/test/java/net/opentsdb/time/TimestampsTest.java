package net.opentsdb.time;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimestampsTest {
  private static final long BAD_TIMESTAMP = -9L;
  private static final long GOOD_TIMESTAMP = 9L;

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimestampNegativeTimestamp() {
    Timestamps.checkTimestamp(BAD_TIMESTAMP);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_TIMESTAMP, Timestamps.checkTimestamp(GOOD_TIMESTAMP));
  }
}
