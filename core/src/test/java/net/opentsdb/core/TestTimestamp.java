package net.opentsdb.core;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestTimestamp {
  private final static long BAD_TIMESTAMP = -9L;
  private final static long GOOD_TIMESTAMP = 9L;

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimestampNegativeTimestamp() {
    Timestamp.checkTimestamp(BAD_TIMESTAMP);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_TIMESTAMP, Timestamp.checkTimestamp(GOOD_TIMESTAMP));
  }
}
