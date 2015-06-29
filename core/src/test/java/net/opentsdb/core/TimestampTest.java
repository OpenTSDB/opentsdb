package net.opentsdb.core;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

/**
 * Created by johannes on 2015-06-29.
 */
public class TimestampTest {
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final long BAD_START = -5L;
  private static final long BAD_END = -3L;
  private static final long GOOD_END_LESS_THAN_START = 9L;


  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeStartTimestamp() {
    Timestamp.checkStartTime(BAD_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeEndTimestamp() {
    Timestamp.checkEndTime(BAD_END);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_START, Timestamp.checkStartTime(GOOD_START));
  }

  @Test
  public void testCheckGoodEndTimestamp() {
    assertEquals(GOOD_END, Timestamp.checkStartTime(GOOD_END));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadEnd() {
    Timestamp.checkTimeSpan(GOOD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadStart() {
    Timestamp.checkTimeSpan(BAD_START, GOOD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBothBad() {
    Timestamp.checkTimeSpan(BAD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeTimeSpan() {
    Timestamp.checkTimeSpan(GOOD_START, GOOD_END_LESS_THAN_START);
  }

}
