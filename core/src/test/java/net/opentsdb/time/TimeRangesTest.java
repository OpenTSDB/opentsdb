package net.opentsdb.time;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TimeRangesTest {
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final long BAD_START = -5L;
  private static final long BAD_END = -3L;
  private static final long GOOD_END_LESS_THAN_START = 9L;
  private static final long INFINITE = TimeRanges.NOT_ENDED;


  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeStartTimestamp() {
    TimeRanges.checkStartTime(BAD_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeEndTimestamp() {
    TimeRanges.checkEndTime(BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeFiniteEndTimestamp() {
    TimeRanges.checkFiniteEndTime(BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckInfiniteFiniteEndTimestamp() {
    TimeRanges.checkFiniteEndTime(INFINITE);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_START, TimeRanges.checkStartTime(GOOD_START));
  }

  @Test
  public void testCheckGoodEndTimestamp() {
    assertEquals(GOOD_END, TimeRanges.checkStartTime(GOOD_END));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadEnd() {
    TimeRanges.checkTimeRange(GOOD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadStart() {
    TimeRanges.checkTimeRange(BAD_START, GOOD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBothBad() {
    TimeRanges.checkTimeRange(BAD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeTimespan() {
    TimeRanges.checkTimeRange(GOOD_START, GOOD_END_LESS_THAN_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeFiniteTimespan() {
    TimeRanges.checkFiniteTimeRange(GOOD_START, GOOD_END_LESS_THAN_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckInifiniteFiniteTimespan() {
    TimeRanges.checkFiniteTimeRange(GOOD_START, INFINITE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBadStart() {
    TimeRanges.checkFiniteTimeRange(BAD_START, GOOD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBadEnd() {
    TimeRanges.checkFiniteTimeRange(GOOD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBothBad() {
    TimeRanges.checkFiniteTimeRange(BAD_START, BAD_END);
  }
}
