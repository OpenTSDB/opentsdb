package net.opentsdb.core;


import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class TestTimeRange {
  private static final long GOOD_START = 10L;
  private static final long GOOD_END = 12L;
  private static final long BAD_START = -5L;
  private static final long BAD_END = -3L;
  private static final long GOOD_END_LESS_THAN_START = 9L;
  private static final long INFINITE = net.opentsdb.core.TimeRange.NOT_ENDED;


  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeStartTimestamp() {
    TimeRange.checkStartTime(BAD_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeEndTimestamp() {
    TimeRange.checkEndTime(BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeFiniteEndTimestamp() {
    TimeRange.checkFiniteEndTime(BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckInfiniteFiniteEndTimestamp() {
    TimeRange.checkFiniteEndTime(INFINITE);
  }

  @Test
  public void testCheckGoodStartTimestamp() {
    assertEquals(GOOD_START, TimeRange.checkStartTime(GOOD_START));
  }

  @Test
  public void testCheckGoodEndTimestamp() {
    assertEquals(GOOD_END, TimeRange.checkStartTime(GOOD_END));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadEnd() {
    TimeRange.checkTimespan(GOOD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBadStart() {
    TimeRange.checkTimespan(BAD_START, GOOD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckTimespanBothBad() {
    TimeRange.checkTimespan(BAD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeTimespan() {
    TimeRange.checkTimespan(GOOD_START, GOOD_END_LESS_THAN_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNegativeFiniteTimespan() {
    TimeRange.checkFiniteTimespan(GOOD_START, GOOD_END_LESS_THAN_START);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckInifiniteFiniteTimespan() {
    TimeRange.checkFiniteTimespan(GOOD_START, INFINITE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBadStart() {
    TimeRange.checkFiniteTimespan(BAD_START, GOOD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBadEnd() {
    TimeRange.checkFiniteTimespan(GOOD_START, BAD_END);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckFiniteTimespanBothBad() {
    TimeRange.checkFiniteTimespan(BAD_START, BAD_END);
  }
}
