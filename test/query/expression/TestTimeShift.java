package net.opentsdb.query.expression;

import com.stumbleupon.async.Deferred;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.MutableDataPoint;
import net.opentsdb.core.SeekableView;
import net.opentsdb.meta.Annotation;
import org.hbase.async.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class TestTimeShift {
  private TimeShift timeshift;
  private static final long BASE_TIME = 1356998400000L;
  private static final DataPoint[] DATA_POINTS = new DataPoint[] {
      // timestamp = 1,356,998,400,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME, 40),
      // timestamp = 1,357,000,400,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME + 2000000, 50),
      // timestamp = 1,357,002,000,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME + 3600000, 40),
      // timestamp = 1,357,002,005,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME + 3605000, 50),
      // timestamp = 1,357,005,600,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME + 7200000, 40),
      // timestamp = 1,357,007,600,000 ms
      MutableDataPoint.ofLongValue(BASE_TIME + 9200000, 50)
  };

  @Before
  public void setUp() throws Exception {
    this.timeshift = new TimeShift();
  }

  @After
  public void tearDown() throws Exception {
  }

  @Test
  public void parseParam() throws Exception {
    assertEquals(TimeUnit.DAYS.toMillis(7), this.timeshift.parseParam("+1week "));
    assertEquals(TimeUnit.DAYS.toMillis(1), this.timeshift.parseParam("+1days "));
    assertEquals(TimeUnit.HOURS.toMillis(1), this.timeshift.parseParam("+1hr "));
    assertEquals(TimeUnit.MINUTES.toMillis(1), this.timeshift.parseParam("+1min "));
    assertEquals(TimeUnit.SECONDS.toMillis(1), this.timeshift.parseParam("+1sec "));
    assertEquals(TimeUnit.DAYS.toMillis(7), this.timeshift.parseParam("+1 week "));
    assertEquals(TimeUnit.DAYS.toMillis(1), this.timeshift.parseParam("+1 days "));
    assertEquals(TimeUnit.HOURS.toMillis(1), this.timeshift.parseParam("+1 hr "));
    assertEquals(TimeUnit.MINUTES.toMillis(1), this.timeshift.parseParam("+1 min "));
    assertEquals(TimeUnit.SECONDS.toMillis(1), this.timeshift.parseParam("+1 sec "));
    assertEquals(TimeUnit.DAYS.toMillis(7), this.timeshift.parseParam("+1week"));
    assertEquals(TimeUnit.DAYS.toMillis(1), this.timeshift.parseParam("+1days"));
    assertEquals(TimeUnit.HOURS.toMillis(1), this.timeshift.parseParam("+1hr"));
    assertEquals(TimeUnit.MINUTES.toMillis(1), this.timeshift.parseParam("+1min"));
    assertEquals(TimeUnit.SECONDS.toMillis(1), this.timeshift.parseParam("+1sec"));
    assertEquals(TimeUnit.DAYS.toMillis(7), this.timeshift.parseParam("+1 week"));
    assertEquals(TimeUnit.DAYS.toMillis(1), this.timeshift.parseParam("+1 days"));
    assertEquals(TimeUnit.HOURS.toMillis(1), this.timeshift.parseParam("+1 hr"));
    assertEquals(TimeUnit.MINUTES.toMillis(1), this.timeshift.parseParam("+1 min"));
    assertEquals(TimeUnit.SECONDS.toMillis(1), this.timeshift.parseParam("+1 sec"));
    assertEquals(60000L, this.timeshift.parseParam("+1min"));
  }

  @Test
  public void shiftDataPoint() throws Exception {
    DataPoint actualDp = timeshift.shift(DATA_POINTS[0], 60000L);
    assertEquals(1356998460000L, actualDp.timestamp());
    DataPoint actualDp1 = timeshift.shift(DATA_POINTS[1], this.timeshift.parseParam("+1week"));
    assertEquals(1357605200000L, actualDp1.timestamp());
    DataPoint actualDp2 = timeshift.shift(DATA_POINTS[1], this.timeshift.parseParam("+130days"));
    assertEquals(1368232400000L, actualDp2.timestamp());
  }
}