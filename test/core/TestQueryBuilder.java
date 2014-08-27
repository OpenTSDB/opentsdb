package net.opentsdb.core;

import net.opentsdb.storage.MemoryStore;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class TestQueryBuilder {
  private QueryBuilder builder;

  @Before
  public void before() throws Exception {
    MemoryStore tsdb_store = new MemoryStore();
    final TSDB tsdb = new TSDB(tsdb_store, new Config(false));

    builder = new QueryBuilder(tsdb);
  }

  @Test
  public void downsample() throws Exception {
    int downsampleInterval = (int) DateTime.parseDuration("60s");

    builder.withStartAndEndTime(1356998400, 1357041600)
            .downsample(downsampleInterval, Aggregators.SUM);

    final Query query = builder.createQuery().joinUninterruptibly();

    assertEquals(60000, query.getSampleInterval());
    assertEquals(1356998400, query.getStartTime());
    assertEquals(1357041600, (long) query.getEndTime().get());
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    int downsampleInterval = (int)DateTime.parseDuration("60s");

    builder.withStartAndEndTime(1356998400000L, 1357041600000L)
            .downsample(downsampleInterval, Aggregators.SUM);

    final Query query = builder.createQuery().joinUninterruptibly();

    assertEquals(60000, query.getSampleInterval());
    assertEquals(1356998400000L, query.getStartTime());
    assertEquals(1357041600000L, (long) query.getEndTime().get());
  }

  @Test(expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    builder.downsample(60, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    builder.downsample(0, Aggregators.SUM);
  }
}
