package net.opentsdb.core;

import java.util.ArrayList;
import java.util.Map;

import net.opentsdb.storage.MemoryStore;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.primitives.Bytes;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestQueryBuilder {
  private QueryBuilder builder;

  private static final long GOOD_START = 1356998400L;
  private static final long GOOD_END   = 1356999400L;

  private static final long TOO_BIG_TIME = 17592186044416L;

  private static final long SAMPLE_INTERVAL = 60000;

  private static final byte[] SYS_CPU_USER_ID = new byte[]{0, 0, 1};
  private static final byte[] HOST_ID         = new byte[]{0, 0, 1};
  private static final byte[] WEB01_ID        = new byte[]{0, 0, 1};

  private static final String SYS_CPU_USER_NAME = "sys.cpu.user";
  private static final String HOST_NAME         = "host";
  private static final String WEB01_NAME        = "web01";

  private static final String TSUID1 = "000001000001000001";

  private Map<String, String> good_tags;

  @Before
  public void before() throws Exception {
    MemoryStore tsdb_store = new MemoryStore();
    final TSDB tsdb = new TSDB(tsdb_store, new Config(false));

    tsdb_store.allocateUID(SYS_CPU_USER_NAME, SYS_CPU_USER_ID, UniqueId.UniqueIdType.METRIC);
    tsdb_store.allocateUID(HOST_NAME, HOST_ID, UniqueId.UniqueIdType.TAGK);
    tsdb_store.allocateUID(WEB01_NAME, WEB01_ID, UniqueId.UniqueIdType.TAGV);

    good_tags = Maps.newHashMap();
    good_tags.put(HOST_NAME, WEB01_NAME);

    builder = new QueryBuilder(tsdb);
  }

  /*
   * Downsampling
   */

  @Test
  public void downsample() throws Exception {
    builder.withStartAndEndTime(GOOD_START, GOOD_END)
            .downsample(SAMPLE_INTERVAL, Aggregators.SUM);

    final Query query = builder.createQuery().joinUninterruptibly();

    assertEquals(SAMPLE_INTERVAL, query.getSampleInterval());
    assertEquals(GOOD_START, query.getStartTime());
    assertEquals(GOOD_END, (long) query.getEndTime().get());
  }

  @Test
  public void downsampleMilliseconds() throws Exception {
    builder.withStartAndEndTime(GOOD_START, GOOD_END)
            .downsample(SAMPLE_INTERVAL, Aggregators.SUM);

    final Query query = builder.createQuery().joinUninterruptibly();

    assertEquals(SAMPLE_INTERVAL, query.getSampleInterval());
    assertEquals(GOOD_START, query.getStartTime());
    assertEquals(GOOD_END, (long) query.getEndTime().get());
  }

  @Test(expected = NullPointerException.class)
  public void downsampleNullAgg() throws Exception {
    builder.downsample(60, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void downsampleInvalidInterval() throws Exception {
    builder.downsample(0, Aggregators.SUM);
  }

  /*
   * Start and end time
   */

  @Test
  public void withStartAndEndTime() throws Exception {
    builder.withStartAndEndTime(GOOD_START, GOOD_END);
    final Query query = builder.createQuery().joinUninterruptibly();
    assertEquals(GOOD_START, query.getStartTime());
    assertEquals(GOOD_END, (long) query.getEndTime().get());
  }

  @Test
  public void withEndTimeAbsent() throws Exception {
    builder.withStartAndEndTime(GOOD_START, Optional.<Long>absent());
    final Query query = builder.createQuery().joinUninterruptibly();
    assertEquals(GOOD_START, query.getStartTime());
    assertFalse(query.getEndTime().isPresent());
  }

  @Test (expected = NullPointerException.class)
  public void withEndTimeNull() throws Exception {
    builder.withStartAndEndTime(GOOD_START, null);
  }

  @Test
  public void withStartTimeZero() throws Exception {
    builder.withStartTime(0);
    final Query query = builder.createQuery().joinUninterruptibly();
    assertEquals(0, query.getStartTime());
  }

  @Test (expected = IllegalArgumentException.class)
  public void withStartTimeInvalidNegative() throws Exception {
    builder.withStartTime(-1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withEndTimeInvalidNegative() throws Exception {
    builder.withStartAndEndTime(GOOD_START, -1L);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withStartTimeInvalidTooBig() throws Exception {
    builder.withStartAndEndTime(TOO_BIG_TIME, GOOD_END);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withEndTimeInvalidTooBig() throws Exception {
    builder.withStartAndEndTime(GOOD_START, TOO_BIG_TIME);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withStartTimeEqualtoEndTime() throws Exception {
    builder.withStartAndEndTime(GOOD_START, GOOD_START);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withStartTimeGreaterThanEndTime() throws Exception {
    builder.withStartAndEndTime(GOOD_START + 10, GOOD_START);
  }

  /*
   * Metric and tags
   */

  @Test (expected = NullPointerException.class)
  public void withNullMetric() {
    builder.withMetric(null);
  }

  @Test
  public void withGoodMetric() throws Exception {
    builder.withMetric(SYS_CPU_USER_NAME);
    Query query = builder.createQuery().joinUninterruptibly();
    assertArrayEquals(SYS_CPU_USER_ID, query.getMetric());
  }

  @Test (expected = NoSuchUniqueName.class)
  public void withMetricNosuchMetric() throws Exception {
    builder.withMetric("nometric");
    builder.createQuery().joinUninterruptibly();
  }

  @Test (expected = NullPointerException.class)
  public void withNullTags() {
    builder.withTags(null);
  }

  @Test
  public void withEmptyTags() throws Exception {
    builder.withTags(Maps.<String, String>newHashMap());
    Query query = builder.createQuery().joinUninterruptibly();

    ArrayList<byte[]> tag_ids = query.getTags();
    assertEquals(0, tag_ids.size());
  }

  @Test
  public void withGoodTags() throws Exception {
    builder.withTags(good_tags);
    Query query = builder.createQuery().joinUninterruptibly();

    ArrayList<byte[]> tag_ids = query.getTags();
    assertEquals(1, tag_ids.size());

    byte[] expected = Bytes.concat(HOST_ID, WEB01_ID);
    assertArrayEquals(expected, tag_ids.get(0));
  }

  @Test (expected = NoSuchUniqueName.class)
  public void withTagsNosuchTagk() throws Exception {
    good_tags.clear();
    good_tags.put("dc", WEB01_NAME);
    builder.withTags(good_tags);
    builder.createQuery().joinUninterruptibly();
  }

  @Test (expected = NoSuchUniqueName.class)
  public void setTimeSeriesNosuchTagv() throws Exception {
    good_tags.clear();
    good_tags.put(HOST_NAME, "noweb");
    builder.withTags(good_tags);
    builder.createQuery().joinUninterruptibly();
  }

  /*
   * TSUIDS
   */
  @Test (expected = NullPointerException.class)
  public void withTSUIDSNullList() throws Exception {
    builder.withTSUIDS(null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void withTSUIDSEmptyList() throws Exception {
    builder.withTSUIDS(Lists.<String>newArrayList());
  }

  @Test (expected = IllegalArgumentException.class)
  public void withTSUIDSDifferentMetrics() throws Exception {
    builder.withTSUIDS(Lists.newArrayList(TSUID1, "000002000001000002"));
  }

  @Test
  public void setTimeSeriesTS() throws Exception {
    builder.withTSUIDS(Lists.newArrayList(TSUID1));
    Query query = builder.createQuery().joinUninterruptibly();
    assertEquals(TSUID1, query.getTSUIDS().get(0));
  }
}
