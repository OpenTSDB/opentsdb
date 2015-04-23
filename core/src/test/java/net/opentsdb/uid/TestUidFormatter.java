package net.opentsdb.uid;

import java.io.IOException;
import java.util.Map;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.UniqueIdClient;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;

import com.google.common.collect.Maps;
import com.stumbleupon.async.DeferredGroupException;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.junit.Assert.assertEquals;

public class TestUidFormatter {
  private UidFormatter formatter;

  @Inject UniqueIdClient idClient;
  @Inject TsdbStore store;

  @Before
  public void setUp() throws IOException {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    formatter = new UidFormatter(idClient);

    store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNullTsdb() {
    new UidFormatter(null);
  }

  @Test(expected = NullPointerException.class)
  public void testFormatMetricNullUid() {
    formatter.formatMetric(null);
  }

  @Test
  public void testFormatMetricsReturnsName() throws Exception {
    assertEquals("sys.cpu.0", formatter.formatMetric(new byte[]{0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test(expected = NoSuchUniqueId.class)
  public void testFormatMetricsNSU() throws Exception {
    formatter.formatMetric(new byte[] {0, 0, 2}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void tagFormatTags() throws Exception {
    Map<String, String> result = Maps.newHashMap();
    result.put("host", "web01");

    Map<byte[], byte[]> query = Maps.newHashMap();
    query.put(new byte[]{0, 0, 1}, new byte[]{0, 0, 1});

    assertEquals(result, formatter.formatTags(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test(expected = NoSuchUniqueId.class)
  public void tagFormatTagsNSU() throws Throwable {
    Map<byte[], byte[]> query = Maps.newHashMap();
    query.put(new byte[]{0, 0, 2}, new byte[]{0, 0, 2});

    try {
      formatter.formatTags(query).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }
}
