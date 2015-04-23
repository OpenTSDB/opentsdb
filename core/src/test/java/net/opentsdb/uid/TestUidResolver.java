package net.opentsdb.uid;

import com.google.common.collect.ImmutableSet;

import com.typesafe.config.Config;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.core.UniqueIdClient;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static net.opentsdb.uid.UniqueIdType.TAGK;
import static net.opentsdb.uid.UniqueIdType.TAGV;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class TestUidResolver {
  public static final ImmutableSet<String> METRICS_1 = ImmutableSet.of("sys.cpu.0", "sys.cpu.1", "sys.cpu.2");
  public static final ImmutableSet<String> METRICS_2 = ImmutableSet.of("sys.cpu.2", "sys.cpu.0", "sys.cpu.1");

  private UidResolver resolver;

  @Inject Config config;
  @Inject UniqueIdClient idClient;
  @Inject TsdbStore store;

  @Before
  public void setUp() throws IOException {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    resolver = new UidResolver(idClient, config);

    store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("sys.cpu.1", new byte[]{0, 0, 2}, METRIC);
    store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, METRIC);
    store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNullClient() {
    new UidResolver(null, config);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNullConfig() {
    new UidResolver(idClient, null);
  }

  @Test(expected = NullPointerException.class)
  public void testResolveNullUidNames() {
    resolver.resolve((Iterator<String>)null, METRIC);
  }

  @Test(expected = NullPointerException.class)
  public void testResolveNullUidType() {
    resolver.resolve(ImmutableSet.of("test"), null);
  }

  @Test
  public void testResolveEmpty() throws Exception {
    final List<byte[]> resolve = resolver.resolve(ImmutableSet.<String>of().iterator(), METRIC).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(0, resolve.size());
  }

  @Test
  public void testResolveOneMetric() throws Exception {
    final List<byte[]> resolve = resolver.resolve(ImmutableSet.of("sys.cpu.0"), METRIC).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertArrayEquals(new byte[]{0, 0, 1}, resolve.get(0));
  }

  @Test
  public void testResolveOneTagk() throws Exception {
    final List<byte[]> resolve = resolver.resolve(ImmutableSet.of("host"), TAGK).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertArrayEquals(new byte[]{0,0,1},resolve.get(0));
  }

  @Test
  public void testResolveOneTagv() throws Exception {
    final List<byte[]> resolve = resolver.resolve(ImmutableSet.of("web01"), TAGV).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertArrayEquals(new byte[]{0,0,1},resolve.get(0));
  }

  @Test
  public void testResolveMultipleMetric() throws Exception {
    final List<byte[]> resolve = resolver.resolve(METRICS_1, METRIC).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final List<byte[]> bytes = new ArrayList<>();
    bytes.add(new byte[]{0,0,1});
    bytes.add(new byte[]{0,0,2});
    bytes.add(new byte[]{0,0,3});
    assertArrayEquals(bytes.toArray(), resolve.toArray());
  }

  @Test
  public void testResolveMultipleMetricUnsorted() throws Exception {
    final List<byte[]> resolve = resolver.resolve(METRICS_2, METRIC).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    final List<byte[]> bytes = new ArrayList<>();
    bytes.add(new byte[]{0,0,1});
    bytes.add(new byte[]{0,0,2});
    bytes.add(new byte[]{0,0,3});
    assertArrayEquals(bytes.toArray(), resolve.toArray());
  }
}
