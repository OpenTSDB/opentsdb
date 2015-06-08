
package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import net.opentsdb.TestModule;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.SortedMap;
import javax.inject.Inject;

public final class TestUniqueId {
  @Inject TsdbStore client;
  @Inject MetricRegistry metrics;
  @Inject EventBus idEventBus;

  private UniqueId uid;

  @Before
  public void setUp() throws IOException{
    ObjectGraph.create(new TestModule()).inject(this);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTsdbStore() {
    uid = new UniqueId(null, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoTable() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoType() {
    uid = new UniqueId(client, null, metrics, idEventBus);
  }

  @Test(expected=NullPointerException.class)
  public void testCtorNoEventbus() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, null);
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getNameSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final LabelId id = client.allocateUID("foo", UniqueIdType.METRIC).join();

    assertEquals("foo", uid.getName(id).join());
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id).join());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected=NoSuchUniqueId.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getNameForNonexistentId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(mock(LabelId.class)).join();
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getIdSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final LabelId id = client.allocateUID("foo", UniqueIdType.METRIC).join();

    assertEquals(id, uid.getId("foo").join());
    // Should be a cache hit ...
    assertEquals(id, uid.getId("foo").join());
    // Should be a cache hit too ...
    assertEquals(id, uid.getId("foo").join());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected=NoSuchUniqueName.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getIdForNonexistentName() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getId("foo").join();
  }

  @Test
  public void createIdPublishesEventOnSuccess() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.createId("foo").joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    verify(idEventBus).post(any(LabelCreatedEvent.class));
  }
}
