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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.SortedMap;
import javax.inject.Inject;

public final class TestUniqueId {
  @Inject TsdbStore client;
  @Inject MetricRegistry metrics;
  @Inject EventBus idEventBus;

  private UniqueId uid;

  @Rule
  public final Timeout timeout = Timeout.millis(MockBase.DEFAULT_TIMEOUT);

  @Before
  public void setUp() throws IOException {
    ObjectGraph.create(new TestModule()).inject(this);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoTsdbStore() {
    uid = new UniqueId(null, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoTable() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoType() {
    uid = new UniqueId(client, null, metrics, idEventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoEventbus() {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, null);
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getNameSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final LabelId id = client.allocateUID("foo", UniqueIdType.METRIC).get();

    assertEquals("foo", uid.getName(id).get());
    // Should be a cache hit ...
    assertEquals("foo", uid.getName(id).get());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected = NoSuchUniqueId.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getNameForNonexistentId() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getName(mock(LabelId.class)).get();
  }

  @Test(timeout = MockBase.DEFAULT_TIMEOUT)
  public void getIdSuccessfulLookup() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);

    final LabelId id = client.allocateUID("foo", UniqueIdType.METRIC).get();

    assertEquals(id, uid.getId("foo").get());
    // Should be a cache hit ...
    assertEquals(id, uid.getId("foo").get());
    // Should be a cache hit too ...
    assertEquals(id, uid.getId("foo").get());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected = NoSuchUniqueName.class, timeout = MockBase.DEFAULT_TIMEOUT)
  public void getIdForNonexistentName() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.getId("foo").get();
  }

  @Test
  public void createIdPublishesEventOnSuccess() throws Exception {
    uid = new UniqueId(client, UniqueIdType.METRIC, metrics, idEventBus);
    uid.createId("foo").get();
    verify(idEventBus).post(any(LabelCreatedEvent.class));
  }
}
