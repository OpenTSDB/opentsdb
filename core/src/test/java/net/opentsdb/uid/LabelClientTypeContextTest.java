package net.opentsdb.uid;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import net.opentsdb.DaggerTestComponent;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.utils.TestUtil;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.util.SortedMap;
import javax.inject.Inject;

public final class LabelClientTypeContextTest {
  @Inject TsdbStore store;
  @Inject MetricRegistry metrics;
  @Inject EventBus eventBus;

  private LabelClientTypeContext typeContext;

  @Rule
  public final Timeout timeout = Timeout.millis(TestUtil.TIMEOUT);

  @Before
  public void setUp() throws IOException {
    DaggerTestComponent.create().inject(this);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoTsdbStore() {
    typeContext = new LabelClientTypeContext(null, LabelType.METRIC, metrics, eventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoTable() {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoType() {
    typeContext = new LabelClientTypeContext(store, null, metrics, eventBus);
  }

  @Test(expected = NullPointerException.class)
  public void testCtorNoEventbus() {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, null);
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getNameSuccessfulLookup() throws Exception {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);

    final LabelId id = store.allocateLabel("foo", LabelType.METRIC).get();

    assertEquals("foo", typeContext.getName(id).get());
    // Should be a cache hit ...
    assertEquals("foo", typeContext.getName(id).get());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(1, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected = NoSuchUniqueId.class, timeout = TestUtil.TIMEOUT)
  public void getNameForNonexistentId() throws Exception {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);
    typeContext.getName(mock(LabelId.class)).get();
  }

  @Test(timeout = TestUtil.TIMEOUT)
  public void getIdSuccessfulLookup() throws Exception {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);

    final LabelId id = store.allocateLabel("foo", LabelType.METRIC).get();

    assertEquals(id, typeContext.getId("foo").get());
    // Should be a cache hit ...
    assertEquals(id, typeContext.getId("foo").get());
    // Should be a cache hit too ...
    assertEquals(id, typeContext.getId("foo").get());

    final SortedMap<String, Counter> counters = metrics.getCounters();
    assertEquals(2, counters.get("uid.cache-hit:kind=metrics").getCount());
    assertEquals(1, counters.get("uid.cache-miss:kind=metrics").getCount());
    assertEquals(2, metrics.getGauges().get("uid.cache-size:kind=metrics").getValue());
  }

  @Test(expected = NoSuchUniqueName.class, timeout = TestUtil.TIMEOUT)
  public void getIdForNonexistentName() throws Exception {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);
    typeContext.getId("foo").get();
  }

  @Test
  public void createIdPublishesEventOnSuccess() throws Exception {
    typeContext = new LabelClientTypeContext(store, LabelType.METRIC, metrics, eventBus);
    typeContext.createId("foo").get();
    verify(eventBus).post(any(LabelCreatedEvent.class));
  }
}
