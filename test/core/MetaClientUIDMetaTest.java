package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.google.common.eventbus.EventBus;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdCreatedEvent;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MetaClientUIDMetaTest {
  private Config config;
  private EventBus idEventBus;
  private TsdbStore store;

  private UniqueIdClient uniqueIdClient;
  private MetaClient metaClient;

  @Mock private SearchPlugin searchPlugin;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.initMocks(this);

    config = new Config(false);
    idEventBus = new EventBus();
    store = new MemoryStore();

    uniqueIdClient = new UniqueIdClient(store, config, new Metrics(new MetricRegistry()), idEventBus);
    metaClient = new MetaClient(store, idEventBus, searchPlugin, config, uniqueIdClient);

    store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, METRIC);

    UIDMeta uidMeta = new UIDMeta(METRIC, new byte[] {0, 0, 1}, "sys.cpu.0");
    uidMeta.setDescription("Description");
    uidMeta.setNotes("MyNotes");
    uidMeta.setCreated(1328140801);
    uidMeta.setDisplayName("System CPU");

    store.add(uidMeta);
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenEnabled() {
    config.overrideConfig("tsd.core.meta.enable_realtime_uid", "true");
    store = mock(TsdbStore.class);
    new MetaClient(store, idEventBus, searchPlugin, config, uniqueIdClient);
    idEventBus.post(new IdCreatedEvent(new byte[]{0, 0, 1}, "test", UniqueIdType.METRIC));
    verify(store).add(any(UIDMeta.class));
    verify(searchPlugin).indexUIDMeta(any(UIDMeta.class));
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenDisabled() {
    store = mock(TsdbStore.class);
    new MetaClient(store, idEventBus, searchPlugin, config, uniqueIdClient);
    idEventBus.post(new IdCreatedEvent(new byte[] {0, 0, 1}, "test", UniqueIdType.METRIC));
    verify(store, never()).add(any(UIDMeta.class));
    verify(searchPlugin, never()).indexUIDMeta(any(UIDMeta.class));
  }

  @Test
  public void getUIDMeta() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, "000003")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 3}, meta.getUID());
  }

  @Test
  public void getUIDMetaByte() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, new byte[]{0, 0, 3})
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.2", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 3}, meta.getUID());
  }

  @Test
  public void getUIDMetaExists() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.getType());
    assertEquals("sys.cpu.0", meta.getName());
    assertArrayEquals(new byte[]{0, 0, 1}, meta.getUID());
    assertEquals("MyNotes", meta.getNotes());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    metaClient.getUIDMeta(METRIC, "000002")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void delete() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.delete(meta);
  }

  @Test
  public void syncToStorage() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 1});
    meta.setDisplayName("New Display Name");
    metaClient.syncUIDMetaToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New Display Name", meta.getDisplayName());
    assertEquals("MyNotes", meta.getNotes());
    assertEquals(1328140801, meta.getCreated());
  }

  @Test
  public void syncToStorageOverwrite() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 1});
    meta.setDisplayName("New Display Name");
    metaClient.syncUIDMetaToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New Display Name", meta.getDisplayName());
    assertNull(meta.getNotes());
  }

  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.syncUIDMetaToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 2});
    meta.setDisplayName("Testing");
    metaClient.syncUIDMetaToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    UIDMeta meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "");
    metaClient.add(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}
