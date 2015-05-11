package net.opentsdb.core;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.ConfigValueFactory;
import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.IdCreatedEvent;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Inject;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

public class MetaClientUIDMetaTest {
  @Inject Config config;
  @Inject EventBus idEventBus;
  @Inject TsdbStore store;

  @Inject UniqueIdClient uniqueIdClient;
  @Inject MetaClient metaClient;

  @Inject RTPublisher realtimePublisher;

  @Mock private SearchPlugin searchPlugin;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);
    MockitoAnnotations.initMocks(this);

    store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, METRIC);

    UIDMeta uidMeta = new UIDMeta(METRIC, new byte[] {0, 0, 1}, "sys.cpu.0");
    uidMeta.setDescription("Description");
    uidMeta.setCreated(1328140801);

    store.add(uidMeta);
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenEnabled() {
    config = config.withValue("tsd.core.meta.enable_realtime_uid",
            ConfigValueFactory.fromAnyRef(true));

    store = mock(TsdbStore.class);
    new MetaClient(store, idEventBus, searchPlugin, config, uniqueIdClient, realtimePublisher);
    idEventBus.post(new IdCreatedEvent(new byte[]{0, 0, 1}, "test", UniqueIdType.METRIC));
    verify(store).add(any(UIDMeta.class));
    verify(searchPlugin).indexUIDMeta(any(UIDMeta.class));
  }

  @Test
  public void createdIdEventCreatesUIDMetaWhenDisabled() {
    store = mock(TsdbStore.class);
    new MetaClient(store, idEventBus, searchPlugin, config, uniqueIdClient, realtimePublisher);
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
    metaClient.updateLabelMeta(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1328140801, meta.getCreated());
  }

  @Test
  public void syncToStorageOverwrite() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 1});
    metaClient.updateLabelMeta(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(meta.getDescription());
  }

  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    final UIDMeta meta = metaClient.getUIDMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.updateLabelMeta(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 2});
    metaClient.updateLabelMeta(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    UIDMeta meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "");
    metaClient.add(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}
