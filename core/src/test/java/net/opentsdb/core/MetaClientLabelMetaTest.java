package net.opentsdb.core;

import com.google.common.eventbus.EventBus;
import dagger.ObjectGraph;
import net.opentsdb.TestModule;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.NoSuchUniqueId;
import com.typesafe.config.Config;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Inject;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class MetaClientLabelMetaTest {
  @Inject Config config;
  @Inject EventBus idEventBus;
  @Inject TsdbStore store;

  @Inject UniqueIdClient uniqueIdClient;
  @Inject MetaClient metaClient;

  @Inject
  RTPublisher realtimePublisher;

  @Mock private SearchPlugin searchPlugin;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);
    MockitoAnnotations.initMocks(this);

    store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    store.allocateUID("sys.cpu.2", new byte[]{0, 0, 3}, METRIC);

    LabelMeta labelMeta = LabelMeta.create(new byte[]{0, 0, 1}, METRIC, "sys.cpu.0", "Description", 1328140801);

    store.updateMeta(labelMeta);
  }

  @Test
  public void getUIDMeta() throws Exception {
    final LabelMeta meta = metaClient.getLabelMeta(METRIC, new byte[]{0, 0, 3})
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.type());
    assertEquals("sys.cpu.2", meta.name());
    assertArrayEquals(new byte[]{0, 0, 3}, meta.identifier());
  }

  @Test
  public void getUIDMetaExists() throws Exception {
    final LabelMeta meta = metaClient.getLabelMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.type());
    assertEquals("sys.cpu.0", meta.name());
    assertArrayEquals(new byte[]{0, 0, 1}, meta.identifier());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getUIDMetaNoSuch() throws Exception {
    metaClient.getLabelMeta(METRIC, "000002")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  /*
  TODO
  This needs major rework. We should probably use injection for this now.
  @Test
  public void syncToStorage() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 1});
    metaClient.update(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(1328140801, meta.created());
  }

  @Test
  public void syncToStorageOverwrite() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 1});
    metaClient.update(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(meta.description());
  }

  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    final UIDMeta meta = metaClient.getLabelMeta(METRIC, "000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.update(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = NoSuchUniqueId.class)
  public void syncToStorageNoSuch() throws Exception {
    final UIDMeta meta = new UIDMeta(METRIC, new byte[]{0, 0, 2});
    metaClient.update(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    UIDMeta meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "");
    metaClient.add(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
  */
}
