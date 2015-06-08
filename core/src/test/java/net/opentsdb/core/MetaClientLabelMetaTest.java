package net.opentsdb.core;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertEquals;

import net.opentsdb.TestModule;
import net.opentsdb.meta.LabelMeta;
import net.opentsdb.plugins.RTPublisher;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.LabelId;

import com.google.common.eventbus.EventBus;
import com.typesafe.config.Config;
import dagger.ObjectGraph;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.inject.Inject;

public class MetaClientLabelMetaTest {
  @Inject Config config;
  @Inject EventBus idEventBus;
  @Inject TsdbStore store;

  @Inject UniqueIdClient uniqueIdClient;
  @Inject MetaClient metaClient;

  @Inject RTPublisher realtimePublisher;

  @Mock private SearchPlugin searchPlugin;

  private LabelId sysCpu0;
  private LabelId sysCpu2;

  @Before
  public void setUp() throws Exception {
    ObjectGraph.create(new TestModule()).inject(this);
    MockitoAnnotations.initMocks(this);

    sysCpu0 = store.allocateUID("sys.cpu.0", METRIC).join();
    sysCpu2 = store.allocateUID("sys.cpu.2", METRIC).join();

    LabelMeta labelMeta = LabelMeta.create(sysCpu0, METRIC, "sys.cpu.0", "Description", 1328140801);

    store.updateMeta(labelMeta);
  }

  @Test
  public void getUIDMeta() throws Exception {
    final LabelMeta meta = metaClient.getLabelMeta(METRIC, sysCpu2)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals(METRIC, meta.type());
    assertEquals("sys.cpu.2", meta.name());
    assertEquals(sysCpu2, meta.identifier());
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
