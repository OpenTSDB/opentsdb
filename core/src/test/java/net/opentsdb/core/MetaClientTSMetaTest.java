package net.opentsdb.core;

import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import org.junit.Before;
import org.junit.Test;

import javax.inject.Inject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

 public class MetaClientTSMetaTest {
  @Inject MetaClient metaClient;
  @Inject MemoryStore tsdb_store;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);
  }

  @Test
  public void delete() throws Exception {
    final TSMeta meta = metaClient.getTSMeta("000001000001000001", true)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.delete(meta);
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteNull() throws Exception {
    final TSMeta meta = new TSMeta();
    metaClient.delete(meta);
  }

  @Test
  public void storeNew() throws Exception {
    final TSMeta meta = new TSMeta(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1357300800000L);
    meta.setDisplayName("New DN");
    metaClient.create(meta);
    assertEquals("New DN", meta.getDisplayName());
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNull() throws Exception {
    final TSMeta meta = new TSMeta((String) null);
    metaClient.create(meta);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmpty() throws Exception {
    final TSMeta meta = new TSMeta("");
    metaClient.create(meta);
  }

  @Test
  public void metaExistsInStorage() throws Exception {
    assertTrue(metaClient.TSMetaExists("000001000001000001")
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void getTSMeta() throws Exception {
    final TSMeta meta = metaClient.getTSMeta("000001000001000001", true)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertEquals("sys.cpu.0", meta.getMetric().name());
    assertEquals(2, meta.getTags().size());
    assertEquals("host", meta.getTags().get(0).name());
    assertEquals("web01", meta.getTags().get(1).name());
    assertEquals(1, meta.getTotalDatapoints());
    // no support for timestamps in mockbase yet
    //assertEquals(1328140801L, meta.getLastReceived());
  }

  @Test
  public void getTSMetaDoesNotExist() throws Exception {
    final TSMeta meta = metaClient.getTSMeta("000002000001000001", true)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNull(meta);
  }

  @Test
  public void syncToStorage() throws Exception {
    final TSMeta meta = new TSMeta(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1357300800000L);
    meta.setDisplayName("New DN");
    metaClient.syncToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(42, meta.getRetention());
  }

  @Test
  public void syncToStorageOverwrite() throws Exception {
    final TSMeta meta = new TSMeta(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1357300800000L);
    meta.setDisplayName("New DN");
    metaClient.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("New DN", meta.getDisplayName());
    assertEquals(0, meta.getRetention());
  }

  @Test (expected = IllegalStateException.class)
  public void syncToStorageNoChanges() throws Exception {
    final TSMeta meta = new TSMeta("ABCD");
    metaClient.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageNullTSUID() throws Exception {
    final TSMeta meta = new TSMeta();
    metaClient.syncToStorage(meta, true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}
