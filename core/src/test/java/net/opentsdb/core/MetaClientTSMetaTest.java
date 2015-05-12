package net.opentsdb.core;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.DeferredGroupException;
import dagger.ObjectGraph;
import net.opentsdb.TestModuleMemoryStore;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.NoSuchUniqueId;
import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.inject.Inject;

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@PowerMockIgnore({"javax.management.*", "javax.xml.*",
          "ch.qos.*", "org.slf4j.*",
          "com.sum.*", "org.xml.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest({KeyValue.class, Scanner.class, TSMeta.class})
 public class MetaClientTSMetaTest {
  private static byte[] NAME_FAMILY = toBytes("name");

  @Inject MetaClient metaClient;
  @Inject MemoryStore tsdb_store;

  @Before
  public void before() throws Exception {
    ObjectGraph.create(new TestModuleMemoryStore()).inject(this);

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("metrics"),
            toBytes("sys.cpu.0"));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("metric_meta"),
            toBytes("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"System CPU\"}"));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("tagk"),
            toBytes("host"));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("tagk_meta"),
            toBytes("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"Host server name\"}"));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("tagv"),
            toBytes("web01"));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            toBytes("tagv_meta"),
            toBytes("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"Web server 1\"}"));

    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_meta"),
            toBytes("{\"tsuid\":\"000001000001000001\",\"" +
                    "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
                    "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
                    "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}"));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_ctr"),
            Bytes.fromLong(1L));
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
  public void metaExistsInStorageNot() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    assertFalse(metaClient.TSMetaExists("000001000001000001")
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

  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUMetric() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 2, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_meta"),
            toBytes("{\"tsuid\":\"000002000001000001\",\"" +
                    "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
                    "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
                    "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}"));
    try {
      metaClient.getTSMeta( "000002000001000001", true)
              .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagk() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 2, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_meta"),
            toBytes("{\"tsuid\":\"000001000002000001\",\"" +
                    "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
                    "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
                    "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}"));
    try {
      metaClient.getTSMeta( "000001000002000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
  }

  @Test (expected = NoSuchUniqueId.class)
  public void getTSMetaNSUTagv() throws Throwable {
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 2},
            NAME_FAMILY,
            toBytes("ts_meta"),
            toBytes("{\"tsuid\":\"000001000001000002\",\"" +
                    "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
                    "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
                    "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}"));
    try {
      metaClient.getTSMeta( "000001000001000002", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    } catch (DeferredGroupException e) {
      throw e.getCause();
    }
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

  @Test (expected = IllegalArgumentException.class)
  public void syncToStorageDoesNotExist() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    final TSMeta meta = new TSMeta(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}, 1357300800000L);
    metaClient.syncToStorage(meta, false).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }

  @Test
  public void parseFromColumn() throws Exception {
    final KeyValue column = PowerMockito.mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(tsdb_store.getColumn(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_meta")));
    final TSMeta meta = metaClient.parseFromColumn(column.key(), column.value(), false)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertNull(meta.getMetric());
  }

  @Test
  public void parseFromColumnWithUIDMeta() throws Exception {
    final KeyValue column = PowerMockito.mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 });
    when(column.value()).thenReturn(tsdb_store.getColumn(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            toBytes("ts_meta")));
    final TSMeta meta = metaClient.parseFromColumn(column.key(), column.value(), true)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(meta);
    assertEquals("000001000001000001", meta.getTSUID());
    assertNotNull(meta.getMetric());
    assertEquals("sys.cpu.0", meta.getMetric().name());
  }

  @Test (expected = NoSuchUniqueId.class)
  public void parseFromColumnWithUIDMetaNSU() throws Exception {
    class ErrBack implements Callback<Object, Exception> {
      @Override
      public Object call(Exception e) throws Exception {
        Throwable ex = e;
        while (ex.getClass().equals(DeferredGroupException.class)) {
          ex = ex.getCause();
        }
        throw (Exception)ex;
      }
    }

    final KeyValue column = PowerMockito.mock(KeyValue.class);
    when(column.key()).thenReturn(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 2 });
    when(column.value()).thenReturn(toBytes("{\"tsuid\":\"000001000001000002\",\"" +
            "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
            "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
            "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}"));
    metaClient.parseFromColumn(column.key(), column.value(), true).addErrback(new ErrBack())
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
  }
}
