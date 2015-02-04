package net.opentsdb.core;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Maps;
import com.google.common.eventbus.EventBus;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.stats.Metrics;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;
import org.hbase.async.Bytes;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MetaClientTSMetaTest {
  private static byte[] NAME_FAMILY = "name".getBytes(Const.CHARSET_ASCII);
  private TSDB tsdb;
  private Config config;
  private MemoryStore tsdb_store;
  private TSMeta meta = new TSMeta();
  private MetaClient metaClient;
  private EventBus idEventBus;
  private SearchPlugin searchPlugin;

  @Before
  public void before() throws Exception {
    Map<String, String> overrides = Maps.newHashMap();
    overrides.put("tsd.core.meta.enable_tsuid_incrementing", "TRUE");
    overrides.put("tsd.core.meta.enable_realtime_ts", "TRUE");
    config = new Config(false, overrides);

    tsdb_store = new MemoryStore();
    tsdb = TsdbBuilder.createFromConfig(config)
            .withStore(tsdb_store)
            .build();

    idEventBus = new EventBus();
    searchPlugin = mock(SearchPlugin.class);

    UniqueIdClient uniqueIdClient = new UniqueIdClient(tsdb_store, config, new Metrics(new MetricRegistry()), idEventBus);
    metaClient = new MetaClient(tsdb_store, idEventBus, searchPlugin, config, uniqueIdClient);

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "metrics".getBytes(Const.CHARSET_ASCII),
            "sys.cpu.0".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "metric_meta".getBytes(Const.CHARSET_ASCII),
            ("{\"uid\":\"000001\",\"type\":\"METRIC\",\"name\":\"sys.cpu.0\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"System CPU\"}")
                    .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "tagk".getBytes(Const.CHARSET_ASCII),
            "host".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "tagk_meta".getBytes(Const.CHARSET_ASCII),
            ("{\"uid\":\"000001\",\"type\":\"TAGK\",\"name\":\"host\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"Host server name\"}")
                    .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "tagv".getBytes(Const.CHARSET_ASCII),
            "web01".getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1},
            NAME_FAMILY,
            "tagv_meta".getBytes(Const.CHARSET_ASCII),
            ("{\"uid\":\"000001\",\"type\":\"TAGV\",\"name\":\"web01\"," +
                    "\"description\":\"Description\",\"notes\":\"MyNotes\",\"created\":" +
                    "1328140801,\"displayName\":\"Web server 1\"}")
                    .getBytes(Const.CHARSET_ASCII));

    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            "ts_meta".getBytes(Const.CHARSET_ASCII),
            ("{\"tsuid\":\"000001000001000001\",\"" +
                    "description\":\"Description\",\"notes\":\"Notes\",\"created\":1328140800," +
                    "\"custom\":null,\"units\":\"\",\"retention\":42,\"max\":1.0,\"min\":" +
                    "\"NaN\",\"displayName\":\"Display\",\"dataType\":\"Data\"}")
                    .getBytes(Const.CHARSET_ASCII));
    tsdb_store.addColumn(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1},
            NAME_FAMILY,
            "ts_ctr".getBytes(Const.CHARSET_ASCII),
            Bytes.fromLong(1L));
  }

  @Test
  public void delete() throws Exception {
    meta = tsdb.getTSMeta( "000001000001000001", true).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    metaClient.delete(meta);
  }

  @Test (expected = IllegalArgumentException.class)
  public void deleteNull() throws Exception {
    meta = new TSMeta();
    metaClient.delete(meta);
  }

  @Test
  public void storeNew() throws Exception {
    meta = new TSMeta(new byte[] { 0, 0, 1, 0, 0, 1, 0, 0, 1 }, 1357300800000L);
    meta.setDisplayName("New DN");
    metaClient.create(meta);
    assertEquals("New DN", meta.getDisplayName());
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNull() throws Exception {
    meta = new TSMeta((String) null);
    metaClient.create(meta);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmpty() throws Exception {
    meta = new TSMeta("");
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
  public void counterExistsInStorage() throws Exception {
    assertTrue(metaClient.TSMetaCounterExists(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }

  @Test
  public void counterExistsInStorageNot() throws Exception {
    tsdb_store.flushRow(new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1});
    assertFalse(metaClient.TSMetaCounterExists(
            new byte[]{0, 0, 1, 0, 0, 1, 0, 0, 1}).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT));
  }
}
