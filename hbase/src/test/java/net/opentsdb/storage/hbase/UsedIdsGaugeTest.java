package net.opentsdb.storage.hbase;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueIdType;
import com.typesafe.config.Config;
import org.hbase.async.HBaseClient;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class UsedIdsGaugeTest {
  Config config;
  HBaseClient client;
  HBaseStore store;

  private byte[] table;

  @Test
  public void getUsedIDs() throws Exception {
    store.allocateUID("olga1", UniqueIdType.METRIC);
    store.allocateUID("olga2", UniqueIdType.METRIC);
    store.allocateUID("bogdan1", UniqueIdType.TAGK);
    store.allocateUID("romanov1", UniqueIdType.TAGV);

    final Map<UniqueIdType, Long> ids = UsedIdsGauge.getUsedIDs(client, table)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertNotNull(ids);
    assertEquals(3, ids.size());
    assertEquals(2, ids.get(UniqueIdType.METRIC).longValue());
    assertEquals(1, ids.get(UniqueIdType.TAGK).longValue());
    assertEquals(1, ids.get(UniqueIdType.TAGV).longValue());
  }
}