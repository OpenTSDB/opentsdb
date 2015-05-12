package net.opentsdb.storage;

import net.opentsdb.meta.LabelMeta;
import net.opentsdb.uid.UniqueIdType;

import com.typesafe.config.Config;
import org.hbase.async.GetRequest;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;
import org.junit.Test;

import java.io.IOException;

import static net.opentsdb.uid.UniqueIdType.METRIC;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;

public abstract class TestTsdbStore {
  protected static final boolean SAME_TSUID = true;
  protected static final boolean NOT_SAME_TSUID = false;
  protected TsdbStore tsdb_store;
  protected LabelMeta meta;
  protected Config config;

  protected static boolean STORE_DATA = true;

  private static long UIDMETA_CREATED = 100;

  /*META TESTS*/

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, UniqueIdType.TAGK);
  }

  /* COUNTER TESTS */

  @Test
  public void deleteTimeseriesCounter() {
    //tsdb_store.deleteTimeseriesCounter(TSMeta ts);
    fail();
  }

  @Test
  public void setTSMetaCounter() {
    //tsdb_store.setTSMetaCounter(byte[] tsuid, long number);
    fail();
  }

  protected byte[] emptyArray() {
    return eq(HBaseClient.EMPTY_ARRAY);
  }
  protected PutRequest anyPut() {
    return any(PutRequest.class);
  }
  protected GetRequest anyGet() {
    return any(GetRequest.class);
  }

  protected byte[] anyBytes() {
    return any(byte[].class);
  }

  @Test
  public void storeNew() throws Exception {
    meta = LabelMeta.create(new byte[]{0, 0, 1}, METRIC, "sys.cpu.1", "System CPU", UIDMETA_CREATED);
    tsdb_store.add(meta).joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);

    final LabelMeta newMeta = tsdb_store.getMeta(new byte[] { 0, 0, 1 }, METRIC)
            .joinUninterruptibly(MockBase.DEFAULT_TIMEOUT);
    assertEquals("System CPU", newMeta.description());
  }
}
