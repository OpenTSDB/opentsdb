package net.opentsdb.storage;

import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.UniqueId;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

import static net.opentsdb.uid.UniqueId.UniqueIdType.METRIC;

@Ignore
public abstract class TestTsdbStore {
  protected TsdbStore tsdb_store;
  protected UIDMeta meta;

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[]{0, 0, 1}, "derp", UniqueId.UniqueIdType.TAGK);
  }
}
