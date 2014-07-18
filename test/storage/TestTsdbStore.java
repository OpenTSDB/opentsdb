package net.opentsdb.storage;

import net.opentsdb.uid.UniqueId;
import org.junit.Test;

import java.io.IOException;

public abstract class TestTsdbStore {
  protected TsdbStore tsdb_store;

  @Test
  public void testGetMetaNullCell() throws IOException {
    tsdb_store.getMeta(new byte[] {0, 0, 1}, "derp", UniqueId.UniqueIdType.TAGK);
  }
}
