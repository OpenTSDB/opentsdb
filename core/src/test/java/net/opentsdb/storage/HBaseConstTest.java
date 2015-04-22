package net.opentsdb.storage;

import org.junit.Test;

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.assertArrayEquals;

public class HBaseConstTest {
  @Test
  public void META_QUALIFIER() throws Exception {
    assertArrayEquals(toBytes("ts_meta"), HBaseConst.TSMeta.META_QUALIFIER);
  }

  @Test
  public void COUNTER_QUALIFIER() throws Exception {
    assertArrayEquals(toBytes("ts_ctr"), HBaseConst.TSMeta.COUNTER_QUALIFIER);
  }
}