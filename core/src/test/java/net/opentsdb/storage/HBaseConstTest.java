package net.opentsdb.storage;

import org.junit.Test;

import static net.opentsdb.core.StringCoder.fromBytes;
import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class HBaseConstTest {
  @Test
  public void META_QUALIFIER() throws Exception {
    assertArrayEquals(toBytes("ts_meta"), HBaseConst.TSMeta.META_QUALIFIER);
  }

  @Test
  public void COUNTER_QUALIFIER() throws Exception {
    assertArrayEquals(toBytes("ts_ctr"), HBaseConst.TSMeta.COUNTER_QUALIFIER);
  }

  @Test
  public void LEAF_PREFIX() throws Exception {
    assertEquals("leaf:", fromBytes(HBaseConst.Leaf.LEAF_PREFIX));
  }
}