package net.opentsdb.storage.hbase;

import org.junit.Test;

import static net.opentsdb.core.StringCoder.toBytes;
import static org.junit.Assert.*;

public class HBaseConstTest {
  @Test
  public void BRANCH_QUALIFIER() {
    assertArrayEquals(toBytes("branch"), HBaseConst.BRANCH_QUALIFIER);
  }
}