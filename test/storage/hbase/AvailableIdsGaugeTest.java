package net.opentsdb.storage.hbase;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AvailableIdsGaugeTest {
  @Test
  public void testMaxPossibleId() {
    assertEquals(16777215L, AvailableIdsGauge.maxPossibleId(3));
  }
}