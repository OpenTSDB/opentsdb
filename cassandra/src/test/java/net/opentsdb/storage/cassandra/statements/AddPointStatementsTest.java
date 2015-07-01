package net.opentsdb.storage.cassandra.statements;

import static org.junit.Assert.assertEquals;

import net.opentsdb.storage.cassandra.statements.AddPointStatements.AddPointStatementMarkers;

import org.junit.Test;

public class AddPointStatementsTest {
  @Test
  public void testEnumMarkerIdOrdinal0() throws Exception {
    assertEquals(0, AddPointStatementMarkers.ID.ordinal());
  }

  @Test
  public void testEnumMarkerBaseTimeOrdinal1() throws Exception {
    assertEquals(1, AddPointStatementMarkers.BASE_TIME.ordinal());
  }

  @Test
  public void testEnumMarkerTimestampOrdinal2() throws Exception {
    assertEquals(2, AddPointStatementMarkers.TIMESTAMP.ordinal());
  }

  @Test
  public void testEnumMarkerValueOrdinal3() throws Exception {
    assertEquals(3, AddPointStatementMarkers.VALUE.ordinal());
  }

  @Test
  public void testEnumMarkerUsingTimestampOrdinal4() throws Exception {
    assertEquals(4, AddPointStatementMarkers.USING_TIMESTAMP.ordinal());
  }
}