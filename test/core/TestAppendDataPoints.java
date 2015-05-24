// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.Collection;
import java.util.Iterator;

import net.opentsdb.core.Internal.Cell;
import net.opentsdb.storage.MockBase;

import org.hbase.async.HBaseClient;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

public class TestAppendDataPoints extends BaseTsdbTest {

  private static final byte[] CF = "t".getBytes();
  private static final byte[] DPQ_S = new byte[] { 0, 0x20 };
  private static final byte[] DPQ_MS = new byte[] { (byte) 0xF0, 0, 0x20, 0 };
  private static final byte[] DPV = new byte[] { 42 };
  private static final byte[] DPV2 = new byte[] { 24 };
  private static final byte[] ROW_KEY = new byte[] {
    0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 0, 0, 1, 0, 0, 1 };
  
  @Test
  public void ctorForWrites() throws Exception {
    assertNotNull(new AppendDataPoints(DPQ_S, DPV));    
    assertNotNull(new AppendDataPoints(DPQ_MS, DPV));
    
    try {
      assertNotNull(new AppendDataPoints(DPQ_S, null));
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { } 
    
    try {
      assertNotNull(new AppendDataPoints(null, DPV));
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { } 
    
    try {
      assertNotNull(new AppendDataPoints(null, null));
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { } 
  }
  
  @Test
  public void toByteArray() throws Exception {
    AppendDataPoints adp = new AppendDataPoints(DPQ_S, DPV);
    assertArrayEquals(MockBase.concatByteArrays(DPQ_S, DPV), adp.getBytes());

    adp = new AppendDataPoints(DPQ_MS, DPV);
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPV), adp.getBytes());
    
    adp = new AppendDataPoints(MockBase.concatByteArrays(DPQ_MS, DPV), 
        MockBase.concatByteArrays(DPQ_S, DPV2));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2), 
        adp.getBytes());
  }

  @Test
  public void parseKeyValue() throws Exception {
    KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_S, DPV));
    AppendDataPoints adp = new AppendDataPoints();
    Collection<Cell> cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(1, cells.size());
    Cell cell = cells.iterator().next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(DPQ_S, adp.qualifier());
    assertArrayEquals(DPV, adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, DPV));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(1, cells.size());
    cell = cells.iterator().next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    assertArrayEquals(DPQ_MS, adp.qualifier());
    assertArrayEquals(DPV, adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    // some odd offset
    kv = new KeyValue(ROW_KEY, CF, 
        new byte[] { AppendDataPoints.APPEND_COLUMN_PREFIX, 42, 42 },
        MockBase.concatByteArrays(DPQ_MS, DPV));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(1, cells.size());
    cell = cells.iterator().next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    assertArrayEquals(DPQ_MS, adp.qualifier());
    assertArrayEquals(DPV, adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    Iterator<Cell> iterator = cells.iterator();
    cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    // out of order
    kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_S, DPV2, DPQ_MS, DPV));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    iterator = cells.iterator();
    cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    // duplicates
    kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2, DPQ_S, DPV2));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    iterator = cells.iterator();
    cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, never()).put(any(PutRequest.class));
    
    // duplicates AND out of order, just a mess
    kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(
            DPQ_S, DPV2, DPQ_MS, DPV, DPQ_MS, DPV, DPQ_S, DPV2, DPQ_MS, DPV));
    adp = new AppendDataPoints();
    cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    iterator = cells.iterator();
    cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, never()).put(any(PutRequest.class));
  }
  
  @Test
  public void parseKeyValueNotAppends() throws Exception {
    // regular data points
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, DPQ_S, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, DPQ_MS, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    // different object
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, new byte[] { 1, 0, 0 }, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    // bad coder!
    try {
      new AppendDataPoints().parseKeyValue(tsdb, null);
      fail("Expected an NullPointerException");
    } catch (NullPointerException iae) { }
  }
  
  @Test
  public void parseKeyValueCorrupt() throws Exception {
    // shouldn't happen, but who knows?
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, null, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an NullPointerException");
    } catch (NullPointerException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, HBaseClient.EMPTY_ARRAY, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, DPQ_S, null);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an NullPointerException");
    } catch (NullPointerException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, DPQ_S, HBaseClient.EMPTY_ARRAY);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException iae) { }
    
    try {
      KeyValue kv = new KeyValue(null, CF, DPQ_S, DPV);
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an NullPointerException");
    } catch (NullPointerException iae) { }
    
    try {
      KeyValue kv = new KeyValue(METRIC_BYTES, CF, 
          AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
          MockBase.concatByteArrays(DPQ_S, DPV2));
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalDataException");
    } catch (IllegalDataException iae) { }
    
    // bad values
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, /*DPV, oops*/ DPQ_S, DPV2));
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalDataException");
    } catch (IllegalDataException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, DPV2, new byte[] { 0, 0, 0, 0, }));
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalDataException");
    } catch (IllegalDataException iae) { }
    
    try {
      KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPV2, DPQ_MS));
      new AppendDataPoints().parseKeyValue(tsdb, kv);
      fail("Expected an IllegalDataException");
    } catch (IllegalDataException iae) { }
  }
  
  @Test
  public void repairDuplicates() throws Exception {
    setDataPointStorage();
    Whitebox.setInternalState(config, "repair_appends", true);
    final KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2, DPQ_S, DPV2));
    final AppendDataPoints adp = new AppendDataPoints();
    final Collection<Cell>cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    final Iterator<Cell> iterator = cells.iterator();
    Cell cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, times(1)).put(any(PutRequest.class));

    adp.repairedDeferred().join();
    assertArrayEquals(
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2), 
        storage.getColumn(ROW_KEY, AppendDataPoints.APPEND_COLUMN_QUALIFIER));
  }
  
  @Test
  public void repairOutOfOrder() throws Exception {
    setDataPointStorage();
    Whitebox.setInternalState(config, "repair_appends", true);
    final KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(DPQ_S, DPV2, DPQ_MS, DPV));
    final AppendDataPoints adp = new AppendDataPoints();
    final Collection<Cell>cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    final Iterator<Cell> iterator = cells.iterator();
    Cell cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, times(1)).put(any(PutRequest.class));

    adp.repairedDeferred().join();
    assertArrayEquals(
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2), 
        storage.getColumn(ROW_KEY, AppendDataPoints.APPEND_COLUMN_QUALIFIER));
  }
  
  @Test
  public void repairOutOfOrderAndDuplicates() throws Exception {
    setDataPointStorage();
    Whitebox.setInternalState(config, "repair_appends", true);
    final KeyValue kv = new KeyValue(ROW_KEY, CF, 
        AppendDataPoints.APPEND_COLUMN_QUALIFIER, 
        MockBase.concatByteArrays(
            DPQ_S, DPV2, DPQ_MS, DPV, DPQ_MS, DPV, DPQ_S, DPV2, DPQ_MS, DPV));
    final AppendDataPoints adp = new AppendDataPoints();
    final Collection<Cell>cells = adp.parseKeyValue(tsdb, kv);
    assertEquals(2, cells.size());
    final Iterator<Cell> iterator = cells.iterator();
    Cell cell = iterator.next();
    assertArrayEquals(DPV, cell.value);
    assertEquals(1356998400128L, cell.timestamp(1356998400));
    cell = iterator.next();
    assertArrayEquals(DPV2, cell.value);
    assertEquals(1356998402000L, cell.timestamp(1356998400));
    assertArrayEquals(MockBase.concatByteArrays(DPQ_MS, DPQ_S), adp.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(DPV, DPV2), adp.value());
    verify(client, times(1)).put(any(PutRequest.class));

    adp.repairedDeferred().join();
    assertArrayEquals(
        MockBase.concatByteArrays(DPQ_MS, DPV, DPQ_S, DPV2), 
        storage.getColumn(ROW_KEY, AppendDataPoints.APPEND_COLUMN_QUALIFIER));
  }
}
