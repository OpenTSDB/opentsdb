// This file is part of OpenTSDB.
// Copyright (C) 2011-2012  The OpenTSDB Authors.
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
package net.opentsdb.storage.hbase;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Map;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Const;
import net.opentsdb.core.IllegalDataException;

import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.json.StorageModule;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.DeleteRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.PutRequest;
import org.junit.Before;
import org.junit.Test;

import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.powermock.api.mockito.PowerMockito;

public final class TestCompactionQueue {
  private HBaseStore tsdb_store;
  private Config config;

  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = { 0, 0, 1, 78, 36, -84, 42, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  private static final byte[] MIXED_FLAG = { Const.MS_MIXED_COMPACT };
  private static final String annotation = 
      "{\"tsuid\":\"ABCD\",\"description\":\"Description\"," + 
      "\"notes\":\"Notes\",\"custom\":null,\"endTime\":1328140801,\"startTime" + 
      "\":1328140800}";
  private static final byte[] note = annotation.getBytes(Charset.forName("UTF-8"));
  private static final byte[] note_qual = { 1, 0, 0 };
  private CompactionQueue compactionq;
  private ObjectMapper jsonMapper;

  @Before
  public void before() throws Exception {
    tsdb_store = mock(HBaseStore.class);

    Map<String,String> overrides = Maps.newHashMap();
    overrides.put("tsd.storage.fix_duplicates", "TRUE");
    config = new Config(false, overrides);

    // Stub out the compaction thread, so it doesn't even start.
    PowerMockito.whenNew(CompactionQueue.Thrd.class).withNoArguments()
      .thenReturn(mock(CompactionQueue.Thrd.class));

    jsonMapper = new ObjectMapper();
    jsonMapper.registerModule(new StorageModule());

    compactionq = new CompactionQueue(tsdb_store, jsonMapper, config, TABLE, FAMILY);

    when(tsdb_store.put(any(PutRequest.class))).thenAnswer(newDeferred());
    when(tsdb_store.delete(any(DeleteRequest.class))).thenAnswer(newDeferred());
  }

  @Test
  public void emptyRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(0);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertNull(kv);
    
    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }

  @Test
  public void oneCellRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual = { 0x00, 0x07 };
    final byte[] val = Bytes.fromLong(42L);
    kvs.add(makekv(qual, val));
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual, kv.qualifier());
    assertArrayEquals(val, kv.value());
    
    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void oneCellRowWAnnotation() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(note_qual, note));
    final byte[] qual = { 0x00, 0x07 };
    final byte[] val = Bytes.fromLong(42L);
    kvs.add(makekv(qual, val));
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual, kv.qualifier());
    assertArrayEquals(val, kv.value());
    assertEquals(1, annotations.size());
    
    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void oneCellRowWAnnotationMS() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(new byte[] { 1, 0, 0, 0, 0 }, note));
    final byte[] qual = { 0x00, 0x07 };
    final byte[] val = Bytes.fromLong(42L);
    kvs.add(makekv(qual, val));
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual, kv.qualifier());
    assertArrayEquals(val, kv.value());
    assertEquals(1, annotations.size());
    
    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }

  @Test
  public void oneCellRowBadLength() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual = { 0x00, 0x03 };
    final byte[] cqual = { 0x00, 0x07 };
    byte[] val = Bytes.fromLong(42L);
    kvs.add(makekv(qual, val));
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(cqual, kv.qualifier());
    assertArrayEquals(val, kv.value());

    // The old one needed the length fixed up, so verify that we wrote the new one
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // ... and deleted the old one
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void oneCellRowMS() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    byte[] val = Bytes.fromLong(42L);
    kvs.add(makekv(qual, val));
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual, kv.qualifier());
    assertArrayEquals(val, kv.value());
    
    // We had nothing to do so...
    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }

  @Test
  public void twoCellRow() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, ZERO), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void twoCellRowWAnnotation() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(note_qual, note));
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, ZERO), kv.value());
    assertEquals(1, annotations.size());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void fullRowSeconds() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3600);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);

    byte[] qualifiers = new byte[] {};
    byte[] values = new byte[] {};

    for (int i = 0; i < 3600; i++) {
      final short qualifier = (short) (i << Const.FLAG_BITS | 0x07);
      kvs.add(makekv(Bytes.fromShort(qualifier), Bytes.fromLong(i)));
      qualifiers = MockBase.concatByteArrays(qualifiers,
              Bytes.fromShort(qualifier));
      values = MockBase.concatByteArrays(values, Bytes.fromLong(i));
    }

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qualifiers), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(values, ZERO), kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void bigRowMs() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3599999);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);

    byte[] qualifiers = new byte[] {};
    byte[] values = new byte[] {};
    for (int i = 0; i < 3599999; i++) {
      final int qualifier = (((i << Const.MS_FLAG_BITS ) | 0x07) | 0xF0000000);
      kvs.add(makekv(Bytes.fromInt(qualifier), Bytes.fromLong(i)));
      qualifiers = MockBase.concatByteArrays(qualifiers, 
          Bytes.fromInt(qualifier));
      values = MockBase.concatByteArrays(values, Bytes.fromLong(i));
      i += 100;
    }
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qualifiers), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(values, ZERO), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void twoCellRowMS() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, ZERO), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void sortMsAndS() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(5L);
    kvs.add(makekv(qual3, val3));
    
    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, 
        new byte[] { 1 }), kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondsOutOfOrder() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x02, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));
    final byte[] qual3 = { 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual2, qual3, qual1), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val2, val3, val1, ZERO), 
        kv.value());

    // We compacted all columns to one, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void msOutOfOrder() throws Exception {
    // all rows with an ms qualifier will go through the compaction 
    // process and they'll be sorted
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual2, qual3, qual1), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val2, val3, val1, ZERO), 
        kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondAndMs() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 1 }), 
        kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondAndMsWAnnotation() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(note_qual, note));
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, new byte[] { 1 }), 
        kv.value());
    assertEquals(1, annotations.size());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test (expected = IllegalDataException.class)
  public void msSameAsSecond() throws Exception {
    tsdb_store = mock(HBaseStore.class);
    config = new Config(false);

    compactionq = new CompactionQueue(tsdb_store, jsonMapper, config, TABLE, FAMILY);

    when(tsdb_store.put(any(PutRequest.class))).thenAnswer(newDeferred());
    when(tsdb_store.delete(any(DeleteRequest.class))).thenAnswer(newDeferred());

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs, annotations);
  }

  @Test
  public void msSameAsSecondFix() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual2, kv.qualifier());
    assertArrayEquals(val2, kv.value());
    
    // no compacted row
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // And we had to delete the earlier entry.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void fixQualifierFlags() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // Note: here the flags pretend the value is on 4 bytes, but it's actually
    // on 8 bytes, so we expect the code to fix the flags as it's compacting.
    final byte[] qual1 = { 0x00, 0x03 };   // Pretends 4 bytes...
    final byte[] val1 = Bytes.fromLong(4L);    // ... 8 bytes actually.
    final byte[] cqual1 = { 0x00, 0x07 };  // Should have been this.
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(cqual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val2, ZERO), kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void fixFloatingPoint() throws Exception {
    // Check that the compaction process is fixing incorrectly encoded
    // floating point values.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // Note: here the flags pretend the value is on 4 bytes, but it's actually
    // on 8 bytes, so we expect the code to fix the flags as it's compacting.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x1B };  // +1s, float, 4 bytes.
    final byte[] val2 = Bytes.fromLong(Float.floatToRawIntBits(4.2F));
    final byte[] cval2 = Bytes.fromInt(Float.floatToRawIntBits(4.2F));
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual2), kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, cval2, ZERO), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete individual cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test (expected = IllegalDataException.class)
  public void overlappingDataPoints() throws Exception {
    tsdb_store = mock(HBaseStore.class);
    config = new Config(false);

    compactionq = new CompactionQueue(tsdb_store, jsonMapper, config, TABLE, FAMILY);

    when(tsdb_store.put(any(PutRequest.class))).thenAnswer(newDeferred());
    when(tsdb_store.delete(any(DeleteRequest.class))).thenAnswer(newDeferred());

    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    // Same data point (same time delta, 0), but encoded on 4 bytes, not 8.
    final byte[] qual2 = { 0x00, 0x03 };
    final byte[] val2 = Bytes.fromInt(4);
    kvs.add(makekv(qual2, val2));

    compactionq.compact(kvs, annotations);
  }
  
  @Test
  public void overlappingDataPointsFix() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    // Same data point (same time delta, 0), but encoded on 4 bytes, not 8.
    final byte[] qual2 = { 0x00, 0x03 };
    final byte[] val2 = Bytes.fromInt(4);
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual2, kv.qualifier());
    assertArrayEquals(val2, kv.value());

    // We didn't have anything to write.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // We had to delete the first entry as it was older.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void failedCompactNoop() throws Exception {
    // In this case, the row contains both the compacted form as well as the
    // non-compacted form.  This could happen if the TSD dies in between the
    // `put' of a compaction, before getting a change to do the deletes.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(3);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    final byte[] qual2 = { 0x00, 0x17 };
    final byte[] val2 = Bytes.fromLong(5L);
    kvs.add(makekv(qual2, val2));
    final byte[] qualcompact = MockBase.concatByteArrays(qual1, qual2);
    final byte[] valcompact = MockBase.concatByteArrays(val1, val2, ZERO);
    kvs.add(makekv(qualcompact, valcompact));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qualcompact, kv.qualifier());
    assertArrayEquals(valcompact, kv.value());
    
    // We didn't have anything to write.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // We had to delete stuff in 1 row.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void annotationOnly() throws Exception {
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(1);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(note_qual, note));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertNull(kv);
    assertEquals(1, annotations.size());

    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void annotationsOnly() throws Exception {
    // Two annotations in a row only (the value of the second isn't parsed at
    // this time)
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(2);
    kvs.add(makekv(note_qual, note));
    kvs.add(makekv(new byte[] { 1, 0, 1 }, note));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertNull(kv);
    assertEquals(2, annotations.size());

    // ... verify there were no put.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // ... verify there were no delete.
    verify(tsdb_store, never()).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondCompact() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, ZERO), 
        kv.value());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondCompactWAnnotation() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(1);
    kvs.add(makekv(note_qual, note));
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, ZERO), 
        kv.value());
    assertEquals(1, annotations.size());

    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void secondCompactMS() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, ZERO), 
        kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondCompactMixedSecond() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x0A, 0x41, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, 
        new byte[] { 1 })));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { 0x00, 0x57 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, 
        new byte[] { 1 }), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondCompactMixedMS() throws Exception {
    // In this test the row has already been compacted, and another data
    // point was written in the mean time.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x0A, 0x41, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, 
        new byte[] { 1 })));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x01, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, 
        new byte[] { 1 }), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void secondCompactMixedMSAndS() throws Exception {
    // In this test the row has already been compacted with a ms flag as the
    // first qualifier. Then a second qualifier is added to the row, ordering
    // it BEFORE the compacted row
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { (byte) 0xF0, 0x0A, 0x41, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, (byte) 0xF7 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, 
        new byte[] { 1 })));
    // This data point came late.  Note that its time delta falls in between
    // that of the two data points above.
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual3, qual1, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val3, val1, val2, 
        new byte[] { 1 }), kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell + pre-existing compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test (expected = IllegalDataException.class)
  public void secondCompactOverwrite() throws Exception {
    tsdb_store = mock(HBaseStore.class);
    config = new Config(false);

    compactionq = new CompactionQueue(tsdb_store, jsonMapper, config, TABLE, FAMILY);

    when(tsdb_store.put(any(PutRequest.class))).thenAnswer(newDeferred());
    when(tsdb_store.delete(any(DeleteRequest.class))).thenAnswer(newDeferred());

    // In this test the row has already been compacted, and a new value for an
    // old data point was written in the mean time
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta matches the first point with
    // a different value, so it should replace the earlier one.
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    compactionq.compact(kvs, annotations);
  }
  
  @Test
  public void secondCompactOverwriteFix() throws Exception {
    // In this test the row has already been compacted, and a new value for an
    // old data point was written in the mean time
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(2);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // This is 2 values already compacted together.
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.  Note that its time delta matches the first point with
    // a different value, so it should replace the earlier one.
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    kvs.add(makekv(qual3, val3));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val3, val2, new byte[] { 0 }), 
        kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the individual cell, but we overwrite the pre-existing compacted cell
    // rather than delete it.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void doubleFailedCompactNoop() throws Exception {
    // In this test the row has already been compacted once, but we didn't
    // clean up the individual data points.  Then another data point was
    // written, another compaction ran, but once again didn't delete the
    // individual data points.  So the rows contains 2 compacted cells and
    // several individual cells.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    // Data points 1 + 2 compacted.
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    // This data point came late.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    // Data points 1 + 3 + 2 compacted.
    final byte[] qual132 = MockBase.concatByteArrays(qual1, qual3, qual2);
    kvs.add(makekv(qual1, val1));
    kvs.add(makekv(qual132, MockBase.concatByteArrays(val1, val3, val2, ZERO)));
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    kvs.add(makekv(qual3, val3));
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(qual132, kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, ZERO), 
        kv.value());
    
    // We didn't have anything to write, the last cell is already the correct
    // compacted version of the row.
    verify(tsdb_store, never()).put(any(PutRequest.class));
    // And we had to delete the 3 individual cells + the first pre-existing
    // compacted cell.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void weirdOverlappingCompactedCells() throws Exception {
    // Here we have two partially compacted cell, but they contain different
    // data points.  Although a possible scenario, this is extremely unlikely,
    // but we need to test that logic works in this case too.
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    kvs.add(makekv(qual1, val1));
    // Data points 1 + 2 compacted.
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    // This data point came late.
    final byte[] qual3 = { 0x00, 0x17 };
    final byte[] val3 = Bytes.fromLong(6L);
    // Data points 1 + 3 compacted.
    final byte[] qual13 = MockBase.concatByteArrays(qual1, qual3);
    kvs.add(makekv(qual13, MockBase.concatByteArrays(val1, val3, ZERO)));
    kvs.add(makekv(qual3, val3));
    kvs.add(makekv(qual2, val2));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(MockBase.concatByteArrays(qual1, qual3, qual2), 
        kv.qualifier());
    assertArrayEquals(MockBase.concatByteArrays(val1, val3, val2, ZERO), 
        kv.value());
    
    // We had one row to compact, so one put to do.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the 3 individual cells + 2 pre-existing
    // compacted cells.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }

  @Test
  public void tripleCompacted() throws Exception {
    // Here we have a row with #kvs > scanner.maxNumKeyValues and the result
    // that was compacted during a query. The result is a bunch of compacted
    // columns. We want to make sure that we can merge them nicely
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(8L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(9L);
    final byte[] qual56 = MockBase.concatByteArrays(qual5, qual6);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    kvs.add(makekv(qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    kvs.add(makekv(qual56, MockBase.concatByteArrays(val5, val6, ZERO)));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(
        MockBase.concatByteArrays(qual12, qual34, qual56), kv.qualifier());
    assertArrayEquals(
        MockBase.concatByteArrays(val1, val2, val3, val4, val5, val6, ZERO), 
        kv.value());

    // We wrote only the combined column.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the 3 partially compacted columns.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void tripleCompactedOutOfOrder() throws Exception {
    // Here we have a row with #kvs > scanner.maxNumKeyValues and the result
    // that was compacted during a query. The result is a bunch of compacted
    // columns. We want to make sure that we can merge them nicely
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    // 3rd compaction
    final byte[] qual5 = { 0x00, 0x57 };
    final byte[] val5 = Bytes.fromLong(8L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(9L);
    final byte[] qual56 = MockBase.concatByteArrays(qual5, qual6);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    kvs.add(makekv(qual56, MockBase.concatByteArrays(val5, val6, ZERO)));
    kvs.add(makekv(qual34, MockBase.concatByteArrays(val3, val4, ZERO)));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(
        MockBase.concatByteArrays(qual12, qual34, qual56), kv.qualifier());
    assertArrayEquals(
        MockBase.concatByteArrays(val1, val2, val3, val4, val5, val6, ZERO), 
        kv.value());    
    
    // We wrote only the combined column.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the 3 partially compacted columns.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  @Test
  public void tripleCompactedSecondsAndMs() throws Exception {
    // Here we have a row with #kvs > scanner.maxNumKeyValues and the result
    // that was compacted during a query. The result is a bunch of compacted
    // columns. We want to make sure that we can merge them nicely
    ArrayList<KeyValue> kvs = new ArrayList<KeyValue>(5);
    ArrayList<Annotation> annotations = new ArrayList<Annotation>(0);
    // start one off w ms
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    // 2nd compaction
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { (byte) 0xF0, 0x04, 0x65, 0x07 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    // 3rd compaction
    final byte[] qual5 = { (byte) 0xF0, 0x05, 0x5F, 0x07 };
    final byte[] val5 = Bytes.fromLong(8L);
    final byte[] qual6 = { 0x00, 0x67 };
    final byte[] val6 = Bytes.fromLong(9L);
    final byte[] qual56 = MockBase.concatByteArrays(qual5, qual6);
    kvs.add(makekv(qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    kvs.add(makekv(qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    kvs.add(makekv(qual56, MockBase.concatByteArrays(val5, val6, ZERO)));

    final KeyValue kv = compactionq.compact(kvs, annotations);
    assertArrayEquals(
        MockBase.concatByteArrays(qual12, qual34, qual56), kv.qualifier());
    // TODO(jat): metadata byte should be 0x01?
    assertArrayEquals(
        MockBase.concatByteArrays(val1, val2, val3, val4, val5, val6, MIXED_FLAG),
        kv.value());

    // We wrote only the combined column.
    verify(tsdb_store, times(1)).put(any(PutRequest.class));
    // And we had to delete the 3 partially compacted columns.
    verify(tsdb_store, times(1)).delete(any(DeleteRequest.class));
  }
  
  // ----------------- //
  // Helper functions. //
  // ----------------- //

  // fake timestamp is derived from the sequence number of new makekv calls
  private static long kvCount = 0;

  /** Shorthand to create a {@link KeyValue}.  */
  private static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    return new KeyValue(KEY, FAMILY, qualifier, kvCount++, value);
  }

  /** Creates a new Deferred that's already called back.  */
  private static <T> Answer<Deferred<T>> newDeferred() {
    return new Answer<Deferred<T>>() {
      @Override
      public Deferred<T> answer(final InvocationOnMock invocation) {
        return Deferred.fromResult(null);
      }
    };
  }

}
