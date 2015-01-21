// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import java.util.NoSuchElementException;

import com.google.common.collect.Lists;
import net.opentsdb.core.DataPoint;
import net.opentsdb.core.Internal;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;

import net.opentsdb.uid.UniqueIdType;
import static org.junit.Assert.*;

public final class TestCompactedRow {
  private TSDB tsdb;
  private static final byte[] KEY = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  
  @Before
  public void before() throws Exception {
    final MemoryStore tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, new Config(false), null, null);

    tsdb_store.allocateUID("sys.cpu.user", new byte[]{0, 0, 1}, UniqueIdType.METRIC);
  }
  
  @Test
  public void setRow() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    assertEquals(2, rs.size());
  }

  
  @Test (expected = IllegalStateException.class)
  public void addRowNotSet() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    final CompactedRow rs = new CompactedRow(makekv(qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
  }
  
  @Test
  public void timestamp() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12,
        MockBase.concatByteArrays(val1, val2, ZERO));

      final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
  }
  
  @Test
  public void timestampNormalizeMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());

    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMixedNormalized() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test
  public void timestampMixedNonNormalized() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
  }
  
  @Test (expected = IndexOutOfBoundsException.class)
  public void timestampOutofBounds() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998400008L, rs.timestamp(1));
    rs.timestamp(2);
  }
  
  @Test
  public void iterateNormalizedMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());

    assertEquals(2, rs.size());
    
    final SeekableView it = rs.iterator();
    DataPoint dp = it.next();
    
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
 
    dp = it.next();    
    assertEquals(1356998402000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void iterateMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    DataPoint dp = it.next();
    
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
 
    dp = it.next();    
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertFalse(it.hasNext());
  }

  @Test
  public void iterateMsLarge() throws Exception {
    long ts = 1356998400500L;
    // mimicks having 64K data points in a row
    final int limit = 64000;
    final byte[] qualifier = new byte[4 * limit];
    for (int i = 0; i < limit; i++) {
      System.arraycopy(Internal.buildQualifier(ts, (short) 7), 0,
          qualifier, i * 4, 4);
      ts += 50;
    }
    final byte[] values = new byte[(4 * limit) + 1];
    final KeyValue kv = makekv(qualifier, values);

    final CompactedRow rs = new CompactedRow(kv, Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    ts = 1356998400500L;
    while (it.hasNext()) {
      assertEquals(ts, it.next().timestamp());
      ts += 50;
    }
    assertFalse(it.hasNext());
  }
  
  @Test
  public void seekMs() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998400008L);
    DataPoint dp = it.next();
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsStart() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998400000L);
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsBetween() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998400005L);
    DataPoint dp = it.next();
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsEnd() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998400016L);
    DataPoint dp = it.next();
    assertEquals(1356998400016L, dp.timestamp());
    assertEquals(6, dp.longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void seekMsTooEarly() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998300000L);
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void seekMsPastLastDp() throws Exception {
      final CompactedRow rs = new CompactedRow(getMs(), Lists.<Annotation>newArrayList());

    final SeekableView it = rs.iterator();
    it.seek(1356998400032L);
    it.next();
  }
  
  /** Shorthand to create a {@link KeyValue}.  */
  private static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    return new KeyValue(KEY, FAMILY, qualifier, value);
  }
  
  private static KeyValue getMs() {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final KeyValue kv = makekv(qual123, 
        MockBase.concatByteArrays(val1, val2, val3, ZERO));
    return kv;
  }
}
