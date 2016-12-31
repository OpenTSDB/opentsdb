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
package net.opentsdb.core;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.Arrays;
import java.util.NoSuchElementException;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Bytes.ByteMap;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
               "ch.qos.*", "org.slf4j.*",
               "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
  Config.class, RowKey.class, Const.class })
public final class TestRowSeq {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] KEY = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] SALTED_KEY = 
    { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.metrics.width()).thenReturn((short)3);
    when(RowKey.metricNameAsync(tsdb, KEY))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
  }
  
  @Test
  public void setRow() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    assertEquals(2, rs.size());
  }
  
  @Test
  public void setRowSalted() throws Exception {
    setupSalt();
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    assertEquals(2, rs.size());
  }
  
  @Test (expected = IllegalStateException.class)
  public void setRowAlreadySet() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    assertEquals(2, rs.size());
    rs.setRow(kv);
  }
  
  @Test
  public void addRowMergeLater() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeLaterSalted() throws Exception {
    setupSalt();
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    final byte[] salted_key2 = Arrays.copyOf(SALTED_KEY, SALTED_KEY.length);
    salted_key2[0] = 1;
    rs.addRow(makekv(salted_key2, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeEarlier() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x37 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x00, 0x47 };
    final byte[] val2 = Bytes.fromLong(7L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    final byte[] qual4 = { 0x00, 0x27 };
    final byte[] val4 = Bytes.fromLong(5L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeEarlierSalted() throws Exception {
    setupSalt();
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x37 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x00, 0x47 };
    final byte[] val2 = Bytes.fromLong(7L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    final byte[] qual4 = { 0x00, 0x27 };
    final byte[] val4 = Bytes.fromLong(5L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    final byte[] salted_key2 = Arrays.copyOf(SALTED_KEY, SALTED_KEY.length);
    salted_key2[0] = 1;
    rs.addRow(makekv(salted_key2, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeMiddle() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x57 };
    final byte[] val3 = Bytes.fromLong(8L);
    final byte[] qual4 = { 0x00, 0x67 };
    final byte[] val4 = Bytes.fromLong(9L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    assertEquals(4, rs.size());
    
    final byte[] qual5 = { 0x00, 0x37 };
    final byte[] val5 = Bytes.fromLong(6L);
    final byte[] qual6 = { 0x00, 0x47 };
    final byte[] val6 = Bytes.fromLong(7L);
    final byte[] qual56 = MockBase.concatByteArrays(qual5, qual6);
    rs.addRow(makekv(KEY, qual56, MockBase.concatByteArrays(val5, val6, ZERO)));
    
    assertEquals(6, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
    assertEquals(1356998405000L, rs.timestamp(4));
    assertEquals(8, rs.longValue(4));
    assertEquals(1356998406000L, rs.timestamp(5));
    assertEquals(9, rs.longValue(5));
  }
  
  @Test
  public void addRowMergeMiddleSalted() throws Exception {
    setupSalt();
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x57 };
    final byte[] val3 = Bytes.fromLong(8L);
    final byte[] qual4 = { 0x00, 0x67 };
    final byte[] val4 = Bytes.fromLong(9L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(SALTED_KEY, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
    assertEquals(4, rs.size());
    
    final byte[] qual5 = { 0x00, 0x37 };
    final byte[] val5 = Bytes.fromLong(6L);
    final byte[] qual6 = { 0x00, 0x47 };
    final byte[] val6 = Bytes.fromLong(7L);
    final byte[] qual56 = MockBase.concatByteArrays(qual5, qual6);
    final byte[] salted_key2 = Arrays.copyOf(SALTED_KEY, SALTED_KEY.length);
    salted_key2[0] = 1;
    rs.addRow(makekv(salted_key2, qual56, 
        MockBase.concatByteArrays(val5, val6, ZERO)));
    
    assertEquals(6, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
    assertEquals(1356998405000L, rs.timestamp(4));
    assertEquals(8, rs.longValue(4));
    assertEquals(1356998406000L, rs.timestamp(5));
    assertEquals(9, rs.longValue(5));
  }
  
  @Test
  public void addRowMergeDuplicateLater() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, val3, ZERO)));
    assertEquals(3, rs.size());
    
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeDuplicateEarlier() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual4 = { 0x00, 0x27 };
    final byte[] val4 = Bytes.fromLong(5L);
    final byte[] qual1 = { 0x00, 0x37 };
    final byte[] val1 = Bytes.fromLong(6L);
    final byte[] qual2 = { 0x00, 0x47 };
    final byte[] val2 = Bytes.fromLong(7L);
    final byte[] qual12 = MockBase.concatByteArrays(qual4, qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val4, val1, val2, ZERO)));
    assertEquals(3, rs.size());
    
    final byte[] qual3 = { 0x00, 0x07 };
    final byte[] val3 = Bytes.fromLong(4L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998402000L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998404000L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowDiffBaseTime() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    final byte[] row2 = { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
    rs.addRow(new KeyValue(row2, FAMILY, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
  }
  
  @Test (expected = IllegalDataException.class)
  public void addRowDiffBaseTimeSalt() throws Exception {
    setupSalt();
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { 0x00, 0x47 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    final byte[] row2 = { 1, 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 
        0, 0, 1, 0, 0, 2 };
    rs.addRow(new KeyValue(row2, FAMILY, qual34, 
        MockBase.concatByteArrays(val3, val4, ZERO)));
  }
  
  @Test
  public void addRowMergeMs() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x07, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { (byte) 0xF0, 0x00, 0x09, 0x07 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, ZERO)));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998400008L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998400028L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998400036L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test
  public void addRowMergeSecAndMs() throws Exception {
    // this happens if the same row key is used for the addRow call
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(makekv(KEY, qual12, MockBase.concatByteArrays(val1, val2, 
        new byte[] { 1 })));
    assertEquals(2, rs.size());
    
    final byte[] qual3 = { 0x00, 0x37 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual4 = { (byte) 0xF0, 0x01, 0x09, 0x07 };
    final byte[] val4 = Bytes.fromLong(7L);
    final byte[] qual34 = MockBase.concatByteArrays(qual3, qual4);
    rs.addRow(makekv(KEY, qual34, MockBase.concatByteArrays(val3, val4, 
        new byte[] { 1 })));
    
    assertEquals(4, rs.size());
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(4, rs.longValue(0));
    assertEquals(1356998400008L, rs.timestamp(1));
    assertEquals(5, rs.longValue(1));
    assertEquals(1356998403000L, rs.timestamp(2));
    assertEquals(6, rs.longValue(2));
    assertEquals(1356998401060L, rs.timestamp(3));
    assertEquals(7, rs.longValue(3));
  }
  
  @Test (expected = IllegalStateException.class)
  public void addRowNotSet() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.addRow(kv);
  }
  
  @Test
  public void timestamp() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(1356998400000L, rs.timestamp(0));
    assertEquals(1356998402000L, rs.timestamp(1));
  }
  
  @Test
  public void timestampSalted() throws Exception {
    setupSalt();
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(SALTED_KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);

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
    final KeyValue kv = makekv(KEY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);

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
    final KeyValue kv = makekv(KEY, qualifier, values);
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);

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
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998400008L);
    DataPoint dp = it.next();
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsSalted() throws Exception {
    setupSalt();
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(true));

    final SeekableView it = rs.iterator();
    it.seek(1356998400008L);
    DataPoint dp = it.next();
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsStart() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998400000L);
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsBetween() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998400005L);
    DataPoint dp = it.next();
    assertEquals(1356998400008L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test
  public void seekMsEnd() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998400016L);
    DataPoint dp = it.next();
    assertEquals(1356998400016L, dp.timestamp());
    assertEquals(6, dp.longValue());
    
    assertFalse(it.hasNext());
  }
  
  @Test
  public void seekMsTooEarly() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998300000L);
    DataPoint dp = it.next();
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    assertTrue(it.hasNext());
  }
  
  @Test (expected = NoSuchElementException.class)
  public void seekMsPastLastDp() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(getMs(false));

    final SeekableView it = rs.iterator();
    it.seek(1356998400032L);
    it.next();
  }
  
  @Test
  public void metricUID() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, rs.metricUID());
  }
  
  @Test
  public void metricUIDSalted() throws Exception {
    setupSalt();
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertArrayEquals(new byte[] { 0, 0, 1 }, rs.metricUID());
  }
  
  @Test (expected = NullPointerException.class)
  public void metricUIDKeyNotSet() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.metricUID();
  }
  
  @Test
  public void getTagUids() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    final ByteMap<byte[]> uids = rs.getTagUids();
    assertEquals(1, uids.size());
    assertArrayEquals(new byte[] { 0, 0, 1 }, uids.firstKey());
    assertArrayEquals(new byte[] { 0, 0, 2 }, 
        uids.firstEntry().getValue());
  }
  
  @Test
  public void getTagUidsSalted() throws Exception {
    setupSalt();
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    final ByteMap<byte[]> uids = rs.getTagUids();
    assertEquals(1, uids.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, 
        uids.firstEntry().getValue()));
  }
  
  @Test (expected = NullPointerException.class)
  public void getTagUidsNotSet() throws Exception {
    final RowSeq rs = new RowSeq(tsdb);
    rs.getTagUids();
  }
  
  @Test
  public void getAggregatedTagUids() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    final KeyValue kv = makekv(qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    final RowSeq rs = new RowSeq(tsdb);
    rs.setRow(kv);
    
    assertEquals(0, rs.getAggregatedTagUids().size());
  }
  
  /** Shorthand to create a {@link KeyValue}.  */
  public static KeyValue makekv(final byte[] qualifier, final byte[] value) {
    if (Const.SALT_WIDTH() > 0) {
      return new KeyValue(SALTED_KEY, FAMILY, qualifier, value);
    }
    return new KeyValue(KEY, FAMILY, qualifier, value);
  }
  
  /** Shorthand to create a {@link KeyValue}.  */
  public static KeyValue makekv(final byte[] key, final byte[] qualifier, 
      final byte[] value) {
    return new KeyValue(key, FAMILY, qualifier, value);
  }
  
  /** Helper that builds a KeyValue with millisecond timestamps */
  private static KeyValue getMs(final boolean salted) {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual3 = { (byte) 0xF0, 0x00, 0x04, 0x07 };
    final byte[] val3 = Bytes.fromLong(6L);
    final byte[] qual123 = MockBase.concatByteArrays(qual1, qual2, qual3);
    final KeyValue kv = makekv((salted ? SALTED_KEY : KEY), 
        qual123, MockBase.concatByteArrays(val1, val2, val3, ZERO));
    return kv;
  }

  /** Helper to mockout the salt configuration */
  private void setupSalt() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
  }
}
