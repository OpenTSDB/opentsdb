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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;
import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.stumbleupon.async.Deferred;
import java.util.ArrayList;
import java.util.List;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
Config.class, RowKey.class })
public final class TestSpan {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] HOUR1 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR2 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR3 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x43, 0x20, 0, 0, 1, 0, 0, 2 };
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
    when(RowKey.metricNameAsync(tsdb, HOUR1))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
  }
  
  @Test
  public void addRow() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    assertEquals(2, span.size());
  }
  
  @Test
  public void appendRow() {
    when(tsdb.followAppendRowLogic()).thenReturn(true);
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    qualifiers.add(new byte[]{ 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    
    final Span span = new Span(tsdb);
    span.addRow(TestRowSeq.appendkv(HOUR1, qualifiers, values));
    
    assertEquals(2, span.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void addRowNull() {
    final Span span = new Span(tsdb);
    span.addRow(null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowBadKeyLength() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 1, 0x50, (byte)0xE2, 0x43, 0x20, 0, 0, 1 };
    span.addRow(new KeyValue(bad_key, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedMetric() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 2, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
    span.addRow(new KeyValue(bad_key, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagk() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 2, 0, 0, 2 };
    span.addRow(new KeyValue(bad_key, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagv() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 3 };
    span.addRow(new KeyValue(bad_key, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
  }
  
  @Test
  public void addRowOutOfOrder() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR2, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    assertEquals(4, span.size());
    
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(4, span.longValue(0));
    assertEquals(1356998402000L, span.timestamp(1));
    assertEquals(5, span.longValue(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(4, span.longValue(2));
    assertEquals(1357002002000L, span.timestamp(3));
    assertEquals(5, span.longValue(3));
  }

    @Test
  public void appendRowOutOfOrder() {
    when(tsdb.followAppendRowLogic()).thenReturn(true);
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    qualifiers.add(new byte[]{ 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    
    final Span span = new Span(tsdb);
    span.addRow(TestRowSeq.appendkv(HOUR2, qualifiers, values));
    span.addRow(TestRowSeq.appendkv(HOUR1, qualifiers, values));
    assertEquals(4, span.size());
    
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(4, span.longValue(0));
    assertEquals(1356998402000L, span.timestamp(1));
    assertEquals(5, span.longValue(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(4, span.longValue(2));
    assertEquals(1357002002000L, span.timestamp(3));
    assertEquals(5, span.longValue(3));
  }
    
  @Test
  public void timestampNormalized() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    assertEquals(6, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1356998402000L, span.timestamp(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(1357002002000L, span.timestamp(3));
    assertEquals(1357005600000L, span.timestamp(4));
    assertEquals(1357005602000L, span.timestamp(5));
  }
  
  @Test
  public void timestampFullSeconds() throws Exception {
    
    final byte[] qualifiers = new byte[3600 * 2];
    final byte[] values = new byte[3600 * 8];
    for (int i = 0; i < 3600; i++) {
      final short qualifier = (short) (i << Const.FLAG_BITS | 0x07);
      System.arraycopy(Bytes.fromShort(qualifier), 0, qualifiers, i * 2, 2);
      System.arraycopy(Bytes.fromLong(i), 0, values, i * 8, 8);
    }
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qualifiers, values));
    span.addRow(new KeyValue(HOUR2, FAMILY, qualifiers, values));
    span.addRow(new KeyValue(HOUR3, FAMILY, qualifiers, values));
    
    assertEquals(3600 * 3, span.size());
  }
  
  @Test
  public void timestampMS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    assertEquals(6, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1356998400008L, span.timestamp(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(1357002000008L, span.timestamp(3));
    assertEquals(1357005600000L, span.timestamp(4));
    assertEquals(1357005600008L, span.timestamp(5));
  }
  
  @Test
  public void iterateNormalizedMS() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));

    assertEquals(6, span.size());
    final SeekableView it = span.iterator();
    DataPoint dp = it.next();
    
    assertEquals(1356998400000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    dp = it.next();
    assertEquals(1356998402000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    dp = it.next(); 
    assertEquals(1357002000000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    dp = it.next();
    assertEquals(1357002002000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    dp = it.next();
    assertEquals(1357005600000L, dp.timestamp());
    assertEquals(4, dp.longValue());
    
    dp = it.next();
    assertEquals(1357005602000L, dp.timestamp());
    assertEquals(5, dp.longValue());
    
    assertFalse(it.hasNext());
 
    
  }

  @Test
  public void lastTimestampInRow() throws Exception {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final KeyValue kv = new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    assertEquals(1356998402L, Span.lastTimestampInRow((short) 3, kv));
  }
  
  @Test
  public void lastTimestampInRowMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final KeyValue kv = new KeyValue(HOUR1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO));
    
    assertEquals(1356998400008L, Span.lastTimestampInRow((short) 3, kv));
  }
}
