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

import java.util.List;

import net.opentsdb.storage.MockBase;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
  Config.class, RowKey.class, Const.class })
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
  public void addRowSalted() {
    PowerMockito.mockStatic(Const.class);
    PowerMockito.when(Const.SALT_WIDTH()).thenReturn(1);
    PowerMockito.when(Const.SALT_BUCKETS()).thenReturn(2);
    
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);
    
    final byte[] hour1 = { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x27, 
        0, 0, 0, 1, 0, 0, 2 };
    final byte[] hour2 = { 1, 0, 0, 1, 0x50, (byte)0xE2, 0x35, 
        0x10, 0, 0, 1, 0, 0, 2 };
    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(hour1, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    span.addRow(new KeyValue(hour2, FAMILY, qual12, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
    
    assertEquals(4, span.size());
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
  public void downsampler() throws Exception {
    final byte[] val40 = Bytes.fromLong(40L);
    final byte[] val50 = Bytes.fromLong(50L);
    // For a value at the offset 0 seconds from a base timestamp.
    final byte[] qual0 = { 0x00, 0x07 };
    // For a value at the offset 5 seconds from a base timestamp.
    final byte[] qual5 = { 0x00, 0x57 };
    // For a value at the offset 2000 (0x7D0) seconds from a base timestamp.
    final byte[] qual2000 = { 0x7D, 0x07 };
    // For values at the offsets 0 and 2000 seconds from a base timestamp.
    final byte[] qual02000 = MockBase.concatByteArrays(qual0, qual2000);
    // For values at the offsets 0 and 5 seconds from a base timestamp.
    final byte[] qual05 = MockBase.concatByteArrays(qual0, qual5);

    final Span span = new Span(tsdb);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual02000,
        MockBase.concatByteArrays(val40, val50, ZERO)));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual05,
        MockBase.concatByteArrays(val40, val50, ZERO)));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual02000,
        MockBase.concatByteArrays(val40, val50, ZERO)));

    assertEquals(6, span.size());
    long interval_ms = 1000000;
    Aggregator downsampler = Aggregators.get("avg");
    final SeekableView it = span.downsampler(1356998000L, 1357007000L, 
        interval_ms, downsampler, FillPolicy.NONE);
    List<Double> values = Lists.newArrayList();
    List<Long> timestamps_in_millis = Lists.newArrayList();
    while (it.hasNext()) {
      DataPoint dp = it.next();
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamps_in_millis.add(dp.timestamp());
    }

    assertEquals(5, values.size());
    assertEquals(40, values.get(0).longValue());
    assertEquals(1356998000000L, timestamps_in_millis.get(0).longValue());
    assertEquals(50, values.get(1).longValue());
    assertEquals(1357000000000L, timestamps_in_millis.get(1).longValue());
    assertEquals(45, values.get(2).longValue());
    assertEquals(1357002000000L, timestamps_in_millis.get(2).longValue());
    assertEquals(40, values.get(3).longValue());
    assertEquals(1357005000000L, timestamps_in_millis.get(3).longValue());
    assertEquals(50, values.get(4).longValue());
    assertEquals(1357007000000L, timestamps_in_millis.get(4).longValue());
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
