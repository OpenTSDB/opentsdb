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

import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupSpan;
import static net.opentsdb.rollup.RollupUtils.ROLLUP_QUAL_DELIM;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ RowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
Config.class, RowKey.class })
public final class TestRollupSpan {
  private TSDB tsdb = mock(TSDB.class);
  private Config config = mock(Config.class);
  private UniqueId metrics = mock(UniqueId.class);
  private static final byte[] TABLE = { 't', 'a', 'b', 'l', 'e' };
  private static final byte[] HOUR1 = new byte[]
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR2 = new byte[]
    { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR3 = new byte[]
    { 0, 0, 1, 0x50, (byte)0xE2, 0x43, 0x20, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };
  private static final Aggregator aggr_sum = Aggregators.SUM;
  
  private static final RollupQuery rollup_query = 
    new RollupQuery(new RollupInterval("tsdb", "tsdb-agg", "1s", "1h"), 
    aggr_sum, 1000);
  
  @Before
  public void before() throws Exception {
    // Inject the attributes we need into the "tsdb" object.
    Whitebox.setInternalState(tsdb, "metrics", metrics);
    Whitebox.setInternalState(tsdb, "table", TABLE);
    Whitebox.setInternalState(tsdb, "config", config);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.metrics.width()).thenReturn((short)4);
    when(RowKey.metricNameAsync(tsdb, HOUR1))
      .thenReturn(Deferred.fromResult("sys.cpu.user"));
  }
  
  @Test
  public void addRow() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual1, val1));
    
    assertEquals(1, span.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void addRowNull() {
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(null);
  }
  
  @Test
  public void timestampNormalized() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR1, FAMILY, qual2, val2));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual2, val2));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual2, val2));
    
    assertEquals(6, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1356998402000L, span.timestamp(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(1357002002000L, span.timestamp(3));
    assertEquals(1357005600000L, span.timestamp(4));
    assertEquals(1357005602000L, span.timestamp(5));
  }
  
//  @Test
  public void timestampFullSeconds() throws Exception {
    //6 = num of bytes ("sum) + num of bytes (":") + num of bytes in actual qual
    final byte[] agg = (aggr_sum.toString() + ROLLUP_QUAL_DELIM).
            getBytes(Const.ASCII_CHARSET);
    final byte[] qualifiers = new byte[agg.length + 2];
    System.arraycopy(agg, 0, qualifiers, 0, agg.length);
    final Span span = new RollupSpan(tsdb, rollup_query);

    for (int i = 0; i < 100; i++) {
      final short qualifier = (short) (i << Const.FLAG_BITS | 0x07);
      System.arraycopy(Bytes.fromShort(qualifier), 0, qualifiers, agg.length, 2);
      span.addRow(new KeyValue(HOUR1, FAMILY, qualifiers, Bytes.fromLong(i)));
      span.addRow(new KeyValue(HOUR2, FAMILY, qualifiers, Bytes.fromLong(i)));
      span.addRow(new KeyValue(HOUR3, FAMILY, qualifiers, Bytes.fromLong(i)));
    }
    
    
    assertEquals(3600 * 3, span.size());
  }
  
  @Test
  public void timestampMS() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR1, FAMILY, qual2, val2));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual2, val2));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual1, val1));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual2, val2));
    
    assertEquals(6, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(1356998400000L, span.timestamp(1));
    assertEquals(1357002000000L, span.timestamp(2));
    assertEquals(1357002000000L, span.timestamp(3));
    assertEquals(1357005600000L, span.timestamp(4));
    assertEquals(1357005600000L, span.timestamp(5));
  }
  
  @Test
  public void iterateNormalizedMS() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, FAMILY, qual1,val1));
    span.addRow(new KeyValue(HOUR1, FAMILY, qual2,val2));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual1,val1));
    span.addRow(new KeyValue(HOUR2, FAMILY, qual2,val2));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual1,val1));
    span.addRow(new KeyValue(HOUR3, FAMILY, qual2,val2));

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
    
    final KeyValue kv = new KeyValue(HOUR1, FAMILY, qual2, val2);
    
    assertEquals(1356998402L, Span.lastTimestampInRow((short) 3, kv));
  }
  
  @Test
  public void lastTimestampInRowMs() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final KeyValue kv = new KeyValue(HOUR1, FAMILY, qual2, val2);
    
    assertEquals(1356998400008L, Span.lastTimestampInRow((short) 3, kv));
  }
}
