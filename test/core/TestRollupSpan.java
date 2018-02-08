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

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.rollup.RollupInterval;
import net.opentsdb.rollup.RollupQuery;
import net.opentsdb.rollup.RollupSpan;
import static net.opentsdb.rollup.RollupUtils.ROLLUP_QUAL_DELIM;

public class TestRollupSpan extends BaseTsdbTest {
  protected byte[] hour1 = null;
  protected byte[] hour2 = null;
  protected byte[] hour3 = null;
  protected RollupConfig rollup_config;
  protected static final Aggregator aggr_sum = Aggregators.SUM;
  
  protected static final RollupQuery rollup_query = 
    new RollupQuery(RollupInterval.builder()
        .setTable("tsdb")
        .setPreAggregationTable("tsdb-agg")
        .setInterval("1s")
        .setRowSpan("1h")
        .build(), 
    aggr_sum, 
    1000,
    aggr_sum);
  
  @Before
  public void beforeLocal() throws Exception {
    hour1 = getRowKey(METRIC_STRING, 1356998400, TAGK_STRING, TAGV_STRING);
    hour2 = getRowKey(METRIC_STRING, 1357002000, TAGK_STRING, TAGV_STRING);
    hour3 = getRowKey(METRIC_STRING, 1357005600, TAGK_STRING, TAGV_STRING);
    
    rollup_config = RollupConfig.builder()
        .addAggregationId("sum", 0)
        .addAggregationId("count", 1)
        .addAggregationId("max", 2)
        .addAggregationId("min", 3)
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-10m")
            .setPreAggregationTable("tsdb-rollup-agg-10m")
            .setInterval("10m")
            .setRowSpan("6h"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1h")
            .setPreAggregationTable("tsdb-rollup-agg-1h")
            .setInterval("1h")
            .setRowSpan("1d"))
        .addInterval(RollupInterval.builder()
            .setTable("tsdb-rollup-1d")
            .setPreAggregationTable("tsdb-rollup-agg-1d")
            .setInterval("1d")
            .setRowSpan("1n"))
        .build();
    Whitebox.setInternalState(tsdb, "rollup_config", rollup_config);
  }
  
  @Test
  public void addRow() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual1, val1));
    
    assertEquals(1, span.size());
  }
  
  @Test (expected = NullPointerException.class)
  public void addRowNull() {
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(null);
  }
 /*
  * TODO - fix up these tests 
  @Test (expected = IllegalArgumentException.class)
  public void addRowBadKeyLength() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(8L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qual1, val1));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x43, 0x20, 0, 0, 0, 1 };
    span.addRow(new KeyValue(bad_key, TSDB.FAMILY(), qual2, 
        MockBase.concatByteArrays(val1, val2, ZERO)));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedMetric() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(8L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qual1,val1));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 0, 2, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 0, 1, 0, 0, 0, 2 };
    span.addRow(new KeyValue(bad_key, TSDB.FAMILY(), qual2,val2));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagk() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(8L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qual1, val1));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 0, 2, 0, 0, 0, 2 };
    span.addRow(new KeyValue(bad_key, TSDB.FAMILY(), qual2, val2));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagv() {
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val2 = Bytes.fromLong(8L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qual1, val1));
    
    final byte[] bad_key = 
      new byte[] { 0, 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 0, 1, 0, 0, 0, 3 };
    span.addRow(new KeyValue(bad_key, TSDB.FAMILY(), qual2, val2));
  }
  
  @Test
  public void addRowOutOfOrder() {
    //2nd hour
    final byte[] qual1 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    //1st hour
    final byte[] qual2 = {0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(HOUR2, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qual2, val2));
    assertEquals(2, span.size());
    
    assertEquals(1356998402000L, span.timestamp(0));
    assertEquals(5, span.longValue(0));
    assertEquals(1357002000000L, span.timestamp(1));
    assertEquals(4, span.longValue(1));
  }

  @Test
  public void addDifferentSalt() throws Exception {
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 });
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 });
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 });
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 });
    values.add(Bytes.fromLong(4L));
    values.add(Bytes.fromLong(5L));
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    final byte[] hour1 = Arrays.copyOf(HOUR1, HOUR1.length);
    hour1[0] = 2;
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(2), values.get(0)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(3), values.get(1)));
    assertEquals(4, span.size());
    assertEquals(1356998400000L, span.timestamp(0));
    assertEquals(4, span.longValue(0));
    assertEquals(1356998402000L, span.timestamp(1));
    assertEquals(5, span.longValue(1));
    assertEquals(1356998403000L, span.timestamp(2));
    assertEquals(4, span.longValue(2));
    assertEquals(1356998404000L, span.timestamp(3));
    assertEquals(5, span.longValue(3));
  }

  @Test
  public void addDifferentSaltDiffHour() throws Exception {
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();

    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    final byte[] hour2 = Arrays.copyOf(HOUR2, HOUR2.length);
    hour2[0] = 2;
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
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
  public void addDifferentSaltDiffHourOO() throws Exception {
    when(tsdb.followAppendRowLogic()).thenReturn(true);
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();


    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    final byte[] hour2 = Arrays.copyOf(HOUR2, HOUR2.length);
    hour2[0] = 2;
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));

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
  
  @Test (expected = IllegalArgumentException.class)
  public void addDifferentKey() throws Exception {
    when(tsdb.followAppendRowLogic()).thenReturn(true);
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
  
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 });
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 });
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    final byte[] hour1 = Arrays.copyOf(HOUR1, HOUR1.length);
    hour1[hour1.length - 1] = 3;
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(2), values.get(0)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(3), values.get(1)));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addDifferentSaltAndKey() throws Exception {
    when(tsdb.followAppendRowLogic()).thenReturn(true);
    List<byte[]> qualifiers = new ArrayList<byte[]>();
    List<byte[]> values = new ArrayList<byte[]>();
  
  
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 });
    values.add(Bytes.fromLong(4L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 });
    values.add(Bytes.fromLong(5L));
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x37 });
    qualifiers.add(new byte[]{ 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x47 });
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    final byte[] hour1 = Arrays.copyOf(HOUR1, HOUR1.length);
    hour1[0] = 2;
    hour1[hour1.length - 1] = 3;
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(0), values.get(0)));
    span.addRow(new KeyValue(HOUR1, TSDB.FAMILY(), qualifiers.get(1), values.get(1)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(2), values.get(0)));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers.get(3), values.get(1)));
  }
  */
  @Test
  public void timestampNormalized() throws Exception {
    final byte[] qual1 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x73, 0x75, 0x6D, 0x3A, 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final Span span = new RollupSpan(tsdb, rollup_query);
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual2, val2));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual2, val2));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual2, val2));
    
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
      span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qualifiers, Bytes.fromLong(i)));
      span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qualifiers, Bytes.fromLong(i)));
      span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qualifiers, Bytes.fromLong(i)));
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
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual2, val2));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual2, val2));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual1, val1));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual2, val2));
    
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
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual1,val1));
    span.addRow(new KeyValue(hour1, TSDB.FAMILY(), qual2,val2));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual1,val1));
    span.addRow(new KeyValue(hour2, TSDB.FAMILY(), qual2,val2));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual1,val1));
    span.addRow(new KeyValue(hour3, TSDB.FAMILY(), qual2,val2));

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
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final KeyValue kv = new KeyValue(hour1, TSDB.FAMILY(), qual2, val2);
    
    assertEquals(1356998402L, Span.lastTimestampInRow((short) 3, kv));
  }
  
  @Test
  public void lastTimestampInRowMs() throws Exception {
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    
    final KeyValue kv = new KeyValue(hour1, TSDB.FAMILY(), qual2, val2);
    
    assertEquals(1356998400008L, Span.lastTimestampInRow((short) 3, kv));
  }
}
