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
import static org.mockito.Mockito.mock;

import java.util.List;

import com.google.common.collect.ImmutableSortedSet;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.hbase.CompactedRow;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public final class TestSpan {
  private TSDB tsdb = mock(TSDB.class);
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
    tsdb = new TSDB(new MemoryStore(), new Config(false));
  }

  
  @Test (expected = IllegalArgumentException.class)
  public void createEmptyRow() {
    new Span(ImmutableSortedSet.<DataPoints>naturalOrder().build());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowBadKeyLength() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final byte[] bad_key = new byte[]{0, 0, 1, 0x50, (byte) 0xE2, 0x43, 0x20, 0, 0, 1};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(badRow)
            .build());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedMetric() {
    final byte[] qual1 = {0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x00, 0x27};
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final byte[] bad_key =
            new byte[]{0, 0, 2, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(badRow)
            .build());
  }

  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagk() {
    final byte[] qual1 = {0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x00, 0x27};
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final byte[] bad_key = new byte[]{0, 0, 1, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 2, 0, 0, 2};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(badRow)
            .build());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addRowMissMatchedTagv() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final byte[] bad_key = new byte[]{0, 0, 1, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 3};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());


  }
  
  @Test
  public void addRowOutOfOrder() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row2)
            .add(row1)
            .build());
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

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(row2)
            .add(row3)
            .build());
    
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

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qualifiers, values), Lists.<Annotation>newArrayList());
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qualifiers, values), Lists.<Annotation>newArrayList());
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qualifiers, values), Lists.<Annotation>newArrayList());

    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(row2)
            .add(row3)
            .build());

    assertEquals(3600 * 3, span.size());
  }
  
  @Test
  public void timestampMS() throws Exception {
    final byte[] qual1 = { (byte) 0xF0, 0x00, 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { (byte) 0xF0, 0x00, 0x02, 0x07 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(row2)
            .add(row3)
            .build());

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

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), Lists.<Annotation>newArrayList());

    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(row2)
            .add(row3)
            .build());

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

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual02000,
            MockBase.concatByteArrays(val40, val50, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual05,
            MockBase.concatByteArrays(val40, val50, ZERO)), Lists.<Annotation>newArrayList());
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual02000,
            MockBase.concatByteArrays(val40, val50, ZERO)), Lists.<Annotation>newArrayList());

    final Span span = new Span(ImmutableSortedSet.<DataPoints>naturalOrder()
            .add(row1)
            .add(row2)
            .add(row3)
            .build());

    assertEquals(6, span.size());
    long interval_ms = 1000000;
    Aggregator downsampler = Aggregators.get("avg");
    final SeekableView it = span.downsampler(interval_ms, downsampler);
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
}
