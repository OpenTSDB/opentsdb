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

import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import net.opentsdb.meta.Annotation;
import net.opentsdb.storage.MockBase;
import net.opentsdb.storage.hbase.CompactedRow;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

public final class TestSpan {
  private static final byte[] HOUR1 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR2 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2 };
  private static final byte[] HOUR3 = 
    { 0, 0, 1, 0x50, (byte)0xE2, 0x43, 0x20, 0, 0, 1, 0, 0, 2 };
  private static final byte[] FAMILY = { 't' };
  private static final byte[] ZERO = { 0 };

  private static final List<Annotation> EMPTY_ANNOTATIONS = ImmutableList.of();

  @Before
  public void before() throws Exception {
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorEmptyDataPoints() {
    new Span(ImmutableSortedSet.<DataPoints>of());
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMissMatchedMetric() {
    final byte[] qual1 = {0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x00, 0x27};
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final byte[] bad_key =
            new byte[]{0, 0, 2, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 2};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    new Span(ImmutableSortedSet.<DataPoints>of(row1, badRow));
  }

  @Test (expected = IllegalArgumentException.class)
  public void ctorMissMatchedTagk() {
    final byte[] qual1 = {0x00, 0x07};
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = {0x00, 0x27};
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final byte[] bad_key = new byte[]{0, 0, 1, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 2, 0, 0, 2};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    new Span(ImmutableSortedSet.<DataPoints>of(row1, badRow));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorMissMatchedTagv() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final byte[] bad_key = new byte[]{0, 0, 1, 0x50, (byte) 0xE2, 0x35, 0x10, 0, 0, 1, 0, 0, 3};
    CompactedRow badRow = new CompactedRow(new KeyValue(bad_key, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    new Span(ImmutableSortedSet.<DataPoints>of(row1, badRow));
  }

  @Test
  public void datapointForIdxZero() {
    final byte[] qual = { 0x00, 0x07 };
    final byte[] val = Bytes.fromLong(4L);

    CompactedRow row = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual,
            MockBase.concatByteArrays(val, ZERO)), EMPTY_ANNOTATIONS);

    final Span span = new Span(ImmutableSortedSet.<DataPoints>of(row));

    assertEquals(1356998400000L, span.dataPointForIndex(0).timestamp());
  }

  @Test
  public void datapointForLastIdx() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final Span span = new Span(ImmutableSortedSet.<DataPoints>of(row1, row2, row3));

    assertEquals(6, span.size());
    assertEquals(1357005602000L, span.dataPointForIndex(5).timestamp());
  }

  @Test
  public void size() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final Span span = new Span(ImmutableSortedSet.<DataPoints>of(row1, row2, row3));

    assertEquals(6, span.size());
  }
  
  @Test
  public void iterateNormalizedMS() {
    final byte[] qual1 = { 0x00, 0x07 };
    final byte[] val1 = Bytes.fromLong(4L);
    final byte[] qual2 = { 0x00, 0x27 };
    final byte[] val2 = Bytes.fromLong(5L);
    final byte[] qual12 = MockBase.concatByteArrays(qual1, qual2);

    CompactedRow row1 = new CompactedRow(new KeyValue(HOUR1, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual12,
            MockBase.concatByteArrays(val1, val2, ZERO)), EMPTY_ANNOTATIONS);

    final Span span = new Span(ImmutableSortedSet.<DataPoints>of(row1, row2, row3));

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
  public void downsampler() {
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
            MockBase.concatByteArrays(val40, val50, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row2 = new CompactedRow(new KeyValue(HOUR2, FAMILY, qual05,
            MockBase.concatByteArrays(val40, val50, ZERO)), EMPTY_ANNOTATIONS);
    CompactedRow row3 = new CompactedRow(new KeyValue(HOUR3, FAMILY, qual02000,
            MockBase.concatByteArrays(val40, val50, ZERO)), EMPTY_ANNOTATIONS);

    final Span span = new Span(ImmutableSortedSet.<DataPoints>of(row1, row2, row3));

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
