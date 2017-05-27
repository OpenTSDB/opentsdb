// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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

import static org.junit.Assert.*;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
//"Classloader hell"... It's real. Tell PowerMock to ignore these classes
//because they fiddle with the class loader. We don't test them anyway.
@PowerMockIgnore({ "javax.management.*", "javax.xml.*", "ch.qos.*", "org.slf4j.*", "com.sum.*", "org.xml.*" })
@PrepareForTest({ HistogramSpan.class, HistogramRowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, Config.class,
 RowKey.class })

public class TestHistogramAggregationIterator {
  private TSDB tsdb = mock(TSDB.class);
  private static final long BASE_TIME = 1356998400000L;
  public static final byte[] KEY = 
      new byte[] { 0, 0, 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 0, 1, 0, 0, 0, 2 };
  
  @Test
  public void testOneHistogramSpanWithNoDownsampler() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);

    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps.add(hdp.timestamp());
    } // end while

    assertEquals(10, values.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps.get(i).longValue());
    } // end for
  } // end testOneHistogramSpanWithNoDownsampler()
  
  @Test
  public void testOneHistogramSpanWithDownsampler_10secs() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("10s-sum");
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, specification, 0, 0, false);
   
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(5, values.size());
    // 0 + 1
    assertEquals(1, values.get(0).longValue());
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    
    // 2 + 3
    assertEquals(5, values.get(1).longValue());
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    
    // 4 + 5
    assertEquals(9, values.get(2).longValue());
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
    
    // 6 + 7
    assertEquals(13, values.get(3).longValue());
    assertEquals(BASE_TIME + 30000L, timestamps_in_millis.get(3).longValue());
    
    // 8 + 9
    assertEquals(17, values.get(4).longValue());
    assertEquals(BASE_TIME + 40000L, timestamps_in_millis.get(4).longValue());
  } // end testOneHistogramSpanWithDownsampler_10secs()
  
  @Test
  public void testOneHistogramSpanNoDownSamplerSkipEarlyDataPoints() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME + 5000L,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
   
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(9, values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(i + 1, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * (i + 1), timestamps_in_millis.get(i).longValue());
    }
  } // end testOneHistogramSpanNoDownSamplerSkipEarlyDataPoints()
  
  @Test
  public void testOneHistogramSpanNoDownSamplerOutofRange() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME + 5000L * 10,
        BASE_TIME + 5000L * 20, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    assertFalse(histAggIt.hasNext());
  } // end testOneHistogramSpanNoDownSamplerOutofRange()
  
  @Test
  public void testOneHistogramSpanNoDownSamplerLaterDataPoints() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 5, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
   
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(6, values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
  } // end testOneHistogramSpanNoDownSamplerLaterDataPoints()
  
  @Test
  public void testOneHistogramSpanDownSamplerLaterDataPoints() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("10s-sum");
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 5, HistogramAggregation.SUM, specification, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(3, values.size());
    // 0 + 1
    assertEquals(1, values.get(0).longValue());
    assertEquals(BASE_TIME + 00000L, timestamps_in_millis.get(0).longValue());
    
    // 2 + 3
    assertEquals(5, values.get(1).longValue());
    assertEquals(BASE_TIME + 10000L, timestamps_in_millis.get(1).longValue());
    
    // 4 + 5
    assertEquals(9, values.get(2).longValue());
    assertEquals(BASE_TIME + 20000L, timestamps_in_millis.get(2).longValue());
  } // end testOneHistogramSpanWithLaterDataPoints()
  
  @Test
  public void testTwoHistogramSpanNoDownSamplerSameTimestamp() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(10, values.size());
    for (int i = 0; i < 10; ++i) {
      assertEquals(i * 2, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
  } // end testTwoHistogramSpanNoDownSamplerSameTimestamp()
  
  @Test
  public void testTwoHistogramSpanDownSamplerSameTimestamp() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("10s-sum");
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, specification, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(5, values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(2 * (i * 2 + i * 2 + 1), values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * 2 * i, timestamps_in_millis.get(i).longValue());
    }
  } // end testTwoHistogramSpanDownSamplerSameTimestamp()
  
  @Test
  public void testTwoHistogramSpanNoDownSamplerDiffTimestamp() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    // 0, 2, 4...
    for (int i = 0; i < 10; ) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
      i += 2;
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    // 1, 3, 5...
    for (int i = 1; i < 10; ) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
      i += 2;
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
   
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(10, values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
  } // end testTwoHistogramSpanNoDownSamplerDiffTimestamp()
  
  @Test
  public void testTwoHistogramSpanNoDownSamplerMergeSome() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    for (int i = 1; i < 5; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    for (int i = 5; i < 10; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L *  (5 + i), Bytes.fromLong(5 + i)));
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
    
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 20, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(15, values.size());
    for (int i = 0; i < 5; ++i) {
      assertEquals(2 * i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
    
    for (int i = 5; i < 10; ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
    
    for (int i = 10; i < 15; ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    }
  } // end testTwoHistogramSpanNoDownSamplerOverlap()
  
  @Test
  public void testTwoHistogramSpanNoDownSamplerOneHasMore() {
    // span 1 has 10 data points
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    // span 2 has 5 data points
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    for (int i = 1; i < 5; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME,
        BASE_TIME + 5000L * 20, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(10, values.size());
    for (int i = 0; i < 5; ++i) {
      assertEquals(2 * i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    } // end for
    
    for (int i = 5; i < 10; ++i) {
      assertEquals(i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * i, timestamps_in_millis.get(i).longValue());
    } // end for
  } // end testTwoHistogramSpanNoDownSamplerOneHasMore()
  
  @Test
  public void testTwoHistogramSpanNoDownSamplerOneOutofRange() {
    // span1 has 10 data points
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);
    
    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);
    
    // span 2 has 5 data points
    List<HistogramDataPoint> row2 = new ArrayList<HistogramDataPoint>();
    for (int i = 1; i < 5; ++i) {
      row2.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }
    
    final HistogramSpan hspan2 = new HistogramSpan(tsdb);
    hspan2.addRow(KEY, row2);
    spans.add(hspan2);
    
    HistogramAggregationIterator histAggIt = HistogramAggregationIterator.create(spans, BASE_TIME + 5000L * 5,
        BASE_TIME + 5000L * 10, HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, false);
    
    List<Long> values = new ArrayList<Long>();
    List<Long> timestamps_in_millis = new ArrayList<Long>();
    while (histAggIt.hasNext()) {
      HistogramDataPoint hdp = histAggIt.next();
      values.add(Bytes.getLong(hdp.getRawData()));
      timestamps_in_millis.add(hdp.timestamp());
    } // end while
    
    assertEquals(5, values.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(5 + i, values.get(i).longValue());
      assertEquals(BASE_TIME + 5000L * (5 + i), timestamps_in_millis.get(i).longValue());
    } // end for
  } // end testTwoHistogramSpanNoDownSamplerOneOutofRange()
}
