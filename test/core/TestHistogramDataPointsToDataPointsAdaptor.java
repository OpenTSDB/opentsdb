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

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

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

import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Config;

@RunWith(PowerMockRunner.class)
//"Classloader hell"...  It's real.  Tell PowerMock to ignore these classes
//because they fiddle with the class loader.  We don't test them anyway.
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
             "ch.qos.*", "org.slf4j.*",
             "com.sum.*", "org.xml.*"})
@PrepareForTest({ HistogramSpanGroup.class, HistogramSpan.class, HistogramRowSeq.class, TSDB.class, UniqueId.class, KeyValue.class, 
Config.class, RowKey.class })
public final class TestHistogramDataPointsToDataPointsAdaptor {
  private static final long BASE_TIME = 1356998400000L;
  public static final byte[] KEY = 
      new byte[] { 0, 0, 0, 0, 1, 0x50, (byte)0xE2, 0x27, 0, 0, 0, 0, 1, 0, 0, 0, 2 };
  
  private static long start_ts = 1356998400L;
  private static long end_ts = 1356998600L;
  
  private TSDB tsdb;
  
  @Before
  public void before() {
    tsdb = PowerMockito.mock(TSDB.class);
  }
  
  @Test
  public void getTagUids() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
  }
  
  @Test
  public void getTagUidsAggedOut() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    final ByteMap<byte[]> uids2 = new ByteMap<byte[]>();
    uids2.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 3 });
    final HistogramSpan span2 = mock(HistogramSpan.class);
    when(span2.getTagUids()).thenReturn(uids2);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    spans.add(span2);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    
    assertEquals(0, uids_read.size());
  }
  
  @Test
  public void getTagUidsNoSpans() throws Exception {
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    assertEquals(0, uids_read.size());
  }

  @Test
  public void getAggregatedTagUidsNotAgged() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final List<byte[]> uids_read = dps_ada.getAggregatedTagUids();
    
    assertEquals(0, uids_read.size());
  }
  
  @Test
  public void getAggregatedTagUids() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    final ByteMap<byte[]> uids2 = new ByteMap<byte[]>();
    uids2.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 3 });
    final HistogramSpan span2 = mock(HistogramSpan.class);
    when(span2.getTagUids()).thenReturn(uids2);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    spans.add(span2);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final List<byte[]> uids_read = dps_ada.getAggregatedTagUids();
    
    assertEquals(1, uids_read.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, uids_read.get(0));
  }
  
  @Test
  public void getAggregatedTagUidsNoSpans() throws Exception {
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final List<byte[]> uids_read = dps_ada.getAggregatedTagUids();
    
    assertEquals(0, uids_read.size());
  }

  @Test
  public void getTagUidsAggedNotInQuery() throws Exception {
    final ByteSet query_tags = new ByteSet();
    query_tags.add(new byte[] { 0, 0, 0, 3 });
    
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, query_tags));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    assertEquals(0, uids_read.size());
    
    final List<byte[]> agg_tags = dps_ada.getAggregatedTagUids();
    assertEquals(1, agg_tags.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, agg_tags.get(0));
  }
  
  @Test
  public void getTagUidsInQueryTags() throws Exception {
    final ByteSet query_tags = new ByteSet();
    query_tags.add(new byte[] { 0, 0, 0, 1 });
    
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, query_tags));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
    assertEquals(0, dps_ada.getAggregatedTagUids().size());
  }
  
  @Test
  public void getTagUidsNullQueryTags() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 0, 1 }, new byte[] { 0, 0, 0, 2 });
    final HistogramSpan span = mock(HistogramSpan.class);
    when(span.getTagUids()).thenReturn(uids);
    
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ArrayList<HistogramSpan> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(group, 0.98f);
    final ByteMap<byte[]> uids_read = dps_ada.getTagUids();
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
    assertEquals(0, dps_ada.getAggregatedTagUids().size());
  }
  
  @Test
  public void iteratorAllItems() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }

    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);

    final ByteSet query_tags = new ByteSet();
    query_tags.add(new byte[] { 0, 0, 0, 1 });
    HistogramSpanGroup hist_span_group = new HistogramSpanGroup(tsdb, BASE_TIME, BASE_TIME + 5000L * 10, spans,
        HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, 0, false, query_tags);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(hist_span_group, 0.98f);
    List<Double> values = new ArrayList<Double>();
    List<Long> timestamp_in_ms = new ArrayList<Long>();
    for (DataPoint dp : dps_ada) {
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
      timestamp_in_ms.add(dp.timestamp());
    } // end for
    
    assertTrue(dps_ada.isPercentile());
    List<Double> to_checks = new ArrayList<Double>();
    for (int i = 0; i < 10; ++i) {
      to_checks.add(i * 0.98);
    } // end for
    
    assertEquals(values.size(), to_checks.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(values.get(i).doubleValue(), to_checks.get(i).doubleValue(), 0.0001);
      assertEquals(timestamp_in_ms.get(i).longValue(), BASE_TIME + 5000L * i);
    } // end for
  }
  
  @Test
  public void doubleIteratorAllItems() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }

    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);

    final ByteSet query_tags = new ByteSet();
    query_tags.add(new byte[] { 0, 0, 0, 1 });
    HistogramSpanGroup hist_span_group = new HistogramSpanGroup(tsdb, BASE_TIME, BASE_TIME + 5000L * 10, spans,
        HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, 0, false, query_tags);
    
    HistogramDataPointsToDataPointsAdaptor dps_ada = new HistogramDataPointsToDataPointsAdaptor(hist_span_group, 0.98f);
    List<Double> values = new ArrayList<Double>();
    for (DataPoint dp : dps_ada) {
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
    } // end for
    
    List<Double> values2 = new ArrayList<Double>();
    for (DataPoint dp : dps_ada) {
      values2.add(dp.doubleValue());
    } // end for
    
    assertTrue(dps_ada.isPercentile());
    List<Double> to_checks = new ArrayList<Double>();
    for (int i = 0; i < 10; ++i) {
      to_checks.add(i * 0.98);
    } // end for
    
    assertEquals(values.size(), to_checks.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(values.get(i).doubleValue(), to_checks.get(i).doubleValue(), 0.0001);
      assertEquals(values.get(i).doubleValue(), values2.get(i).doubleValue(), 0.0001);
    } // end for
  }
  
  @Test
  public void iteratorAllItemsWithDiffPercentile() {
    List<HistogramDataPoint> row = new ArrayList<HistogramDataPoint>();
    for (int i = 0; i < 10; ++i) {
      row.add(new LongHistogramDataPointForTest(BASE_TIME + 5000L * i, Bytes.fromLong(i)));
    }

    final HistogramSpan hspan = new HistogramSpan(tsdb);
    hspan.addRow(KEY, row);

    List<HistogramSpan> spans = new ArrayList<HistogramSpan>();
    spans.add(hspan);

    final ByteSet query_tags = new ByteSet();
    query_tags.add(new byte[] { 0, 0, 0, 1 });
    HistogramSpanGroup hist_span_group = new HistogramSpanGroup(tsdb, BASE_TIME, BASE_TIME + 5000L * 10, spans,
        HistogramAggregation.SUM, DownsamplingSpecification.NO_DOWNSAMPLER, 0, 0, 0, false, query_tags);
    
    // 98 percentile
    HistogramDataPointsToDataPointsAdaptor dps_ada_98 = new HistogramDataPointsToDataPointsAdaptor(hist_span_group, 0.98f);
    List<Double> values = new ArrayList<Double>();
    for (DataPoint dp : dps_ada_98) {
      assertFalse(dp.isInteger());
      values.add(dp.doubleValue());
    } // end for
    
    assertTrue(dps_ada_98.isPercentile());
    List<Double> to_checks = new ArrayList<Double>();
    for (int i = 0; i < 10; ++i) {
      to_checks.add(i * 0.98);
    } // end for
    
    assertEquals(values.size(), to_checks.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(values.get(i).doubleValue(), to_checks.get(i).doubleValue(), 0.0001);
    } // end for
    
    // 95 percentile
    HistogramDataPointsToDataPointsAdaptor dps_ada_95 = new HistogramDataPointsToDataPointsAdaptor(hist_span_group, 0.95f);
    List<Double> values_95 = new ArrayList<Double>();
    for (DataPoint dp : dps_ada_95) {
      assertFalse(dp.isInteger());
      values_95.add(dp.doubleValue());
    } // end for
    
    assertTrue(dps_ada_95.isPercentile());
    List<Double> to_checks_95 = new ArrayList<Double>();
    for (int i = 0; i < 10; ++i) {
      to_checks_95.add(i * 0.95);
    } // end for
    
    assertEquals(values_95.size(), to_checks_95.size());
    for (int i = 0; i < values.size(); ++i) {
      assertEquals(values_95.get(i).doubleValue(), to_checks_95.get(i).doubleValue(), 0.0001);
    } // end for
  }
}
