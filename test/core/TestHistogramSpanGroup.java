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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.List;

import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import org.powermock.reflect.Whitebox;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ TSDB.class, HBaseClient.class, Config.class, HistogramSpanGroup.class,
  HistogramSpan.class })
public final class TestHistogramSpanGroup {
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
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
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
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(0, uids_read.size());
  }
  
  @Test
  public void getTagUidsNoSpans() throws Exception {
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
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
    
    final List<byte[]> uids_read = group.getAggregatedTagUids();
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
    
    final List<byte[]> uids_read = group.getAggregatedTagUids();
    assertEquals(1, uids_read.size());
    assertArrayEquals(new byte[] { 0, 0, 0, 1 }, uids_read.get(0));
  }
  
  @Test
  public void getAggregatedTagUidsNoSpans() throws Exception {
    DownsamplingSpecification specification = new DownsamplingSpecification("1dc-sum");
    final HistogramSpanGroup group = PowerMockito.spy(new HistogramSpanGroup(tsdb, start_ts, 
        end_ts, null, HistogramAggregation.SUM, specification, 0, 0, 0, false, null));
    
    final List<byte[]> uids_read = group.getAggregatedTagUids();
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
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(0, uids_read.size());
    final List<byte[]> agg_tags = group.getAggregatedTagUids();
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
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
    assertEquals(0, group.getAggregatedTagUids().size());
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
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] {0, 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
    assertEquals(0, group.getAggregatedTagUids().size());
  }
}
