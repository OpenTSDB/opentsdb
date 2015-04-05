// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;

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
@PrepareForTest({ TSDB.class, HBaseClient.class, Config.class, SpanGroup.class,
  Span.class })
public final class TestSpanGroup {
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
    uids.put(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 2 });
    final Span span = mock(Span.class);
    when(span.getTagUids()).thenReturn(uids);
    
    final SpanGroup group = PowerMockito.spy(new SpanGroup(tsdb, start_ts, 
        end_ts, null, false, Aggregators.SUM, 0, null));
    final ArrayList<Span> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(1, uids_read.size());
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 1 }, uids_read.firstKey()));
    assertEquals(0, Bytes.memcmp(new byte[] { 0, 0, 2 }, 
        uids_read.firstEntry().getValue()));
  }
  
  @Test
  public void getTagUidsAggedOut() throws Exception {
    final ByteMap<byte[]> uids = new ByteMap<byte[]>();
    uids.put(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 2 });
    final Span span = mock(Span.class);
    when(span.getTagUids()).thenReturn(uids);
    
    final ByteMap<byte[]> uids2 = new ByteMap<byte[]>();
    uids2.put(new byte[] { 0, 0, 1 }, new byte[] { 0, 0, 3 });
    final Span span2 = mock(Span.class);
    when(span2.getTagUids()).thenReturn(uids2);
    
    final SpanGroup group = PowerMockito.spy(new SpanGroup(tsdb, start_ts, 
        end_ts, null, false, Aggregators.SUM, 0, null));
    final ArrayList<Span> spans = Whitebox.getInternalState(group, "spans");
    spans.add(span);
    spans.add(span2);
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(0, uids_read.size());
  }
  
  @Test
  public void getTagUidsNoSpans() throws Exception {
    final SpanGroup group = new SpanGroup(tsdb, start_ts, end_ts, null, 
        false, Aggregators.SUM, 0, null);
    
    final ByteMap<byte[]> uids_read = group.getTagUids();
    assertEquals(0, uids_read.size());
  }
}
