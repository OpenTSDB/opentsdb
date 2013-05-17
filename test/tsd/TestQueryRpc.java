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
package net.opentsdb.tsd;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mock;

import java.lang.reflect.Method;

import net.opentsdb.core.DataPoints;
import net.opentsdb.core.Query;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSQuery;
import net.opentsdb.core.TSSubQuery;
import net.opentsdb.utils.Config;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Unit tests for the Query RPC class that handles parsing user queries for
 * timeseries data and returning that data
 * <b>Note:</b> Testing query validation and such should be done in the 
 * core.TestTSQuery and TestTSSubQuery classes
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({TSDB.class, Config.class, HttpQuery.class, Query.class})
public final class TestQueryRpc {
  private TSDB tsdb = null;
  final private QueryRpc rpc = new QueryRpc();
  final private Query empty_query = mock(Query.class);
  
  private static final Method parseQuery;
  static {
    try {
      parseQuery = QueryRpc.class.getDeclaredMethod("parseQuery", 
          TSDB.class, HttpQuery.class);
      parseQuery.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    tsdb = NettyMocks.getMockedHTTPTSDB();
    when(tsdb.newQuery()).thenReturn(empty_query);
    when(empty_query.run()).thenReturn(new DataPoints[0]);
  }
  
  @Test
  public void parseQueryMType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals("sys.cpu.0", sub.getMetric());
  }
  
  @Test
  public void parseQueryMTypeWEnd() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&end=5m-ago&m=sum:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertEquals("5m-ago", tsq.getEnd());
  }
  
  @Test
  public void parseQuery2MType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0&m=avg:sys.cpu.1");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq.getQueries());
    assertEquals(2, tsq.getQueries().size());
    TSSubQuery sub1 = tsq.getQueries().get(0);
    assertNotNull(sub1);
    assertEquals("sum", sub1.getAggregator());
    assertEquals("sys.cpu.0", sub1.getMetric());
    TSSubQuery sub2 = tsq.getQueries().get(1);
    assertNotNull(sub2);
    assertEquals("avg", sub2.getAggregator());
    assertEquals("sys.cpu.1", sub2.getMetric());
  }
  
  @Test
  public void parseQueryMTypeWRate() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:rate:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryMTypeWDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:1h-avg:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertEquals("1h-avg", sub.getDownsample());
  }
  
  @Test
  public void parseQueryMTypeWRateAndDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:1h-avg:rate:sys.cpu.0");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertTrue(sub.getRate());
    assertEquals("1h-avg", sub.getDownsample());
  }
  
  @Test
  public void parseQueryMTypeWTag() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0{host=web01}");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub.getTags());
    assertEquals("web01", sub.getTags().get("host"));
  }
  
  @Test
  public void parseQueryTSUIDType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
  }
  
  @Test
  public void parseQueryTSUIDTypeMulti() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101,020202");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(2, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("020202", sub.getTsuids().get(1));
  }
  
  @Test
  public void parseQuery2TSUIDType() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:010101&tsuid=avg:020202");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    assertEquals(2, tsq.getQueries().size());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    sub = tsq.getQueries().get(1);
    assertNotNull(sub);
    assertEquals("avg", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("020202", sub.getTsuids().get(0));
  }
  
  @Test
  public void parseQueryTSUIDTypeWRate() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:rate:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryTSUIDTypeWDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:1m-sum:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("1m-sum", sub.getDownsample());
  }
  
  @Test
  public void parseQueryTSUIDTypeWRateAndDS() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&tsuid=sum:1m-sum:rate:010101");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertEquals("1h-ago", tsq.getStart());
    assertNotNull(tsq.getQueries());
    TSSubQuery sub = tsq.getQueries().get(0);
    assertNotNull(sub);
    assertEquals("sum", sub.getAggregator());
    assertEquals(1, sub.getTsuids().size());
    assertEquals("010101", sub.getTsuids().get(0));
    assertEquals("1m-sum", sub.getDownsample());
    assertTrue(sub.getRate());
  }
  
  @Test
  public void parseQueryWPadding() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago&m=sum:sys.cpu.0&padding");
    TSQuery tsq = (TSQuery) parseQuery.invoke(rpc, tsdb, query);
    assertNotNull(tsq);
    assertTrue(tsq.getPadding());
  }
  
  @Test (expected = BadRequestException.class)
  public void parseQueryStartMissing() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?end=1h-ago&m=sum:sys.cpu.0");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test (expected = BadRequestException.class)
  public void parseQueryNoSubQuery() throws Exception {
    HttpQuery query = NettyMocks.getQuery(tsdb, 
      "/api/query?start=1h-ago");
    parseQuery.invoke(rpc, tsdb, query);
  }
  
  @Test
  public void parse() throws Exception {
    HttpQuery query = NettyMocks.postQuery(tsdb, "/api/query",
        "{\"start\":1356998400,\"end\":1356998460,\"queries\":[{\"aggregator"
        + "\": \"sum\",\"metric\": \"sys.cpu.0\",\"rate\": \"true\",\"tags\": "
        + "{\"host\": \"*\",\"dc\": \"lga\"}}]}");
    rpc.execute(tsdb, query);
    assertEquals(HttpResponseStatus.OK, query.response().getStatus());
  }
}
