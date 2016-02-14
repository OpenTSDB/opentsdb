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
package net.opentsdb.stats;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyLong;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.CacheBuilder;

import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSQuery;
import net.opentsdb.stats.QueryStats.QueryStat;
import net.opentsdb.utils.DateTime;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({ DateTime.class, QueryStats.class })
public final class TestQueryStats {

  private static String remote = "192.168.1.1:4242";
  private static Field running_queries; 
  static {
      try {
        running_queries = QueryStats.class.getDeclaredField("running_queries");
        running_queries.setAccessible(true);
      } catch (Exception e) {
        throw new RuntimeException("Failed in static initializer", e);
      }
  }
  private static Field completed_queries;
  static {
    try {
      completed_queries = QueryStats.class.getDeclaredField("completed_queries");
      completed_queries.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  private Map<String, String> headers;
  
  @Before
  public void before() throws Exception {
    running_queries.set(null, new ConcurrentHashMap<Integer, QueryStats>());
    completed_queries.set(null, CacheBuilder.newBuilder().maximumSize(2).build());
    headers = new HashMap<String, String>(1);
    headers.put("Cookie", "Hide me!");
    PowerMockito.mockStatic(DateTime.class);
    PowerMockito.doAnswer(new Answer<Long>() {
      long ts = 1000L;
      @Override
      public Long answer(InvocationOnMock invocation) throws Throwable {
        return ts += 1000000000L;
      }
      
    }).when(DateTime.class, "nanoTime");
    PowerMockito.doCallRealMethod().when(DateTime.class, 
        "msFromNano", anyLong());
    PowerMockito.doCallRealMethod().when(DateTime.class, 
        "msFromNanoDiff", anyLong(), anyLong());
  }
  
  @Test
  public void ctor() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertNotNull(stats);
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(1, ((List<Object>)map.get("running")).size());
    assertEquals(0, ((Collection<QueryStats>)map.get("completed")).size());
    assertSame(headers, stats.getRequestHeaders());
  }
  
  @Test
  public void ctorDuplicate() throws Exception {
    QueryStats.setEnableDuplicates(false);
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertNotNull(stats);
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(1, ((List<Object>)map.get("running")).size());
    assertEquals(0, ((Collection<QueryStats>)map.get("completed")).size());
    try {
      new QueryStats(remote, query, headers);
      fail("Expected a QueryException");
    } catch (QueryException e) { }
    QueryStats.setEnableDuplicates(true);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullRemote() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    new QueryStats(null, query, headers);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullQuery() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    new QueryStats(remote, null, headers);
  }
  
  @Test
  public void ctorNullHeaders() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, null);
    assertNotNull(stats);
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(1, ((List<Object>)map.get("running")).size());
    assertEquals(0, ((Collection<QueryStats>)map.get("completed")).size());
  }
  
  @Test
  public void testHashCodeandEquals() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertNotNull(stats);
    final int hash_a = stats.hashCode();
    
    // have to mark the old one as complete before we can test equality
    stats.markSerializationSuccessful();
    
    final TSQuery query2 = new TSQuery();
    query2.setStart("1h-ago");
    final QueryStats stats2 = new QueryStats(remote, query2, headers);
    assertNotNull(stats);
    assertEquals(hash_a, stats2.hashCode());
    assertEquals(stats, stats2);
    assertFalse(stats == stats2);
  }
  
  @Test
  public void testHashCodeandNotEquals() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertNotNull(stats);
    final int hash_a = stats.hashCode();

    final TSQuery query2 = new TSQuery();
    query2.setStart("2h-ago");
    final QueryStats stats2 = new QueryStats(remote, query2, headers);
    assertNotNull(stats);
    assertTrue(hash_a != stats2.hashCode());
    assertFalse(stats.equals(stats2));
    assertFalse(stats == stats2);
  }

  @Test
  public void testEqualsNull() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertFalse(stats.equals(null));
  }
  
  @Test
  public void testEqualsWrongType() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertFalse(stats.equals(new String("foo")));
  }
  
  @Test
  public void testEqualsSame() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    assertTrue(stats.equals(stats));
  }
  
  @Test
  public void markComplete() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    stats.markSerializationSuccessful();
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(0, ((List<Object>)map.get("running")).size());
    assertEquals(1, ((Collection<QueryStats>)map.get("completed")).size());
    final QueryStats completed = ((Collection<QueryStats>)map.get("completed"))
        .iterator().next();
    assertEquals(200, completed.getHttpResponse().getCode());
  }
  
  @Test
  public void markCompleteTimeout() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    final RuntimeException timeout = new RuntimeException("Timeout!");
    stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, timeout);
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(0, ((List<Object>)map.get("running")).size());
    assertEquals(1, ((Collection<QueryStats>)map.get("completed")).size());
    final QueryStats completed = ((Collection<QueryStats>)map.get("completed"))
        .iterator().next();
    assertEquals(408, completed.getHttpResponse().getCode());
    assertTrue(completed.getException().startsWith("Timeout!\n"));
  }
  
  @Test
  public void executed() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, null);
    final Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(0, ((List<Object>)map.get("running")).size());
    assertEquals(1, ((Collection<QueryStats>)map.get("completed")).size());
    final QueryStats completed = ((Collection<QueryStats>)map.get("completed"))
        .iterator().next();
    assertEquals(1, completed.getExecuted());
  }
  
  @Test
  public void executedTwice() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    QueryStats stats = new QueryStats(remote, query, headers);
    stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, null);
    Map<String, Object> map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(0, ((List<Object>)map.get("running")).size());
    assertEquals(1, ((Collection<QueryStats>)map.get("completed")).size());
    QueryStats completed = ((Collection<QueryStats>)map.get("completed"))
        .iterator().next();
    assertEquals(1, completed.getExecuted());
    
    stats = new QueryStats(remote, query, headers);
    stats.markSerialized(HttpResponseStatus.REQUEST_TIMEOUT, null);
    map = QueryStats.getRunningAndCompleteStats();
    assertNotNull(map);
    assertEquals(0, ((List<Object>)map.get("running")).size());
    assertEquals(1, ((Collection<QueryStats>)map.get("completed")).size());
    completed = ((Collection<QueryStats>)map.get("completed"))
        .iterator().next();
    assertEquals(2, completed.getExecuted());
  }

  @Test
  public void getStat() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    stats.addStat(QueryStat.AGGREGATED_SIZE, 42);
    stats.markSerializationSuccessful();
    assertEquals(42, stats.getStat(QueryStat.AGGREGATED_SIZE));
    assertEquals(-1, stats.getStat(QueryStat.BYTES_FROM_STORAGE));
  }
  
  @Test
  public void getStatTime() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query, headers);
    stats.markSerializationSuccessful();
    assertEquals(1000.0, stats.getTimeStat(QueryStat.PROCESSING_PRE_WRITE_TIME), 0.001);
    assertEquals(Double.NaN, stats.getTimeStat(QueryStat.AVG_AGGREGATION_TIME), 0.001);
  }
}
