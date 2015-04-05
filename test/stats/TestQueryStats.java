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
import static org.junit.Assert.assertTrue;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.cache.CacheBuilder;

import net.opentsdb.core.QueryException;
import net.opentsdb.core.TSQuery;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
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
  
  @Before
  public void before() throws Exception {
    running_queries.set(null, new ConcurrentHashMap<Integer, QueryStats>());
    completed_queries.set(null, CacheBuilder.newBuilder().maximumSize(2).build());
  }
  
  @Test
  public void ctor() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertNotNull(stats);
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(1, map.get("running").size());
    assertEquals(0, map.get("completed").size());
  }
  
  @Test (expected = QueryException.class)
  public void ctorDuplicate() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertNotNull(stats);
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(1, map.get("running").size());
    assertEquals(0, map.get("completed").size());
    new QueryStats(remote, query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullRemote() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    new QueryStats(null, query);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void ctorNullQuery() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    new QueryStats(remote, null);
  }
  
  @Test
  public void testHashCodeandEquals() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertNotNull(stats);
    final int hash_a = stats.hashCode();
    
    // have to mark the old one as complete before we can test equality
    stats.markComplete();
    
    final TSQuery query2 = new TSQuery();
    query2.setStart("1h-ago");
    final QueryStats stats2 = new QueryStats(remote, query2);
    assertNotNull(stats);
    assertEquals(hash_a, stats2.hashCode());
    assertEquals(stats, stats2);
    assertFalse(stats == stats2);
  }
  
  @Test
  public void testHashCodeandNotEquals() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertNotNull(stats);
    final int hash_a = stats.hashCode();

    final TSQuery query2 = new TSQuery();
    query2.setStart("2h-ago");
    final QueryStats stats2 = new QueryStats(remote, query2);
    assertNotNull(stats);
    assertTrue(hash_a != stats2.hashCode());
    assertFalse(stats.equals(stats2));
    assertFalse(stats == stats2);
  }

  @Test
  public void testEqualsNull() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertFalse(stats.equals(null));
  }
  
  @Test
  public void testEqualsWrongType() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertFalse(stats.equals(new String("foo")));
  }
  
  @Test
  public void testEqualsSame() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    assertTrue(stats.equals(stats));
  }
  
  @Test
  public void markComplete() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    stats.markComplete();
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    final Map<String, Object> completed = map.get("completed").get(0);
    assertEquals(200, completed.get("status"));
  }
  
  @Test
  public void markCompleteTimeout() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    stats.markComplete(HttpResponseStatus.REQUEST_TIMEOUT, null);
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    final Map<String, Object> completed = map.get("completed").get(0);
    assertEquals(408, completed.get("status"));
  }
  
  @Test
  public void markCompleteDoubleMark() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    stats.markComplete();
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    Map<String, Object> completed = map.get("completed").get(0);
    assertEquals(200, completed.get("status"));
    stats.markComplete();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    completed = map.get("completed").get(0);
    assertEquals(200, completed.get("status"));
  }
  
  @Test
  public void executed() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    final QueryStats stats = new QueryStats(remote, query);
    stats.markComplete(HttpResponseStatus.REQUEST_TIMEOUT, null);
    final Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    final Map<String, Object> completed = map.get("completed").get(0);
    assertEquals(1L, completed.get("executed"));
  }
  
  @Test
  public void executedTwice() throws Exception {
    final TSQuery query = new TSQuery();
    query.setStart("1h-ago");
    QueryStats stats = new QueryStats(remote, query);
    stats.markComplete(HttpResponseStatus.REQUEST_TIMEOUT, null);
    Map<String, List<Map<String, Object>>> map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    Map<String, Object> completed = map.get("completed").get(0);
    assertEquals(1L, completed.get("executed"));
    
    stats = new QueryStats(remote, query);
    stats.markComplete(HttpResponseStatus.REQUEST_TIMEOUT, null);
    map = QueryStats.buildStats();
    assertNotNull(map);
    assertEquals(0, map.get("running").size());
    assertEquals(1, map.get("completed").size());
    completed = map.get("completed").get(0);
    assertEquals(2L, completed.get("executed"));
  }
}
