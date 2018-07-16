// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.query.joins;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.Pair;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestJoiner extends BaseJoinTest {
  
  @Test
  public void ctor() throws Exception {
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    assertSame(config, joiner.config);
    assertNull(joiner.encoded_joins);
    
    try {
      new Joiner(null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void hashStringIdDefaultLeftOneTagMatch() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatch() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_R)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(2, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    assertSame(ts, set.right_map.get(hash).get(1));
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatchDiffJoinTag() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), R_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_R)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdDefaultLeftOneTagMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_R)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // tagless should be kicked
    set.right_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertTrue(set.right_map.isEmpty());
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatchExplicitTagsDiffJoinTag() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), R_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_R)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // tagless should be kicked
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashStringIdDefaultMultiTagsMatch() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_STRING);
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
    
    // missing some tags will fail.
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "db01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }
  
  @Test
  public void hashStringIdDefaultMultiTagsMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_STRING);
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
       
    // still nothing of course.
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "db01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashStringIdNaturalNoTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }
  
  @Test
  public void hashStringIdNaturalWithOneTagMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is kicked out since we have tags.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdNaturalWithTwoTagsNotMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
        
    // tagless is kicked out since we have tags.
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdCrossNoTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }
  
  @Test
  public void hashStringIdCrossWithOneTagMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
        
    // cross kicks these out if tags are present
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdCrossWithTwoTagsNotMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L.getBytes(Const.UTF8_CHARSET), METRIC_R.getBytes(Const.UTF8_CHARSET));
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
        
    // cross kicks these out if tags are present
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(METRIC_L.getBytes(Const.UTF8_CHARSET), ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultLeftOneTagMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_R_BYTES, R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_R_BYTES, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(2, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    assertSame(ts, set.right_map.get(hash).get(1));
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatchDiffJoinTag() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, new byte[] { 1, 1, 1 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_R.getBytes(Const.UTF8_CHARSET), R_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_R.getBytes(Const.UTF8_CHARSET), ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultLeftOneTagMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_R_BYTES, R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_R_BYTES, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // tagless should be kicked
    set.right_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_R_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertTrue(set.right_map.isEmpty());
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatchExplicitTagsDiffJoinTag() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, new byte[] { 1, 1, 1 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_R_BYTES, R_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_R_BYTES, ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // tagless should be kicked
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_R_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultMultiTagsMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_BYTE);
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(OWNER, OWNER);
    encoded_joins.put(DC, DC);
    encoded_joins.put(ROLE, ROLE);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
    
    // missing some tags will fail.
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }

  @Test
  public void hashByteIdDefaultMultiTagsMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_BYTE);
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(OWNER, OWNER);
    encoded_joins.put(DC, DC);
    encoded_joins.put(ROLE, ROLE);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
       
    // still nothing of course.
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdNaturalNoTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }

  @Test
  public void hashByteIdNaturalWithOneTagMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is kicked out since we have tags.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdNaturalWithTwoTagsNotMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.NATURAL)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    encoded_joins.put(new byte[] { 2, 2, 2 }, new byte[] { 2, 2, 2 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertNull(set.left_map);
        
    // tagless is kicked out since we have tags.
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdCrossNoTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }
  
  @Test
  public void hashByteIdCrossWithOneTagMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
        
    // cross kicks these out if tags are present
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdCrossWithTwoTagsNotMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.CROSS)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    encoded_joins.put(new byte[] { 2, 2, 2 }, new byte[] { 2, 2, 2 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 
        METRIC_L_BYTES, METRIC_R_BYTES);
    joiner.hashByteId(METRIC_L_BYTES, L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(ReadableTimeSeriesDataStore.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
        
    // cross kicks these out if tags are present
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(METRIC_L_BYTES, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void joinIdsTwoString() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    Joiner joiner = new Joiner(config);
    
    TimeSeriesStringId id = (TimeSeriesStringId) joiner.joinIds(
        L_1, R_1, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // left only
    id = (TimeSeriesStringId) joiner.joinIds(L_1, null, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // right only
    id = (TimeSeriesStringId) joiner.joinIds(null, R_1, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // two tags
    id = (TimeSeriesStringId) joiner.joinIds(L_6A, R_6A, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(2, id.tags().size());
    assertEquals("web06", id.tags().get("host"));
    assertEquals("tyrion", id.tags().get("owner"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // promote tag
    id = (TimeSeriesStringId) joiner.joinIds(L_4, R_4A, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web04", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertEquals(1, id.disjointTags().size());
    assertTrue(id.disjointTags().contains("owner"));
    
    // tagless promote
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(TAGLESS_STRING);
    id = (TimeSeriesStringId) joiner.joinIds(L_1, ts, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(0, id.tags().size());
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("host"));
    assertTrue(id.disjointTags().isEmpty());
    
    // promote agg
    when(ts.id()).thenReturn(TAG_PROMOTION_L_STRING);
    TimeSeries r = mock(TimeSeries.class);
    when(r.id()).thenReturn(TAG_PROMOTION_R_STRING);
    id = (TimeSeriesStringId) joiner.joinIds(ts, r, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(0, id.tags().size());
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains("host"));
    assertEquals(4, id.disjointTags().size());
    assertTrue(id.disjointTags().contains("owner"));
    assertTrue(id.disjointTags().contains("dc"));
    assertTrue(id.disjointTags().contains("role"));
    assertTrue(id.disjointTags().contains("unit"));
    
    try {
      joiner.joinIds((TimeSeries) null, (TimeSeries) null, ALIAS2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.joinIds(L_1, R_1, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.joinIds(L_1, R_1, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // left join keep metric
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.LEFT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    joiner = new Joiner(config);
    
    id = (TimeSeriesStringId) joiner.joinIds(L_1, R_1, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // right join keep metric
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.RIGHT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    joiner = new Joiner(config);
    
    id = (TimeSeriesStringId) joiner.joinIds(L_1, R_1, ALIAS2);
    assertEquals(ALIAS2, id.alias());
    assertEquals(NAMESPACE, id.namespace());
    assertEquals(ALIAS2, id.metric());
    assertEquals(1, id.tags().size());
    assertEquals("web01", id.tags().get("host"));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
  }

  @Test
  public void joinIdsTwoByte() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    Joiner joiner = new Joiner(config);
    
    TimeSeriesByteId id = (TimeSeriesByteId) joiner.joinIds(
        L_1, R_1, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB01, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // left only
    id = (TimeSeriesByteId) joiner.joinIds(L_1, null, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB01, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // right only
    id = (TimeSeriesByteId) joiner.joinIds(null, R_1, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB01, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // two tags
    id = (TimeSeriesByteId) joiner.joinIds(L_6A, R_6A, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(2, id.tags().size());
    assertArrayEquals(WEB06, id.tags().get(HOST));
    assertArrayEquals(TYRION, id.tags().get(OWNER));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // promote tag
    id = (TimeSeriesByteId) joiner.joinIds(L_4, R_4A, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB04, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertEquals(1, id.disjointTags().size());
    assertTrue(id.disjointTags().contains(OWNER));
    
    // tagless promote
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    id = (TimeSeriesByteId) joiner.joinIds(L_1, ts, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(0, id.tags().size());
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains(HOST));
    assertTrue(id.disjointTags().isEmpty());
    
    // promote agg
    when(ts.id()).thenReturn(TAG_PROMOTION_L_BYTE);
    TimeSeries r = mock(TimeSeries.class);
    when(r.id()).thenReturn(TAG_PROMOTION_R_BYTE);
    id = (TimeSeriesByteId) joiner.joinIds(ts, r, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(0, id.tags().size());
    assertEquals(1, id.aggregatedTags().size());
    assertTrue(id.aggregatedTags().contains(HOST));
    assertEquals(4, id.disjointTags().size());
    assertTrue(id.disjointTags().contains(OWNER));
    assertTrue(id.disjointTags().contains(DC));
    assertTrue(id.disjointTags().contains(ROLE));
    assertTrue(id.disjointTags().contains(UNIT));
    
    try {
      joiner.joinIds((TimeSeries) null, (TimeSeries) null, ALIAS2);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.joinIds(L_1, R_1, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.joinIds(L_1, R_1, "");
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // left join keep metric
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.LEFT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    joiner = new Joiner(config);
    
    id = (TimeSeriesByteId) joiner.joinIds(L_1, R_1, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB01, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
    
    // right join keep metric
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.RIGHT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    joiner = new Joiner(config);
    
    id = (TimeSeriesByteId) joiner.joinIds(L_1, R_1, ALIAS2);
    assertArrayEquals(ALIAS2_BYTES, id.alias());
    assertArrayEquals(NAMESPACE_BYTES, id.namespace());
    assertArrayEquals(ALIAS2_BYTES, id.metric());
    assertEquals(1, id.tags().size());
    assertArrayEquals(WEB01, id.tags().get(HOST));
    assertTrue(id.aggregatedTags().isEmpty());
    assertTrue(id.disjointTags().isEmpty());
  }

  @Test
  public void joinStringSingleResult() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.OUTER_DISJOINT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    iterator = joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            false).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      if (pair.getKey() == null) {
        assertNotNull(pair.getValue());
      } else {
        assertNull(pair.getValue());
      }
      pairs++;
    }
    assertEquals(2, pairs);
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(singleResult(Const.TS_STRING_ID), 
        METRIC_L.getBytes(Const.UTF8_CHARSET), 
        METRIC_R.getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // empty result
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    
    iterator = joiner.join(Lists.newArrayList(result), 
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // null results
    try {
      joiner.join(null, 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty list.
    try {
      joiner.join(Lists.newArrayList(), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad keys
    try {
      joiner.join(singleResult(Const.TS_STRING_ID), 
          null, 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_STRING_ID), 
          new byte[0], 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_STRING_ID), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          null,
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_STRING_ID), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          new byte[0],
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void joinByteSingleResult() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(singleResult(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.OUTER_DISJOINT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    joiner.setEncodedJoins(encoded_joins);
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            false).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      if (pair.getKey() == null) {
        assertNotNull(pair.getValue());
      } else {
        assertNull(pair.getValue());
      }
      pairs++;
    }
    assertEquals(2, pairs);
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID), 
        METRIC_L_BYTES, 
        METRIC_R_BYTES,
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // empty result
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    
    iterator = joiner.join(Lists.newArrayList(result), 
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // null results
    try {
      joiner.join(null, 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty list.
    try {
      joiner.join(Lists.newArrayList(), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad keys
    try {
      joiner.join(singleResult(Const.TS_BYTE_ID), 
          null, 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_BYTE_ID), 
          new byte[0], 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_BYTE_ID), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          null,
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      joiner.join(singleResult(Const.TS_BYTE_ID), 
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          new byte[0],
          false);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void joinStringMultipleResult() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(multiResults(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.OUTER_DISJOINT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    iterator = joiner.join(multiResults(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            false).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      if (pair.getKey() == null) {
        assertNotNull(pair.getValue());
      } else {
        assertNull(pair.getValue());
      }
      pairs++;
    }
    assertEquals(2, pairs);
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(multiResults(Const.TS_STRING_ID), 
        METRIC_L.getBytes(Const.UTF8_CHARSET), 
        METRIC_R.getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // empty results
    List<QueryResult> results = Lists.newArrayListWithExpectedSize(2);
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    results.add(result);
    
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    results.add(result);
    
    iterator = joiner.join(results, 
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void joinByteMultipleResult() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(multiResults(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.OUTER_DISJOINT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    joiner.setEncodedJoins(encoded_joins);
    iterator = joiner.join(multiResults(Const.TS_BYTE_ID), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            false).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      if (pair.getKey() == null) {
        assertNotNull(pair.getValue());
      } else {
        assertNull(pair.getValue());
      }
      pairs++;
    }
    assertEquals(2, pairs);
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(multiResults(Const.TS_BYTE_ID), 
        METRIC_L_BYTES, 
        METRIC_R_BYTES,
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // empty results
    List<QueryResult> results = Lists.newArrayListWithExpectedSize(2);
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    results.add(result);
    
    result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(Collections.emptyList());
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    results.add(result);
    
    iterator = joiner.join(Lists.newArrayList(result), 
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        false).iterator();
    assertFalse(iterator.hasNext());
    
    // encoded_ids not set
    joiner = new Joiner(config);
    try {
      joiner.join(multiResults(Const.TS_BYTE_ID), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
          false);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }

  @Test
  public void joinStringAlias() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + ALIAS_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + ALIAS_R).getBytes(Const.UTF8_CHARSET),
            true).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback left
    iterator = joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + ALIAS_R).getBytes(Const.UTF8_CHARSET),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback right
    iterator = joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + ALIAS_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback both
    iterator = joiner.join(singleResult(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
  }
  
  @Test
  public void joinByteAlias() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(singleResult(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, ALIAS_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, ALIAS_R_BYTES),
            true).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback left
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, ALIAS_R_BYTES),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback right
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, ALIAS_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
    
    // fallback left
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            true).iterator();
    pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      assertNotNull(pair.getValue());
      pairs++;
    }
    assertEquals(9, pairs);
  }
  
  @Test
  public void joinFilterString() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(multiResults(Const.TS_STRING_ID), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET),
            true,
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      pairs++;
    }
    assertEquals(7, pairs);
  }
  
  @Test
  public void joinFilterByte() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<Pair<TimeSeries, TimeSeries>> iterator = 
        joiner.join(multiResults(Const.TS_BYTE_ID), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES),
            true,
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final Pair<TimeSeries, TimeSeries> pair = iterator.next();
      assertNotNull(pair.getKey());
      pairs++;
    }
    assertEquals(7, pairs);
    
    // encoded_ids not set
    joiner = new Joiner(config);
    try {
      joiner.join(multiResults(Const.TS_BYTE_ID), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
          true,
          false);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
}
