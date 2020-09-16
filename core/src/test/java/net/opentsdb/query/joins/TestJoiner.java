// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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

import java.util.Arrays;
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
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.joins.Joiner.Operand;
import net.opentsdb.query.processor.expressions.ExpressionConfig;
import net.opentsdb.query.processor.expressions.ExpressionParseNode;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.ExpressionOp;
import net.opentsdb.query.processor.expressions.ExpressionParseNode.OperandType;
import net.opentsdb.query.processor.expressions.TernaryParseNode;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestJoiner extends BaseJoinTest {
  
  @Test
  public void ctor() throws Exception {
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
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
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatch() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.RIGHT, R_1, set);
    
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
    
    joiner.hashStringId(Operand.RIGHT, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(2, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    assertSame(ts, set.right_map.get(hash).get(1));
  }
  
  @Test
  public void hashStringIdDefaultLeftOneTagMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdDefaultRightOneTagMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.RIGHT, R_1, set);
    
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
    
    joiner.hashStringId(Operand.RIGHT, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // tagless should be kicked
    set.right_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.RIGHT, ts, set);
    
    assertNull(set.left_map);
    assertTrue(set.right_map.isEmpty());
  }
  
  @Test
  public void hashStringIdDefaultMultiTagsMatch() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_STRING);
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
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
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }
  
  @Test
  public void hashStringIdDefaultMultiTagsMatchExplicitTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("owner", "owner")
        .addJoins("dc", "dc")
        .addJoins("role", "role")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    TimeSeries ts = mock(TimeSeries.class);
    when(ts.id()).thenReturn(MANY_TAGS_STRING);
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
       
    // still nothing of course.
    TimeSeriesId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(METRIC_L)
        .addTags("host", "db01")
        .addTags("owner", "tyrion")
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashStringIdNaturalNoTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
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
        .setJoinType(JoinType.NATURAL)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is kicked out since we have tags.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdNaturalWithTwoTagsNotMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertNull(set.left_map);
        
    // tagless is kicked out since we have tags.
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdNaturalNoAliasOrNamespace() throws Exception {
    l1_id = BaseTimeSeriesStringId.newBuilder()
        //.setAlias(ALIAS_L)
        //.setNamespace(NAMESPACE)
        .setMetric(METRIC_L)
        .addTags("host", "web01")
        .build();
    when(L_1.id()).thenReturn(l1_id);
    r1_id = BaseTimeSeriesStringId.newBuilder()
        //.setAlias(ALIAS_R)
        //.setNamespace(NAMESPACE)
        .setMetric(METRIC_R)
        .addTags("host", "web01")
        .build();
    when(R_1.id()).thenReturn(r1_id);
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    joiner.hashStringId(Operand.RIGHT, R_1, set);
    
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertEquals(1, set.right_map.size());
    assertSame(R_1, set.right_map.get(hash).get(0));
  }
  
  @Test
  public void hashStringIdCrossNoTags() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.CROSS)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
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
        .setJoinType(JoinType.CROSS)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
        
    // cross kicks these out if tags are present
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashStringIdCrossWithTwoTagsNotMatching() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.CROSS)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashStringId(Operand.LEFT, L_1, set);
    
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
    
    joiner.hashStringId(Operand.LEFT, ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
        
    // cross kicks these out if tags are present
    when(ts.id()).thenReturn(TAGLESS_STRING);
    joiner.hashStringId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultLeftOneTagMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.RIGHT, R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will hash to the same value
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.RIGHT, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(2, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    assertSame(ts, set.right_map.get(hash).get(1));
  }

  @Test
  public void hashByteIdDefaultLeftOneTagMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // tagless should be kicked
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdDefaultRightOneTagMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setExplicitTags(true)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.RIGHT, R_1, set);
    
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    long hash = set.right_map.keys()[0];
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // extra tags will be kicked out.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.RIGHT, ts, set);
    assertNull(set.left_map);
    assertEquals(1, set.right_map.size());
    assertEquals(1, set.right_map.get(hash).size());
    assertSame(R_1, set.right_map.get(hash).get(0));
    
    // tagless should be kicked
    set.right_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.RIGHT, ts, set);
    
    assertNull(set.left_map);
    assertTrue(set.right_map.isEmpty());
  }

  @Test
  public void hashByteIdDefaultMultiTagsMatch() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
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
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
    
    // missing some tags will fail.
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(ts, set.left_map.get(hash).get(0));
  }

  @Test
  public void hashByteIdDefaultMultiTagsMatchExplicitTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
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
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
       
    // still nothing of course.
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void hashByteIdNaturalNoTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
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
        .setJoinType(JoinType.NATURAL)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is kicked out since we have tags.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdNaturalWithTwoTagsNotMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    encoded_joins.put(new byte[] { 2, 2, 2 }, new byte[] { 2, 2, 2 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertNull(set.left_map);
        
    // tagless is kicked out since we have tags.
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdNaturalNoAliasOrNamespace() throws Exception {
    l1_id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        //.setAlias(ALIAS_L_BYTES)
        //.setNamespace(NAMESPACE_BYTES)
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .build();
    when(L_1.id()).thenReturn(l1_id);
    r1_id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        //.setAlias(ALIAS_R_BYTES)
        //.setNamespace(NAMESPACE_BYTES)
        .setMetric(METRIC_R_BYTES)
        .addTags(HOST, WEB01)
        .build();
    when(R_1.id()).thenReturn(r1_id);
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    joiner.hashByteId(Operand.RIGHT, R_1, set);
    
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertEquals(1, set.right_map.size());
    assertSame(R_1, set.right_map.get(hash).get(0));
  }
  
  @Test
  public void hashByteIdCrossNoTags() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.CROSS)
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // extra tags now hash to something different.
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(2, set.left_map.size());
    assertNull(set.right_map);
        
    // tagless is NOT kicked out for naturals.
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
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
        .setJoinType(JoinType.CROSS)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    long hash = set.left_map.keys()[0];
    assertEquals(1, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertEquals(1, set.left_map.size());
    assertNull(set.right_map);
    assertEquals(2, set.left_map.get(hash).size());
    assertSame(L_1, set.left_map.get(hash).get(0));
    assertSame(ts, set.left_map.get(hash).get(1));
        
    // cross kicks these out if tags are present
    set.left_map.clear();
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertTrue(set.left_map.isEmpty());
    assertNull(set.right_map);
  }
  
  @Test
  public void hashByteIdCrossWithTwoTagsNotMatching() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.CROSS)
        .addJoins("host", "host")
        .addJoins("noTag", "noTag")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    encoded_joins.put(new byte[] { 2, 2, 2 }, new byte[] { 2, 2, 2 });
    joiner.setEncodedJoins(encoded_joins);
    KeyedHashedJoinSet set = new KeyedHashedJoinSet(JoinType.INNER, 1, false);
    joiner.hashByteId(Operand.LEFT, L_1, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
    
    // cross falls back to normal hashing
    TimeSeries ts = mock(TimeSeries.class);
    TimeSeriesId id = BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
        .setMetric(METRIC_L_BYTES)
        .addTags(HOST, WEB01)
        .addTags(OWNER, TYRION)
        .build();
    when(ts.id()).thenReturn(id);
    
    joiner.hashByteId(Operand.LEFT, ts, set);
    assertNull(set.left_map);
    assertNull(set.right_map);
        
    // cross kicks these out if tags are present
    when(ts.id()).thenReturn(TAGLESS_BYTE);
    joiner.hashByteId(Operand.LEFT, ts, set);
    
    assertNull(set.left_map);
    assertNull(set.right_map);
  }

  @Test
  public void joinIdsTwoString() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
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
        .setJoinType(JoinType.LEFT)
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
        .setJoinType(JoinType.RIGHT)
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
        .setJoinType(JoinType.INNER)
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
    assertTrue(Joiner.contains(OWNER, id.disjointTags()));
    
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
        .setJoinType(JoinType.LEFT)
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
        .setJoinType(JoinType.RIGHT)
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
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
            expression_config,
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            null).iterator();
    // need 2
    assertFalse(iterator.hasNext());
    
    expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    joiner = new Joiner(config);
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
            expression_config,
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            null,
            null).iterator();
    assertTrue(iterator.hasNext());
    TimeSeries[] series = iterator.next();
    assertEquals(L_5A, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_5B, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_4, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_6A, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_6B, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_2, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_1, series[0]);
    assertNull(series[1]);
    
    assertFalse(iterator.hasNext());
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
        expression_config,
        METRIC_L.getBytes(Const.UTF8_CHARSET), 
        METRIC_R.getBytes(Const.UTF8_CHARSET),
        null).iterator();
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
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    // null results
    try {
      joiner.join(null, 
          expression_config,
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty list.
    try {
      joiner.join(Lists.newArrayList(), 
          expression_config,
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad keys
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
        expression_config,
        null, 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L),
        expression_config,
        new byte[0], 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        null,
        null).iterator();
    
    iterator = joiner.join(singleResult(Const.TS_STRING_ID, NAMESPACE + METRIC_L), 
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        new byte[0],
        null).iterator();
  }
  
  @Test
  public void joinByteSingleResult() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
            expression_config,
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            null).iterator();
    assertFalse(iterator.hasNext());
    
    expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("42")
        .setRightType(OperandType.LITERAL_NUMERIC)
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    joiner = new Joiner(config);
    joiner.setEncodedJoins(encoded_joins);
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
        null, 
        null).iterator();
    assertTrue(iterator.hasNext());
    TimeSeries[] series = iterator.next();
    assertEquals(L_6A, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_6B, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_2, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_5A, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_5B, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_1, series[0]);
    assertNull(series[1]);
    
    series = iterator.next();
    assertEquals(L_4, series[0]);
    assertNull(series[1]);
    
    assertFalse(iterator.hasNext());
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        METRIC_L_BYTES, 
        null,
        null).iterator();
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
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    // null results
    try {
      joiner.join(null, 
          expression_config,
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // empty list.
    try {
      joiner.join(Lists.newArrayList(), 
          expression_config,
          (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
          (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
          null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // bad keys
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        null, 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        new byte[0], 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        null,
        null).iterator();
    assertFalse(iterator.hasNext());
    
    iterator = joiner.join(singleResult(Const.TS_BYTE_ID, NAMESPACE + METRIC_L), 
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        new byte[0],
        null).iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void joinStringMultipleResult() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(multiResults(Const.TS_STRING_ID,
                                 NAMESPACE + METRIC_L,
                                 NAMESPACE + METRIC_R), 
            expression_config,
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            null).iterator();
    
    TimeSeries[] pair = iterator.next();
    assertEquals(L_5A, pair[0]);
    assertEquals(R_5, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_5B, pair[0]);
    assertEquals(R_5, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_4, pair[0]);
    assertEquals(R_4A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_4, pair[0]);
    assertEquals(R_4B, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6A, pair[0]);
    assertEquals(R_6A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6A, pair[0]);
    assertEquals(R_6B, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6B, pair[0]);
    assertEquals(R_6A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6B, pair[0]);
    assertEquals(R_6B, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_1, pair[0]);
    assertEquals(R_1, pair[1]);
    
    assertFalse(iterator.hasNext());
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(multiResults(Const.TS_STRING_ID,
                                        NAMESPACE + METRIC_L,
                                        NAMESPACE + METRIC_R), 
        expression_config,
        METRIC_L.getBytes(Const.UTF8_CHARSET), 
        METRIC_R.getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    // empty results
    List<QueryResult> results = Lists.newArrayList();
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
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
  }

  @Test
  public void joinByteMultipleResult() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(multiResults(Const.TS_BYTE_ID,
                                  NAMESPACE + METRIC_L,
                                  NAMESPACE + METRIC_R),
            expression_config,
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            null).iterator();
    
    TimeSeries[] pair = iterator.next();
    assertEquals(L_6A, pair[0]);
    assertEquals(R_6A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6A, pair[0]);
    assertEquals(R_6B, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6B, pair[0]);
    assertEquals(R_6A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_6B, pair[0]);
    assertEquals(R_6B, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_5A, pair[0]);
    assertEquals(R_5, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_5B, pair[0]);
    assertEquals(R_5, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_1, pair[0]);
    assertEquals(R_1, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_4, pair[0]);
    assertEquals(R_4A, pair[1]);
    
    pair = iterator.next();
    assertEquals(L_4, pair[0]);
    assertEquals(R_4B, pair[1]);
    
    assertFalse(iterator.hasNext());
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.OUTER_DISJOINT)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    joiner.setEncodedJoins(encoded_joins);
    iterator = joiner.join(multiResults(Const.TS_BYTE_ID,
                                        NAMESPACE + METRIC_L,
                                        NAMESPACE + METRIC_R), 
        expression_config,
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
        null).iterator();
    pair = iterator.next();
    assertEquals(L_2, pair[0]);
    assertEquals(null, pair[1]);
    
    pair = iterator.next();
    assertEquals(null, pair[0]);
    assertEquals(R_3, pair[1]);
    
    assertFalse(iterator.hasNext());
    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(multiResults(Const.TS_BYTE_ID,
                                        NAMESPACE + METRIC_L,
                                        NAMESPACE + METRIC_R), 
        expression_config,
        METRIC_L_BYTES, 
        METRIC_R_BYTES,
        null).iterator();
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
    
    iterator = joiner.join(Lists.newArrayList(result, null),
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        null).iterator();
    assertFalse(iterator.hasNext());
    
    // encoded_ids not set
    joiner = new Joiner(config);
    try {
      joiner.join(multiResults(Const.TS_BYTE_ID,
                               NAMESPACE + METRIC_L,
                               NAMESPACE + METRIC_R), 
          expression_config,
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
          null);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void joinStringTernary() throws Exception {
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    TernaryParseNode expression_config = 
        (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("c")
        .setConditionType(OperandType.VARIABLE)
        .setConditionId(queryResultId(NAMESPACE + TERNARY))
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(ternaryResults(Const.TS_STRING_ID,
                                   NAMESPACE + METRIC_L,
                                   NAMESPACE + METRIC_R,
                                   NAMESPACE + TERNARY),
            expression_config,
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            (NAMESPACE + TERNARY).getBytes(Const.UTF8_CHARSET))
        .iterator();
    
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, iterator.next());
    
    assertFalse(iterator.hasNext());
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL_OUTER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    iterator = joiner.join(ternaryResults(Const.TS_STRING_ID,
                                          NAMESPACE + METRIC_L,
                                          NAMESPACE + METRIC_R,
                                          NAMESPACE + TERNARY), 
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        (NAMESPACE + TERNARY).getBytes(Const.UTF8_CHARSET))
        .iterator();
    
    assertArrayEquals(new TimeSeries[] { null, R_4B, T_4B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5A, null, T_5A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5B, null, T_5B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { null, R_4A, T_4A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_2, null, T_2 }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, iterator.next());
  }
  
  @Test
  public void joinByteTernary() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    TernaryParseNode expression_config = 
        (TernaryParseNode) TernaryParseNode.newBuilder()
        .setCondition("c")
        .setConditionType(OperandType.VARIABLE)
        .setConditionId(queryResultId(NAMESPACE + TERNARY))
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(ternaryResults(Const.TS_BYTE_ID,
                                   NAMESPACE + METRIC_L,
                                   NAMESPACE + METRIC_R,
                                   NAMESPACE + TERNARY),
            expression_config,
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, TERNARY_BYTES))
        .iterator();
    
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6A, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6A, R_6B, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6A, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_6B, R_6B, T_6B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5A, R_5, T_5B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_5B, R_5, T_5B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_1, R_1, T_1 }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4A }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4A, T_4B }, iterator.next());
    assertArrayEquals(new TimeSeries[] { L_4, R_4B, T_4B }, iterator.next());
    
    assertFalse(iterator.hasNext());
    
    config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.NATURAL_OUTER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    joiner = new Joiner(config);
    joiner.setEncodedJoins(encoded_joins);
    iterator = joiner.join(ternaryResults(Const.TS_BYTE_ID,
                                          NAMESPACE + METRIC_L,
                                          NAMESPACE + METRIC_R,
                                          NAMESPACE + TERNARY), 
        expression_config,
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
        com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, TERNARY_BYTES))
        .iterator();
//    pairs = 0;
    while (iterator.hasNext()) {
      final TimeSeries[] pair = iterator.next();
      System.out.println(Arrays.toString(pair));
//      // some left and right are null here but we have the full ternary set.
//      assertNotNull(pair[2]);
//      pairs++;
    }
//    assertEquals(8, pairs);
//    
    // wrong keys. Missing the namespace.
    iterator = joiner.join(ternaryResults(Const.TS_BYTE_ID,
                                          NAMESPACE + METRIC_L,
                                          NAMESPACE + METRIC_R,
                                          NAMESPACE + TERNARY),
        expression_config,
        METRIC_L_BYTES, 
        METRIC_R_BYTES,
        TERNARY_BYTES)
        .iterator();
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
    
    iterator = joiner.join(Lists.newArrayList(result, null),
        expression_config,
        (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
        (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
        (NAMESPACE + TERNARY).getBytes(Const.UTF8_CHARSET)).iterator();
    assertFalse(iterator.hasNext());
    
    // encoded_ids not set
    config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    joiner = new Joiner(config);
    try {
      joiner.join(ternaryResults(Const.TS_BYTE_ID,
                                 NAMESPACE + METRIC_L,
                                 NAMESPACE + METRIC_R,
                                 NAMESPACE + TERNARY),
          expression_config,
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, TERNARY_BYTES));
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }
  
  @Test
  public void joinFilterString() throws Exception {
    // See the various joins for details.
    setStringIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(multiResults(Const.TS_STRING_ID, 
                                 NAMESPACE + METRIC_L, 
                                 NAMESPACE + METRIC_R), 
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET),
            true,
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final TimeSeries[] pair = iterator.next();
      assertNotNull(pair[0]);
      assertNull(pair[1]);
      pairs++;
    }
    assertEquals(7, pairs);
  }
  
  @Test
  public void joinFilterByte() throws Exception {
    // See the various joins for details.
    setByteIds();
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "host")
        .setId(ID)
        .build();
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, HOST);
    joiner.setEncodedJoins(encoded_joins);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(multiResults(Const.TS_BYTE_ID, 
                                 NAMESPACE + METRIC_L, 
                                 NAMESPACE + METRIC_R), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES),
            true,
            false).iterator();
    int pairs = 0;
    while (iterator.hasNext()) {
      final TimeSeries[] pair = iterator.next();
      assertNotNull(pair[0]);
      assertNull(pair[1]);
      pairs++;
    }
    assertEquals(7, pairs);
    
    // encoded_ids not set
    joiner = new Joiner(config);
    try {
      joiner.join(multiResults(Const.TS_BYTE_ID, 
                               NAMESPACE + METRIC_L, 
                               NAMESPACE + METRIC_R), 
          com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
          true,
          false);
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) { }
  }

  @Test
  public void joinStringIdRightOneTagMatchDiffJoinTag() throws Exception {
    setStringIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    final List<QueryResult> results = Lists.newArrayList();
    QueryResult result = mock(QueryResult.class);
    List<TimeSeries> ts = Lists.newArrayList(L_5A);
    when(result.timeSeries()).thenReturn(ts);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    when(result.dataSource()).thenReturn(new DefaultQueryResultId(
        NAMESPACE + METRIC_L, NAMESPACE + METRIC_L));
    results.add(result);
    
    TimeSeries capital_id = mock(TimeSeries.class);
    when(capital_id.id()).thenReturn(BaseTimeSeriesStringId.newBuilder()
        .setAlias(ALIAS_L)
        .setNamespace(NAMESPACE)
        .setMetric(METRIC_R)
        .addTags("HostName", "web05")
        .addTags("owner", "tyrion")
        .build());
    
    result = mock(QueryResult.class);
    ts = Lists.newArrayList(capital_id);
    when(result.timeSeries()).thenReturn(ts);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_STRING_ID;
      }
    });
    when(result.dataSource()).thenReturn(new DefaultQueryResultId(
        NAMESPACE + METRIC_R, NAMESPACE + METRIC_R));
    results.add(result);
    
    Joiner joiner = new Joiner(config);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(results, 
            expression_config,
            (NAMESPACE + METRIC_L).getBytes(Const.UTF8_CHARSET), 
            (NAMESPACE + METRIC_R).getBytes(Const.UTF8_CHARSET),
            null).iterator();
    
    TimeSeries[] pair = iterator.next();
    assertEquals(L_5A, pair[0]);
    assertEquals(capital_id, pair[1]);
  }

  @Test
  public void joinByteIdRightOneTagMatchDiffJoinTag() throws Exception {
    setByteIds();
    
    JoinConfig config = (JoinConfig) JoinConfig.newBuilder()
        .setJoinType(JoinType.INNER)
        .addJoins("host", "HostName") // this is to make sure we use the right join.
        .setId(ID)
        .build();
    
    ExpressionParseNode expression_config = 
        (ExpressionParseNode) ExpressionParseNode.newBuilder()
        .setLeft("a")
        .setLeftType(OperandType.VARIABLE)
        .setLeftId(queryResultId(NAMESPACE + METRIC_L))
        .setRight("b")
        .setRightType(OperandType.VARIABLE)
        .setRightId(queryResultId(NAMESPACE + METRIC_R))
        .setExpressionOp(ExpressionOp.ADD)
        .setExpressionConfig(mock(ExpressionConfig.class))
        .setId("expression")
        .build();
    
    final List<QueryResult> results = Lists.newArrayList();
    QueryResult result = mock(QueryResult.class);
    List<TimeSeries> ts = Lists.newArrayList(L_5A);
    when(result.timeSeries()).thenReturn(ts);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(result.dataSource()).thenReturn(new DefaultQueryResultId(
        NAMESPACE + METRIC_L, NAMESPACE + METRIC_L));
    results.add(result);
    
    TimeSeries capital_id = mock(TimeSeries.class);
    when(capital_id.id()).thenReturn(
        BaseTimeSeriesByteId.newBuilder(mock(TimeSeriesDataSourceFactory.class))
          .setAlias(ALIAS_R_BYTES)
          .setNamespace(NAMESPACE_BYTES)
          .setMetric(METRIC_R_BYTES)
          .addTags("HostName".getBytes(Const.UTF8_CHARSET), WEB05)
          .addTags(OWNER, TYRION)
          .build());
    
    result = mock(QueryResult.class);
    ts = Lists.newArrayList(capital_id);
    when(result.timeSeries()).thenReturn(ts);
    when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return Const.TS_BYTE_ID;
      }
    });
    when(result.dataSource()).thenReturn(new DefaultQueryResultId(
        NAMESPACE + METRIC_R, NAMESPACE + METRIC_R));
    results.add(result);
    
    Joiner joiner = new Joiner(config);
    ByteMap<byte[]> encoded_joins = new ByteMap<byte[]>();
    encoded_joins.put(HOST, "HostName".getBytes(Const.UTF8_CHARSET));
    joiner.setEncodedJoins(encoded_joins);
    Iterator<TimeSeries[]> iterator = 
        joiner.join(results, 
            expression_config,
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_L_BYTES), 
            com.google.common.primitives.Bytes.concat(NAMESPACE_BYTES, METRIC_R_BYTES),
            null).iterator();
    
    TimeSeries[] pair = iterator.next();
    assertEquals(L_5A, pair[0]);
    assertEquals(capital_id, pair[1]);
  }
  
  QueryResult mockResult(final boolean as_strings, final TimeSeries... series) {
    List<TimeSeries> results = Lists.newArrayList(series);
    QueryResult result = mock(QueryResult.class);
    when(result.timeSeries()).thenReturn(results);
    if (as_strings) {
      when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
        @Override
        public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
          return Const.TS_STRING_ID;
        }
      });
    } else {
      when(result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
        @Override
        public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
          return Const.TS_BYTE_ID;
        }
      });
    }
    return result;
  }

  QueryResultId queryResultId(final String id) {
    return new DefaultQueryResultId(id, id);
  }
}