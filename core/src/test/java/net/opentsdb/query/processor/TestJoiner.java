// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericTimeSeries;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;

public class TestJoiner {
  private ExpressionProcessorConfig config;
  private Expression.Builder expression_builder;
  private Join.Builder join_builder;
  
//  @Before
//  public void before() throws Exception {
//    expression_builder = Expression.newBuilder()
//        .setId("e1")
//        .setExpression("a + b");
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setTags(Lists.newArrayList("host", "colo"));
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void joinUnionNullId() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    final IteratorGroups group = new DefaultIteratorGroups();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(null));
//    joiner.join(group);
//  }
//  
//  @Test
//  public void joinUnionMultiType() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(3, join_group.flattenedIterators().size());
//    IteratorGroup its = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertTrue(its.iterators(NumericType.TYPE).isEmpty());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    
//    key = "cololaxhostweb02";
//    join_group = joins.get(key);
//    assertEquals(4, join_group.flattenedIterators().size());
//    its = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    
//    key = "colophxhostweb01";
//    join_group = joins.get(key);
//    assertEquals(3, join_group.flattenedIterators().size());
//    its = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, its.flattenedIterators().size());
//    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//    its = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, its.flattenedIterators().size());
//    assertTrue(its.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, its.iterators(NumericType.TYPE).size());
//  }
//  
//  @Test
//  public void joinUnionOneSeries() throws Exception {
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(1, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(1, join_group.flattenedIterators().size());
//    IteratorGroup join_types = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    assertNull(join_group.group(new SimpleStringGroupId("b")));
//  }
//  
//  @Test
//  public void joinIntersectionMultiType() throws Exception {
//    join_builder.setOperator(SetOperator.INTERSECTION);
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final Map<String, IteratorGroups> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    String key = "cololaxhostweb01";
//    IteratorGroups join_group = joins.get(key);
//    assertEquals(2, join_group.flattenedIterators().size());
//    IteratorGroup join_types = 
//        join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
//    
//    key = "cololaxhostweb02";
//    join_group = joins.get(key);
//    assertEquals(4, join_group.flattenedIterators().size());
//    join_types = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(2, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(2, join_types.flattenedIterators().size());
//    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    
//    key = "colophxhostweb01";
//    join_group = joins.get(key);
//    assertEquals(2, join_group.flattenedIterators().size());
//    join_types = join_group.group(new SimpleStringGroupId("a"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//    join_types = join_group.group(new SimpleStringGroupId("b"));
//    assertEquals(1, join_types.flattenedIterators().size());
//    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
//    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
//  }
//  
//  @Test (expected = UnsupportedOperationException.class)
//  public void joinUnsupportedJoin() throws Exception {
//    join_builder.setOperator(SetOperator.CROSS);
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroups group = new DefaultIteratorGroups();
//    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    joiner.join(group);
//  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void joinKeyNull() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    final Joiner joiner = new Joiner(config);
//    joiner.joinKey(null);
//  }
//  
//  @Test
//  public void joinKeyJoinTagsInTags() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .addTags("dept", "KingsGuard")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("cololaxdeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAgg() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("dept", "KingsGuard")
//        .addAggregatedTag("colo")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addTags("dept", "KingsGuard")
//        .addDisjointTag("colo")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodeptKingsGuardhostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertEquals("colodepthostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingAgg() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
//      .setIncludeAggTags(false);
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertNull(joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingDisjoint() throws Exception {
//    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
//      .setIncludeDisjointTags(false);
//    setConfig();
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("dept")
//        .build();
//    
//    final Joiner joiner = new Joiner(config);
//    assertNull(joiner.joinKey(id));
//  }
//
//  @Test
//  public void joinKeyFullJoin() throws Exception {
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION);
//    setConfig();
//    Joiner joiner = new Joiner(config);
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("Khalasar")
//        .addTags("host", "web01")
//        .addAggregatedTag("colo")
//        .addDisjointTag("owner")
//        .build();
//    
//    assertEquals("hostweb01coloowner", joiner.joinKey(id));
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setIncludeAggTags(false);
//    setConfig();
//    joiner = new Joiner(config);
//    assertEquals("hostweb01owner", joiner.joinKey(id));
//    
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION)
//        .setIncludeAggTags(false)
//        .setIncludeDisjointTags(false);
//    setConfig();
//    joiner = new Joiner(config);
//    assertEquals("hostweb01", joiner.joinKey(id));
//  }
//  
//  @Test
//  public void joinKeyEmpty() throws Exception {
//    join_builder = Join.newBuilder()
//        .setOperator(SetOperator.UNION);
//    setConfig();
//    Joiner joiner = new Joiner(config);
//    
//    final TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
//        .setMetric("sys.cpu.user")
//        .build();
//    
//    assertEquals("", joiner.joinKey(id));
//  }
//  
  private void setConfig() {
    if (join_builder != null) {
      expression_builder.setJoin(join_builder.build());
    }
    
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression_builder.build())
          .build();
  }
}
