// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.common.Const;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestJoiner {
  private ExpressionProcessorConfig config;
  private Expression.Builder expression_builder;
  private Join.Builder join_builder;
  
  @Before
  public void before() throws Exception {
    expression_builder = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b");
    
    join_builder = Join.newBuilder()
        .setOperator(SetOperator.UNION)
        .setTags(Lists.newArrayList("host", "colo"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void joinUnionNullId() throws Exception {
    setConfig();
    final Joiner joiner = new Joiner(config);
    final IteratorGroups group = new DefaultIteratorGroups();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(null));
    joiner.join(group);
  }
  
  @Test
  public void joinUnionMultiType() throws Exception {
    setConfig();
    final Joiner joiner = new Joiner(config);
    
    final IteratorGroups group = new DefaultIteratorGroups();
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "phx")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));

    final ByteMap<IteratorGroups> joins = joiner.join(group);
    
    assertEquals(3, joins.size());
    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
    IteratorGroups join_group = joins.get(key);
    assertEquals(3, join_group.flattenedIterators().size());
    IteratorGroup its = 
        join_group.group(new SimpleStringGroupId("a"));
    assertEquals(1, its.flattenedIterators().size());
    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
    assertTrue(its.iterators(NumericType.TYPE).isEmpty());
    its = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(2, its.flattenedIterators().size());
    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
    assertEquals(1, its.iterators(NumericType.TYPE).size());
    
    key = "cololaxhostweb02".getBytes(Const.UTF8_CHARSET);
    join_group = joins.get(key);
    assertEquals(4, join_group.flattenedIterators().size());
    its = join_group.group(new SimpleStringGroupId("a"));
    assertEquals(2, its.flattenedIterators().size());
    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
    assertEquals(1, its.iterators(NumericType.TYPE).size());
    its = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(2, its.flattenedIterators().size());
    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
    assertEquals(1, its.iterators(NumericType.TYPE).size());
    
    key = "colophxhostweb01".getBytes(Const.UTF8_CHARSET);
    join_group = joins.get(key);
    assertEquals(3, join_group.flattenedIterators().size());
    its = join_group.group(new SimpleStringGroupId("a"));
    assertEquals(2, its.flattenedIterators().size());
    assertEquals(1, its.iterators(AnnotationType.TYPE).size());
    assertEquals(1, its.iterators(NumericType.TYPE).size());
    its = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(1, its.flattenedIterators().size());
    assertTrue(its.iterators(AnnotationType.TYPE).isEmpty());
    assertEquals(1, its.iterators(NumericType.TYPE).size());
  }
  
  @Test
  public void joinUnionOneSeries() throws Exception {
    setConfig();
    final Joiner joiner = new Joiner(config);
    
    final IteratorGroups group = new DefaultIteratorGroups();
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    
    final ByteMap<IteratorGroups> joins = joiner.join(group);
    
    assertEquals(1, joins.size());
    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
    IteratorGroups join_group = joins.get(key);
    assertEquals(1, join_group.flattenedIterators().size());
    IteratorGroup join_types = 
        join_group.group(new SimpleStringGroupId("a"));
    assertEquals(1, join_types.flattenedIterators().size());
    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
    assertNull(join_group.group(new SimpleStringGroupId("b")));
  }
  
  @Test
  public void joinIntersectionMultiType() throws Exception {
    join_builder.setOperator(SetOperator.INTERSECTION);
    setConfig();
    final Joiner joiner = new Joiner(config);
    
    final IteratorGroups group = new DefaultIteratorGroups();
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web02")
        .addTags("colo", "lax")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
    
    id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "phx")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    group.addIterator(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
    group.addIterator(new SimpleStringGroupId("b"), new MockNumericIterator(id));
    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));

    final ByteMap<IteratorGroups> joins = joiner.join(group);
    
    assertEquals(3, joins.size());
    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
    IteratorGroups join_group = joins.get(key);
    assertEquals(2, join_group.flattenedIterators().size());
    IteratorGroup join_types = 
        join_group.group(new SimpleStringGroupId("a"));
    assertEquals(1, join_types.flattenedIterators().size());
    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
    join_types = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(1, join_types.flattenedIterators().size());
    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
    assertTrue(join_types.iterators(NumericType.TYPE).isEmpty());
    
    key = "cololaxhostweb02".getBytes(Const.UTF8_CHARSET);
    join_group = joins.get(key);
    assertEquals(4, join_group.flattenedIterators().size());
    join_types = join_group.group(new SimpleStringGroupId("a"));
    assertEquals(2, join_types.flattenedIterators().size());
    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
    join_types = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(2, join_types.flattenedIterators().size());
    assertEquals(1, join_types.iterators(AnnotationType.TYPE).size());
    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
    
    key = "colophxhostweb01".getBytes(Const.UTF8_CHARSET);
    join_group = joins.get(key);
    assertEquals(2, join_group.flattenedIterators().size());
    join_types = join_group.group(new SimpleStringGroupId("a"));
    assertEquals(1, join_types.flattenedIterators().size());
    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
    join_types = join_group.group(new SimpleStringGroupId("b"));
    assertEquals(1, join_types.flattenedIterators().size());
    assertTrue(join_types.iterators(AnnotationType.TYPE).isEmpty());
    assertEquals(1, join_types.iterators(NumericType.TYPE).size());
  }
  
  @Test (expected = UnsupportedOperationException.class)
  public void joinUnsupportedJoin() throws Exception {
    join_builder.setOperator(SetOperator.CROSS);
    setConfig();
    final Joiner joiner = new Joiner(config);
    
    final IteratorGroups group = new DefaultIteratorGroups();
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .build();
    group.addIterator(new SimpleStringGroupId("a"), new MockNumericIterator(id));
    joiner.join(group);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void joinKeyNull() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
    setConfig();
    final Joiner joiner = new Joiner(config);
    joiner.joinKey(null);
  }
  
  @Test
  public void joinKeyJoinTagsInTags() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("colo", "lax")
        .addTags("dept", "KingsGuard")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertEquals("cololaxdeptKingsGuardhostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyJoinTagsOneAgg() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("dept", "KingsGuard")
        .addAggregatedTag("colo")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertEquals("colodeptKingsGuardhostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyJoinTagsOneDisjoint() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addTags("dept", "KingsGuard")
        .addDisjointTag("colo")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertEquals("colodeptKingsGuardhostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyJoinTagsOneAggOneDisjoint() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"));
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .addDisjointTag("dept")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertEquals("colodepthostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingAgg() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
      .setIncludeAggTags(false);
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .addDisjointTag("dept")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertNull(joiner.joinKey(id));
  }
  
  @Test
  public void joinKeyJoinTagsOneAggOneDisjointNotIncludingDisjoint() throws Exception {
    join_builder.setTags(Lists.newArrayList("host", "colo", "dept"))
      .setIncludeDisjointTags(false);
    setConfig();
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .addDisjointTag("dept")
        .build();
    
    final Joiner joiner = new Joiner(config);
    assertNull(joiner.joinKey(id));
  }

  @Test
  public void joinKeyFullJoin() throws Exception {
    join_builder = Join.newBuilder()
        .setOperator(SetOperator.UNION);
    setConfig();
    Joiner joiner = new Joiner(config);
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .addDisjointTag("owner")
        .build();
    
    assertEquals("hostweb01coloowner", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
    
    join_builder = Join.newBuilder()
        .setOperator(SetOperator.UNION)
        .setIncludeAggTags(false);
    setConfig();
    joiner = new Joiner(config);
    assertEquals("hostweb01owner", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
    
    join_builder = Join.newBuilder()
        .setOperator(SetOperator.UNION)
        .setIncludeAggTags(false)
        .setIncludeDisjointTags(false);
    setConfig();
    joiner = new Joiner(config);
    assertEquals("hostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyEmpty() throws Exception {
    join_builder = Join.newBuilder()
        .setOperator(SetOperator.UNION);
    setConfig();
    Joiner joiner = new Joiner(config);
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addMetric("sys.cpu.user")
        .build();
    
    assertEquals("", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  private void setConfig() {
    if (join_builder != null) {
      expression_builder.setJoin(join_builder.build());
    }
    
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression_builder.build())
          .build();
  }
}
