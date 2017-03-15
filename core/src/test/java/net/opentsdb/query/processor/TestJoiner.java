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

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.annotation.AnnotationType;
import net.opentsdb.data.types.annotation.MockAnnotationIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.utils.Bytes.ByteMap;

public class TestJoiner {
  private JoinConfig config;
  
  @Before
  public void before() throws Exception {
    
  }
//  
//  @Test (expected = IllegalArgumentException.class)
//  public void joinUnionNullId() throws Exception {
//    config = (JoinConfig) 
//        JoinConfig.newBuilder()
//          .setJoin(Join.newBuilder()
//              .setOperator(SetOperator.UNION)
//              .setTags(Lists.newArrayList("host", "colo"))
//              .build())
//          .build();
//    final Joiner joiner = new Joiner(config);
//    final IteratorGroup group = new IteratorGroup();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(null));
//    joiner.join(group);
//  }
//  
//  @Test
//  public void joinUnionMultiType() throws Exception {
//    config = (JoinConfig) 
//        JoinConfig.newBuilder()
//          .setJoin(Join.newBuilder()
//              .setOperator(SetOperator.UNION)
//              .setTags(Lists.newArrayList("host", "colo"))
//              .build())
//          .build();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroup group = new IteratorGroup();
//    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
//        List<TimeSeriesIterator<?>>>>> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
//    Map<TimeSeriesGroupId, Map<TypeToken<?>, List<TimeSeriesIterator<?>>>> join_group
//      = joins.get(key);
//    assertEquals(2, join_group.size());
//    Map<TypeToken<?>, List<TimeSeriesIterator<?>>> join_type = 
//        join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(1, join_type.size());
//    assertNull(join_type.get(NumericType.TYPE));
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    
//    key = "cololaxhostweb02".getBytes(Const.UTF8_CHARSET);
//    join_group = joins.get(key);
//    assertEquals(2, join_group.size());
//    join_type = join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    
//    key = "colophxhostweb01".getBytes(Const.UTF8_CHARSET);
//    join_group = joins.get(key);
//    assertEquals(2, join_group.size());
//    join_type = join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(1, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertNull(join_type.get(AnnotationType.TYPE));
//  }
//  
//  @Test
//  public void joinUnionOneSeries() throws Exception {
//    config = (JoinConfig) 
//        JoinConfig.newBuilder()
//          .setJoin(Join.newBuilder()
//              .setOperator(SetOperator.UNION)
//              .setTags(Lists.newArrayList("host", "colo"))
//              .build())
//          .build();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroup group = new IteratorGroup();
//    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    
//    final ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
//        List<TimeSeriesIterator<?>>>>> joins = joiner.join(group);
//    
//    assertEquals(1, joins.size());
//    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
//    Map<TimeSeriesGroupId, Map<TypeToken<?>, List<TimeSeriesIterator<?>>>> join_group
//      = joins.get(key);
//    assertEquals(1, join_group.size());
//    Map<TypeToken<?>, List<TimeSeriesIterator<?>>> join_type = 
//        join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(1, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//  }
//  
//  @Test
//  public void joinIntersectionMultiType() throws Exception {
//    config = (JoinConfig) 
//        JoinConfig.newBuilder()
//          .setJoin(Join.newBuilder()
//              .setOperator(SetOperator.INTERSECTION)
//              .setTags(Lists.newArrayList("host", "colo"))
//              .build())
//          .build();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroup group = new IteratorGroup();
//    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    //group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web02")
//        .addTags("colo", "lax")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//    
//    id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "phx")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    group.addSeries(new SimpleStringGroupId("a"), new MockAnnotationIterator(id));
//    group.addSeries(new SimpleStringGroupId("b"), new MockNumericIterator(id));
//    //group.addSeries(new SimpleStringGroupId("b"), new MockAnnotationIterator(id));
//
//    final ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
//        List<TimeSeriesIterator<?>>>>> joins = joiner.join(group);
//    
//    assertEquals(3, joins.size());
//    byte[] key = "cololaxhostweb01".getBytes(Const.UTF8_CHARSET);
//    Map<TimeSeriesGroupId, Map<TypeToken<?>, List<TimeSeriesIterator<?>>>> join_group
//      = joins.get(key);
//    assertEquals(2, join_group.size());
//    Map<TypeToken<?>, List<TimeSeriesIterator<?>>> join_type = 
//        join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(1, join_type.size());
//    assertNull(join_type.get(NumericType.TYPE));
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(1, join_type.size());
//    assertNull(join_type.get(NumericType.TYPE));
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    
//    key = "cololaxhostweb02".getBytes(Const.UTF8_CHARSET);
//    join_group = joins.get(key);
//    assertEquals(2, join_group.size());
//    join_type = join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(2, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertEquals(1, join_type.get(AnnotationType.TYPE).size());
//    
//    key = "colophxhostweb01".getBytes(Const.UTF8_CHARSET);
//    join_group = joins.get(key);
//    assertEquals(2, join_group.size());
//    join_type = join_group.get(new SimpleStringGroupId("a"));
//    assertEquals(1, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertNull(join_type.get(AnnotationType.TYPE));
//    join_type = join_group.get(new SimpleStringGroupId("b"));
//    assertEquals(1, join_type.size());
//    assertEquals(1, join_type.get(NumericType.TYPE).size());
//    assertNull(join_type.get(AnnotationType.TYPE));
//  }
//  
//  @Test (expected = UnsupportedOperationException.class)
//  public void joinUnsupportedJoin() throws Exception {
//    config = (JoinConfig) 
//        JoinConfig.newBuilder()
//          .setJoin(Join.newBuilder()
//              .setOperator(SetOperator.CROSS)
//              .setTags(Lists.newArrayList("host", "colo"))
//              .build())
//          .build();
//    final Joiner joiner = new Joiner(config);
//    
//    final IteratorGroup group = new IteratorGroup();
//    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
//        .addTags("host", "web01")
//        .addTags("colo", "lax")
//        .build();
//    group.addSeries(new SimpleStringGroupId("a"), new MockNumericIterator(id));
//    joiner.join(group);
//
//  }
  
  @Test (expected = IllegalArgumentException.class)
  public void joinKeyNull() throws Exception {
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .build())
          .build();
    final Joiner joiner = new Joiner(config);
    joiner.joinKey(null);
  }
  
  @Test
  public void joinKeyJoinTagsInTags() throws Exception {
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .setIncludeAggTags(false)
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setTags(Lists.newArrayList("host", "colo", "dept"))
              .setIncludeDisjointTags(false)
              .build())
          .build();
    
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
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .build())
          .build();
    Joiner joiner = new Joiner(config);
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addTags("host", "web01")
        .addAggregatedTag("colo")
        .addDisjointTag("owner")
        .build();
    
    assertEquals("hostweb01coloowner", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
    
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setIncludeAggTags(false)
              .build())
          .build();
    joiner = new Joiner(config);
    assertEquals("hostweb01owner", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
    
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .setIncludeAggTags(false)
              .setIncludeDisjointTags(false)
              .build())
          .build();
    joiner = new Joiner(config);
    assertEquals("hostweb01", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
  
  @Test
  public void joinKeyEmpty() throws Exception {
    config = (JoinConfig) 
        JoinConfig.newBuilder()
          .setJoin(Join.newBuilder()
              .setOperator(SetOperator.UNION)
              .build())
          .build();
    Joiner joiner = new Joiner(config);
    
    final TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addMetric("sys.cpu.user")
        .build();
    
    assertEquals("", 
        new String(joiner.joinKey(id), Const.UTF8_CHARSET));
  }
}
