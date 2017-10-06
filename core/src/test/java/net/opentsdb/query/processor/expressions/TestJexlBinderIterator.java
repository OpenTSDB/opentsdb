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
package net.opentsdb.query.processor.expressions;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.internal.util.reflection.Whitebox;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MockNumericIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.DefaultQueryContext;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.graph.ExecutionGraph;
import net.opentsdb.query.pojo.Expression;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.pojo.NumericFillPolicy;
import net.opentsdb.query.processor.DefaultTimeSeriesProcessor;
import net.opentsdb.query.processor.ProcessorTestsHelpers;
import net.opentsdb.query.processor.TimeSeriesProcessor;

public class TestJexlBinderIterator {
  private TSDB tsdb;
  private TimeSeriesGroupId group_id_a;
  private TimeSeriesGroupId group_id_b;
  
  private TimeSeriesId id_a;
  private TimeSeriesId id_b;
  
  private List<List<MutableNumericType>> data_a;
  private List<List<MutableNumericType>> data_b;
  
  private Map<String, NumericFillPolicy> fills;
  
  private MockNumericIterator it_a;
  private MockNumericIterator it_b;
  
  private TimeSeriesProcessor group;
  
  private ExpressionProcessorConfig config;
  private Expression expression;
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    fills = Maps.newHashMap();
    fills.put("a", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.ZERO).build());
    fills.put("b", NumericFillPolicy.newBuilder()
        .setPolicy(FillPolicy.SCALAR).setValue(-100).build());
    
    expression = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
        .setFillPolicies(fills)
        .build();
    
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression)
        .build();
    
    group_id_a = new SimpleStringGroupId("a");
    group_id_b = new SimpleStringGroupId("b");
    
    id_a = BaseTimeSeriesId.newBuilder()
        .setAlias("Khaleesi")
        .setMetric("system.cpu.user")
        .build();
    id_b = BaseTimeSeriesId.newBuilder()
        .setAlias("Khalasar")
        .setMetric("system.cpu.idle")
        .build();
    
    data_a = Lists.newArrayListWithCapacity(2);
    List<MutableNumericType> set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_a.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_a.add(set);

    data_b = Lists.newArrayListWithCapacity(2);
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(1000), 1));
    set.add(new MutableNumericType(new MillisecondTimeStamp(2000), 2));
    set.add(new MutableNumericType(new MillisecondTimeStamp(3000), 3));
    data_b.add(set);
    
    set = Lists.newArrayListWithCapacity(3);
    set.add(new MutableNumericType(new MillisecondTimeStamp(4000), 4));
    //set.add(new MutableNumericType(new MillisecondTimeStamp(5000), 5));
    set.add(new MutableNumericType(new MillisecondTimeStamp(6000), 6));
    data_b.add(set);
    
    it_a = spy(new MockNumericIterator(id_a));
    it_a.data = data_a;
    
    it_b = spy(new MockNumericIterator(id_b));
    it_b.data = data_b;
    
    context = spy(new DefaultQueryContext(tsdb, 
        mock(ExecutionGraph.class)));
    
    group = new DefaultTimeSeriesProcessor(context);
    group.addSeries(group_id_a, it_a);
    group.addSeries(group_id_b, it_b);
  }
  
  @Test
  public void ctor() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    verify(context, times(1)).register(it);
    
    new JexlBinderNumericIterator(null, config);
    
    try {
      new JexlBinderNumericIterator(context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void addIterator() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
    
    verify(context, times(1)).register(it, it_a);
    verify(context, times(1)).register(it, it_b);
    
    try {
      it.addIterator(null, it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      it.addIterator("", it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      it.addIterator("a", null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      it.addIterator("c", it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    try {
      it.addIterator("a", it_a);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
  
  @Test
  public void initialize() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
    
    assertNull(it.id());
    assertNull(it.initialize().join());
    
    //assertEquals("system.cpu.idle", it.id().metric());
    assertNull(it.id().alias());
  }
  
  @Test
  public void nextOK() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
    
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    // validate that the iterator was promoted to a sink
    assertTrue(context.iteratorSinks().contains(it));
    assertFalse(context.iteratorSinks().contains(it_a));
    assertFalse(context.iteratorSinks().contains(it_b));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(8, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(-95, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(12, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void nextNoVariableFills() throws Exception {
    expression = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR).setValue(-100).build())
        .build();
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression)
        .build();
    
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
 
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(-98, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(8, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertEquals(-95, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(12, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void nextNoFills() throws Exception {
    expression = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b")
        .build();
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression)
        .build();
    
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
    
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    assertNull(context.fetchNext().join());
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(4000, v.timestamp().msEpoch());
    assertEquals(8, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(5000, v.timestamp().msEpoch());
    assertTrue(Double.isNaN(v.value().doubleValue()));
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(6000, v.timestamp().msEpoch());
    assertEquals(12, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }

  @Test
  public void exceptionStatusOnNext() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    assertNull(it.next());
    assertEquals(IteratorStatus.EXCEPTION, context.advance());
  }
  
  @Test
  public void exceptionThrowOnNext() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
 
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    // inject an exception
    it_b.ex = new RuntimeException("Boo!");
    it_b.throw_ex = true;
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    try {
      it.next();
      fail("Expected RuntimeException");
    } catch (RuntimeException e) {
      assertSame(e, it_b.ex);
    }
    assertEquals(IteratorStatus.EXCEPTION, context.advance());
  }
  
  @Test (expected = IllegalStateException.class)
  public void missingVariable() throws Exception {
    expression = Expression.newBuilder()
        .setId("e1")
        .setExpression("a + b")
        .setFillPolicy(NumericFillPolicy.newBuilder()
            .setPolicy(FillPolicy.SCALAR).setValue(-1).build())
        .build();
    config = (ExpressionProcessorConfig) ExpressionProcessorConfig.newBuilder()
        .setExpression(expression)
        .build();
    
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);

    // since we're not part of a binder we have to manually initialize the it
    it.initialize().join();
  }
  
  public void missingVariableHasFill() throws Exception {
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    
    // since we're not part of a binder we have to manually initialize the it
    it.initialize().join();
  }
  
  @SuppressWarnings("unchecked")
  @Test
  public void getCopy() throws Exception {
    final JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
 
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    final QueryContext ctx2 = 
        new DefaultQueryContext(tsdb, mock(ExecutionGraph.class));
    final JexlBinderNumericIterator copy = 
        (JexlBinderNumericIterator) it.getShallowCopy(ctx2);
    
    // manual hack needed to initialize the cloned iterators since they're not
    // a part of a binder.
    for (final TimeSeriesIterator<?> sink : ((Map<String, TimeSeriesIterator<?>>) 
        Whitebox.getInternalState(copy, "iterators")).values()) {
      sink.initialize().join();
    }
    assertNull(copy.initialize().join());
    assertNull(ctx2.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, ctx2.advance());
    v = (TimeSeriesValue<NumericType>) copy.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertNotSame(copy, it);
  }

  @Test
  public void state1() throws Exception {
    ProcessorTestsHelpers.setState1(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
    
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(4, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state2() throws Exception {
    ProcessorTestsHelpers.setState2(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);
 
    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(1, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state3() throws Exception {
    ProcessorTestsHelpers.setState3(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(6, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state4() throws Exception {
    ProcessorTestsHelpers.setState4(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state5() throws Exception {
    ProcessorTestsHelpers.setState5(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(-99, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state6() throws Exception {
    ProcessorTestsHelpers.setState6(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(-99, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(3, v.value().doubleValue(), 0.01);

    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state7() throws Exception {
    ProcessorTestsHelpers.setState7(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(-99, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state8() throws Exception {
    ProcessorTestsHelpers.setState8(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(-98, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state9() throws Exception {
    ProcessorTestsHelpers.setState9(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(-97, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state10() throws Exception {
    ProcessorTestsHelpers.setState10(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(-99, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(-97, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
  
  @Test
  public void state11() throws Exception {
    ProcessorTestsHelpers.setState11(it_a, it_b);
    JexlBinderNumericIterator it = 
        new JexlBinderNumericIterator(context, config);
    it.addIterator("a", it_a);
    it.addIterator("b", it_b);

    // since we're not part of a binder we have to manually initialize the it
    assertNull(it.initialize().join());
    assertNull(context.initialize().join());
    
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    TimeSeriesValue<NumericType> v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(1000, v.timestamp().msEpoch());
    assertEquals(-99, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(2000, v.timestamp().msEpoch());
    assertEquals(2, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_CHUNK, context.advance());
    context.fetchNext().join();
    assertEquals(IteratorStatus.HAS_DATA, context.advance());
    v = (TimeSeriesValue<NumericType>) it.next();
    assertEquals(3000, v.timestamp().msEpoch());
    assertEquals(-97, v.value().doubleValue(), 0.01);
    
    assertEquals(IteratorStatus.END_OF_DATA, context.advance());
  }
}
