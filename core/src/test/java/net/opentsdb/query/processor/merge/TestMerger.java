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
package net.opentsdb.query.processor.merge;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.aggregators.ArraySumFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.NumericArrayAggregatorFactory;
import net.opentsdb.data.types.numeric.aggregators.SumFactory;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.processor.merge.Merger.Waiter;
import net.opentsdb.query.processor.merge.MergerConfig.MergeMode;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.Lists;

import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class TestMerger {
  private static final NumericInterpolatorConfig NUMERIC_CONFIG =
          (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
                  .setFillPolicy(FillPolicy.NOT_A_NUMBER)
                  .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
                  .setDataType(NumericType.TYPE.toString())
                  .build();

  private static QueryResultId ID1 = new DefaultQueryResultId("ha1_m1", "m1_s1");
  private static QueryResultId ID2 = new DefaultQueryResultId("ha2_m1", "m1_s2");
  private static QueryResultId ID3 = new DefaultQueryResultId("ha3_m1", "m1_s3");

  private QueryPipelineContext context;
  private QueryNodeFactory factory;
  private QueryNode upstream;
  private static TSDB MOCK_TSDB;
  private static List<MockTimeout> TIMEOUTS;

  @BeforeClass
  public static void beforeClass() throws Exception {
    MOCK_TSDB = mock(TSDB.class);
    HashedWheelTimer mockTimer;
    mockTimer = mock(HashedWheelTimer.class);
    when(MOCK_TSDB.getMaintenanceTimer()).thenReturn(mockTimer);
    TIMEOUTS = Lists.newArrayList();

    Registry registry = mock(Registry.class);
    when(MOCK_TSDB.getRegistry()).thenReturn(registry);
    ArraySumFactory asf = new ArraySumFactory();
    asf.initialize(MOCK_TSDB, null).join();
    when(registry.getPlugin(NumericArrayAggregatorFactory.class, "sum"))
            .thenReturn(asf);
    SumFactory sf = new SumFactory();
    sf.initialize(MOCK_TSDB, null).join();
    when(registry.getPlugin(NumericAggregatorFactory.class, "sum"))
            .thenReturn(sf);

    doAnswer(new Answer() {
      @Override
      public Object answer(InvocationOnMock invocation) throws Throwable {
        MockTimeout to = new MockTimeout();
        TIMEOUTS.add(to);
        to.task = (TimerTask) invocation.getArguments()[0];
        to.time = (long) invocation.getArguments()[1];
        to.timeUnit = (TimeUnit) invocation.getArguments()[2];
        to.timeout = mock(Timeout.class);
        return to.timeout;
      }
    }).when(mockTimer).newTimeout(any(TimerTask.class), anyLong(), any(TimeUnit.class));
  }

  @Before
  public void before() throws Exception {
    context = mock(QueryPipelineContext.class);
    factory = new MergerFactory();
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    when(context.tsdb()).thenReturn(MOCK_TSDB);
    when(context.queryContext()).thenReturn(mock(QueryContext.class));

    TIMEOUTS.clear();
  }
  
  @Test
  public void ctorAndInitialize() throws Exception {
    MergerConfig config = setConfig(false);

    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();
    assertSame(config, merger.config());
    assertEquals(2, merger.results.size());

    Waiter waiter = merger.results.get(ID1);
    assertEquals(0, waiter.timeoutMillis);
    waiter = merger.results.get(ID2);
    assertEquals(0, waiter.timeoutMillis);

    // w timeouts
    config = setConfig(true);

    merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();
    assertSame(config, merger.config());
    assertEquals(2, merger.results.size());

    waiter = merger.results.get(ID1);
    assertEquals(60_000, waiter.timeoutMillis);
    waiter = merger.results.get(ID2);
    assertEquals(120_000, waiter.timeoutMillis);

    assertEquals("Sum", merger.aggregatorFactory.id());
    assertEquals("Sum", merger.numericAggregatorFactory.id());

    // missmatch
    merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID3));
    try {
      merger.initialize(null).join();
      fail("Expected QueryDownstreamException");
    } catch (QueryDownstreamException e) { }

    try {
      new Merger(factory, null, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

    try {
      new Merger(factory, context, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }

  }

  @Test
  public void haOnNextNoTimeouts() throws Exception {
    MergerConfig config = setConfig(false);
    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(ID1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(ID2);

    merger.onNext(r1);
    assertSame(r1, merger.results.get(ID1).result);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, never()).onNext(any(QueryResult.class));

    merger.onNext(r2);
    assertSame(r1, merger.results.get(ID1).result);
    assertSame(r2, merger.results.get(ID2).result);
    assertEquals(0, merger.outstanding.get());
    verify(upstream, times(1)).onNext(any(MergerResult.class));
  }

  @Test
  public void haOnNexTimeoutsPrimaryFirstOK() throws Exception {
    MergerConfig config = setConfig(true);
    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(ID1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(ID2);

    merger.onNext(r1);
    Waiter waiter = merger.results.get(ID1);
    assertSame(r1, waiter.result);
    assertNull(waiter.timeout);
    waiter = merger.results.get(ID2);
    assertNull(waiter.result);
    assertNotNull(waiter.timeout);

    assertEquals(1, TIMEOUTS.size());
    assertEquals(120_000, TIMEOUTS.get(0).time);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, never()).onNext(any(QueryResult.class));

    merger.onNext(r2);
    assertSame(r1, merger.results.get(ID1).result);
    assertSame(r2, merger.results.get(ID2).result);
    assertEquals(0, merger.outstanding.get());
    verify(upstream, times(1)).onNext(any(MergerResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(TIMEOUTS.get(0).timeout, times(1)).cancel();
  }

  @Test
  public void haOnNexTimeoutsSecondaryFirstOK() throws Exception {
    MergerConfig config = setConfig(true);
    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(ID1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(ID2);

    merger.onNext(r2);
    Waiter waiter = merger.results.get(ID2);
    assertSame(r2, waiter.result);
    assertNull(waiter.timeout);
    waiter = merger.results.get(ID1);
    assertNull(waiter.result);
    assertNotNull(waiter.timeout);

    assertEquals(1, TIMEOUTS.size());
    assertEquals(60_000, TIMEOUTS.get(0).time);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, never()).onNext(any(QueryResult.class));

    merger.onNext(r1);
    assertSame(r1, merger.results.get(ID1).result);
    assertSame(r2, merger.results.get(ID2).result);
    assertEquals(0, merger.outstanding.get());
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(TIMEOUTS.get(0).timeout, times(1)).cancel();
  }

  @Test
  public void haOnNexTimeoutsPrimaryTimeout() throws Exception {
    MergerConfig config = setConfig(true);
    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(ID1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(ID2);

    merger.onNext(r2);
    Waiter waiter = merger.results.get(ID2);
    assertSame(r2, waiter.result);
    assertNull(waiter.timeout);
    waiter = merger.results.get(ID1);
    assertNull(waiter.result);
    assertNotNull(waiter.timeout);

    assertEquals(1, TIMEOUTS.size());
    assertEquals(60_000, TIMEOUTS.get(0).time);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, never()).onNext(any(QueryResult.class));

    // timeout!
    TIMEOUTS.get(0).task.run(null);
    verify(upstream, times(1)).onNext(any(MergerResult.class));
    verify(upstream, never()).onError(any(Throwable.class));

    merger.onNext(r1);
    assertNull(merger.results.get(ID1).result);
    assertSame(r2, merger.results.get(ID2).result);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, times(1)).onNext(any(MergerResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(TIMEOUTS.get(0).timeout, times(1)).cancel();
  }

  @Test
  public void haOnNexTimeoutsSecondaryTimeout() throws Exception {
    MergerConfig config = setConfig(true);
    Merger merger = new Merger(factory, context, config);
    when(context.downstreamQueryResultIds(merger))
            .thenReturn(Lists.newArrayList(ID1, ID2));
    merger.initialize(null).join();

    QueryResult r1 = mock(QueryResult.class);
    when(r1.dataSource()).thenReturn(ID1);
    QueryResult r2 = mock(QueryResult.class);
    when(r2.dataSource()).thenReturn(ID2);

    merger.onNext(r1);
    Waiter waiter = merger.results.get(ID1);
    assertSame(r1, waiter.result);
    assertNull(waiter.timeout);
    waiter = merger.results.get(ID2);
    assertNull(waiter.result);
    assertNotNull(waiter.timeout);

    assertEquals(1, TIMEOUTS.size());
    assertEquals(120_000, TIMEOUTS.get(0).time);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, never()).onNext(any(QueryResult.class));

    // timeout!
    TIMEOUTS.get(0).task.run(null);
    verify(upstream, times(1)).onNext(any(MergerResult.class));
    verify(upstream, never()).onError(any(Throwable.class));

    merger.onNext(r2);
    assertSame(r1, merger.results.get(ID1).result);
    assertNull(merger.results.get(ID2).result);
    assertEquals(1, merger.outstanding.get());
    verify(upstream, times(1)).onNext(any(MergerResult.class));
    verify(upstream, never()).onError(any(Throwable.class));
    verify(TIMEOUTS.get(0).timeout, times(1)).cancel();
  }

//  @Test
//  public void splitOnNexTimeoutsOK() throws Exception {
//    MergerConfig config = MergerConfig.newBuilder()
//            .setAggregator("sum")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setMode(MergeMode.SPLIT)
//            .setDataSource("m1")
//            .setSortedDataSources(Lists.newArrayList(ID1, ID2, ID3))
//            .setTimeouts(Lists.newArrayList("15s", "30s", "1m"))
//            .setId("merger")
//            .build();
//
//    Merger merger = new Merger(factory, context, config);
//    when(context.downstreamQueryResultIds(merger))
//            .thenReturn(Lists.newArrayList(ID1, ID2, ID3));
//    merger.initialize(null).join();
//
//    QueryResult r1 = mock(QueryResult.class);
//    when(r1.dataSource()).thenReturn(ID1);
//    QueryResult r2 = mock(QueryResult.class);
//    when(r2.dataSource()).thenReturn(ID2);
//    QueryResult r3 = mock(QueryResult.class);
//    when(r3.dataSource()).thenReturn(ID3);
//
//    merger.onNext(r1);
//    Waiter waiter = merger.results.get(ID1);
//    assertSame(r1, waiter.result);
//    assertNull(waiter.timeout);
//    waiter = merger.results.get(ID2);
//    assertNull(waiter.result);
//    assertNotNull(waiter.timeout);
//    waiter = merger.results.get(ID3);
//    assertNull(waiter.result);
//    assertNotNull(waiter.timeout);
//
//    assertEquals(2, TIMEOUTS.size());
//    // order is indeterminate.
//    List<Long> timeouts = Lists.newArrayList(TIMEOUTS.get(0).time,
//                                             TIMEOUTS.get(1).time);
//    assertTrue(timeouts.contains(30_000L));
//    assertTrue(timeouts.contains(60_000L));
//    assertEquals(2, merger.outstanding.get());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//
//    merger.onNext(r2);
//    waiter = merger.results.get(ID2);
//    assertSame(r2, waiter.result);
//    assertNull(waiter.timeout);
//    assertEquals(1, merger.outstanding.get());
//    verify(upstream, never()).onNext(any(QueryResult.class));
//
//    merger.onNext(r3);
//    waiter = merger.results.get(ID3);
//    assertSame(r3, waiter.result);
//    assertNull(waiter.timeout);
//    verify(upstream, times(1)).onNext(any(MergerResult.class));
//    verify(TIMEOUTS.get(0).timeout, times(1)).cancel();
//    verify(TIMEOUTS.get(1).timeout, times(1)).cancel();
//  }

//  @Test
//  public void splitOnNexTimeout() throws Exception {
//    MergerConfig config = MergerConfig.newBuilder()
//            .setAggregator("sum")
//            .addInterpolatorConfig(NUMERIC_CONFIG)
//            .setMode(MergeMode.SPLIT)
//            .setDataSource("m1")
//            .setSortedDataSources(Lists.newArrayList(ID1, ID2, ID3))
//            .setTimeouts(Lists.newArrayList("15s", "30s", "1m"))
//            .setId("merger")
//            .build();
//
//    Merger merger = new Merger(factory, context, config);
//    when(context.downstreamQueryResultIds(merger))
//            .thenReturn(Lists.newArrayList(ID1, ID2, ID3));
//    merger.initialize(null).join();
//
//    QueryResult r1 = mock(QueryResult.class);
//    when(r1.dataSource()).thenReturn(ID1);
//    QueryResult r2 = mock(QueryResult.class);
//    when(r2.dataSource()).thenReturn(ID2);
//    QueryResult r3 = mock(QueryResult.class);
//    when(r3.dataSource()).thenReturn(ID3);
//
//    merger.onNext(r3);
//    assertEquals(2, TIMEOUTS.size());
//
//    // timeout!
//    TIMEOUTS.get(0).task.run(null);
//    verify(upstream, never()).onNext(any(MergerResult.class));
//    verify(upstream, times(1)).onError(any(QueryDownstreamException.class));
//  }

  MergerConfig setConfig(boolean timeouts) {
    MergerConfig.Builder builder = MergerConfig.newBuilder()
            .setAggregator("sum")
            .addInterpolatorConfig(NUMERIC_CONFIG)
            .setMode(MergeMode.HA)
            .setDataSource("m1")
            .setSortedDataSources(Lists.newArrayList(ID1.dataSource(), ID2.dataSource()))
            .setId("merger");
    if (timeouts) {
      builder.setTimeouts(Lists.newArrayList("1m", "2m"));
    }
    return builder.build();
  }

  static class MockTimeout {
    TimerTask task;
    Timeout timeout;
    long time;
    TimeUnit timeUnit;
  }
}
