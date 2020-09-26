// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.bucketquantile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.exceptions.QueryDownstreamException;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;
import net.opentsdb.query.processor.bucketquantile.BucketQuantile.Bucket;
import net.opentsdb.utils.UnitTestException;

public class TestBucketQuantile {

  private BucketQuantileConfig config;
  private NumericInterpolatorConfig numeric_config;
  private QueryNodeFactory factory;
  private QueryPipelineContext context;
  
  @Before
  public void before() throws Exception {
    numeric_config = 
        (NumericInterpolatorConfig) NumericInterpolatorConfig.newBuilder()
    .setFillPolicy(FillPolicy.NOT_A_NUMBER)
    .setRealFillPolicy(FillWithRealPolicy.PREFER_NEXT)
    .setDataType(NumericType.TYPE.toString())
    .build();
    
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .setOverflowMax(1024)
        .setOverflow("m4")
        .setOverflowMetric("m_over")
        .setOverflowId(new DefaultQueryResultId("m4", "m4"))
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .setUnderflow("m5")
        .setUnderflowMetric("m_under")
        .setUnderflowId(new DefaultQueryResultId("m5", "m5"))
        .setUnderflowMin(2)
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    factory = mock(QueryNodeFactory.class);
    context = mock(QueryPipelineContext.class);
    TSDB tsdb = MockTSDBDefault.getMockTSDB();
    when(context.tsdb()).thenReturn(tsdb);
  }
  
  @Test
  public void ctor() throws Exception {
    BucketQuantile node = new BucketQuantile(factory, context, config);
    assertEquals(5, node.results().size());
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m1", "m1")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m2", "m2")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m3", "m3")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m4", "m4")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m5", "m5")));
    
    assertEquals(5, node.buckets().length);
    Bucket bucket = node.buckets()[0];
    assertEquals("m_under", bucket.metric);
    assertEquals(0.0, bucket.lower, 0.001);
    assertEquals(0.0, bucket.upper, 0.001);
    assertEquals(2, bucket.report, 0.001);
    assertTrue(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[1];
    assertEquals("m_0_250", bucket.metric);
    assertEquals(0.0, bucket.lower, 0.001);
    assertEquals(250.0, bucket.upper, 0.001);
    assertEquals(125, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[2];
    assertEquals("m_250_500", bucket.metric);
    assertEquals(250.0, bucket.lower, 0.001);
    assertEquals(500.0, bucket.upper, 0.001);
    assertEquals(375, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[3];
    assertEquals("m_500_1000", bucket.metric);
    assertEquals(500.0, bucket.lower, 0.001);
    assertEquals(1000.0, bucket.upper, 0.001);
    assertEquals(750, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[4];
    assertEquals("m_over", bucket.metric);
    assertEquals(0.0, bucket.lower, 0.001);
    assertEquals(0.0, bucket.upper, 0.001);
    assertEquals(1024, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertTrue(bucket.is_overflow);
    
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .addHistogram("m2")
        .addHistogramMetric("m_250_500")
        .addHistogramId(new DefaultQueryResultId("m2", "m2"))
        .addHistogram("m3")
        .addHistogramMetric("m_500_1000")
        .addHistogramId(new DefaultQueryResultId("m3", "m3"))
        .addHistogram("m1")
        .addHistogramMetric("m_0_250")
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .addQuantile(99.9)
        .addQuantile(99.99)
        .addQuantile(99.0)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    node = new BucketQuantile(factory, context, config);
    assertEquals(3, node.results().size());
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m1", "m1")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m2", "m2")));
    assertSame(BucketQuantile.DUMMY, node.results().get(
        new DefaultQueryResultId("m3", "m3")));
    
    assertEquals(3, node.buckets().length);
    
    bucket = node.buckets()[0];
    assertEquals("m_0_250", bucket.metric);
    assertEquals(0.0, bucket.lower, 0.001);
    assertEquals(250.0, bucket.upper, 0.001);
    assertEquals(125, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[1];
    assertEquals("m_250_500", bucket.metric);
    assertEquals(250.0, bucket.lower, 0.001);
    assertEquals(500.0, bucket.upper, 0.001);
    assertEquals(375, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
    
    bucket = node.buckets()[2];
    assertEquals("m_500_1000", bucket.metric);
    assertEquals(500.0, bucket.lower, 0.001);
    assertEquals(1000.0, bucket.upper, 0.001);
    assertEquals(750, bucket.report, 0.001);
    assertFalse(bucket.is_underflow);
    assertFalse(bucket.is_overflow);
  }
  
  @Test
  public void ctorErrors() throws Exception {
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .addHistogram("m1")
        .addHistogramMetric("m_doesn't_match") // <-- bad string
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .addQuantile(99.9)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    try {
      new BucketQuantile(factory, context, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    config = (BucketQuantileConfig) BucketQuantileConfig.newBuilder()
        .setAs("quantile")
        .addHistogram("m1")
        .addHistogramMetric("m_0") // <-- missing group
        .addHistogramId(new DefaultQueryResultId("m1", "m1"))
        .addQuantile(99.9)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    
    try {
      new BucketQuantile(factory, context, config);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

  @Test
  public void onNext() throws Exception {
    QueryNode upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    
    BucketQuantile node = new BucketQuantile(factory, context, config);
    node.initialize(null).join();
    QueryResult m1 = mock(QueryResult.class);
    when(m1.dataSource()).thenReturn(new DefaultQueryResultId("m1", "m1"));
    QueryResult m2 = mock(QueryResult.class);
    when(m2.dataSource()).thenReturn(new DefaultQueryResultId("m2", "m2"));
    QueryResult m3 = mock(QueryResult.class);
    when(m3.dataSource()).thenReturn(new DefaultQueryResultId("m3", "m3"));
    QueryResult m4 = mock(QueryResult.class);
    when(m4.dataSource()).thenReturn(new DefaultQueryResultId("m4", "m4"));
    QueryResult m5 = mock(QueryResult.class);
    when(m5.dataSource()).thenReturn(new DefaultQueryResultId("m5", "m5"));
    QueryResult unexpected = mock(QueryResult.class);
    when(unexpected.dataSource()).thenReturn(new DefaultQueryResultId("unexpected", "unexpected"));
    
    node.onNext(m3);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(m1);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(m5);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(m2);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(unexpected);
    verify(upstream, never()).onNext(any(QueryResult.class));
    
    node.onNext(m4);
    verify(upstream, times(1)).onNext(any(QueryResult.class));
    
    // error
    when(m1.error()).thenReturn("Boo!");
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    node = new BucketQuantile(factory, context, config);
    node.initialize(null).join();
    node.onNext(m1);
    verify(upstream, times(1)).onError(any(QueryDownstreamException.class));
    
    // exception
    when(m2.exception()).thenReturn(new UnitTestException());
    upstream = mock(QueryNode.class);
    when(context.upstream(any(QueryNode.class)))
      .thenReturn(Lists.newArrayList(upstream));
    node = new BucketQuantile(factory, context, config);
    node.initialize(null).join();
    node.onNext(m2);
    verify(upstream, times(1)).onError(any(UnitTestException.class));
  }
}
