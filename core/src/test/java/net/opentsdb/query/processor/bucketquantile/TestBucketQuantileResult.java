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

import static net.opentsdb.query.processor.bucketquantile.TestBucketQuantileNumericArrayProcessor.assertTimeSeriesId;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.MockTSDBDefault;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.types.numeric.NumericArrayTimeSeries;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesQuery;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryFillPolicy.FillWithRealPolicy;
import net.opentsdb.query.interpolation.types.numeric.NumericInterpolatorConfig;
import net.opentsdb.query.pojo.FillPolicy;

public class TestBucketQuantileResult {

  private BucketQuantile node;
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
    node = new BucketQuantile(factory, context, config);
  }
  
  @Test
  public void ctor() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    assertEquals("q", result.dataSource().nodeID());
    assertEquals("quantile", result.dataSource().dataSource());
    assertSame(node, result.source());
  }
  
  @Test
  public void addResultStringIdsAllPresent() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        mockStringSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        mockStringSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        mockStringSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      mockStringSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      mockStringSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(6, result.size()); // we had 3 percentiles but two tag sets.
    
    TimeSeries[] array = result.finalSeries()[0];
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertEquals("m_0_250", ((TimeSeriesStringId) array[1].id()).metric());
    assertEquals("m_250_500", ((TimeSeriesStringId) array[2].id()).metric());
    assertEquals("m_500_1000", ((TimeSeriesStringId) array[3].id()).metric());
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
    
    array = result.finalSeries()[1];
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertEquals("m_0_250", ((TimeSeriesStringId) array[1].id()).metric());
    assertEquals("m_250_500", ((TimeSeriesStringId) array[2].id()).metric());
    assertEquals("m_500_1000", ((TimeSeriesStringId) array[3].id()).metric());
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
  }
  
  @Test
  public void addResultStringIdsAllMetricsButGapsInSeries() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        mockStringSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        mockStringSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN"))
        //mockStringSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        //mockStringSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      mockStringSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      //mockStringSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(6, result.size()); // we had 3 percentiles but two tag sets.
    
    // TODO - order may be indeterminate.
    TimeSeries[] array = result.finalSeries()[0];
    assertEquals(ImmutableMap.of("host", "web02", "dc", "DEN"), 
        ((TimeSeriesStringId) array[0].id()).tags());
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertEquals("m_0_250", ((TimeSeriesStringId) array[1].id()).metric());
    assertEquals("m_250_500", ((TimeSeriesStringId) array[2].id()).metric());
    assertNull(array[3]);
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
    
    array = result.finalSeries()[1];
    assertEquals(ImmutableMap.of("host", "web01", "dc", "DEN"), 
        ((TimeSeriesStringId) array[0].id()).tags());
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertNull(array[1]);
    assertNull(array[2]);
    assertEquals("m_500_1000", ((TimeSeriesStringId) array[3].id()).metric());
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
  }
  
  @Test
  public void addResultStringIdsEmptyResultAndGapsInSeries() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        mockStringSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        mockStringSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN"))
        //mockStringSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        //mockStringSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        //mockStringSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      mockStringSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      //mockStringSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(6, result.size()); // we had 3 percentiles but two tag sets.
    
    // TODO - order may be indeterminate.
    TimeSeries[] array = result.finalSeries()[0];
    assertEquals(ImmutableMap.of("host", "web02", "dc", "DEN"), 
        ((TimeSeriesStringId) array[0].id()).tags());
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertEquals("m_0_250", ((TimeSeriesStringId) array[1].id()).metric());
    assertNull(array[2]);
    assertNull(array[3]);
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
    
    array = result.finalSeries()[1];
    assertEquals(ImmutableMap.of("host", "web01", "dc", "DEN"), 
        ((TimeSeriesStringId) array[0].id()).tags());
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertNull(array[1]);
    assertNull(array[2]);
    assertEquals("m_500_1000", ((TimeSeriesStringId) array[3].id()).metric());
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
  }
  
  @Test
  public void addResultStringIdsMissingMetricThreshold() throws Exception {
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
        .setMissingMetricThreshold(25)
        .addInterpolatorConfig(numeric_config)
        .setId("q")
        .build();
    node = new BucketQuantile(factory, context, config);
    String[] warning = new String[1];
    TimeSeriesQuery query = mock(TimeSeriesQuery.class);
    when(query.isWarnEnabled()).thenReturn(true);
    when(context.query()).thenReturn(query);
    QueryContext qc = mock(QueryContext.class);
    doAnswer(new Answer<Void>() {
      @Override
      public Void answer(InvocationOnMock invocation) throws Throwable {
        assertSame(node, invocation.getArguments()[0]);
        warning[0] = (String) invocation.getArguments()[1];
        return null;
      }
    }).when(qc).logWarn(any(QueryNode.class), anyString());
    when(context.queryContext()).thenReturn(qc);
    
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        mockStringSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        mockStringSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN"))
        //mockStringSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        //mockStringSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        mockStringSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      mockStringSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
      //mockStringSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
      mockStringSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(3, result.size()); // we had 3 percentiles but two tag sets.
    
    // TODO - order may be indeterminate.
    TimeSeries[] array = result.finalSeries()[0];
    assertEquals(ImmutableMap.of("host", "web02", "dc", "DEN"), 
        ((TimeSeriesStringId) array[0].id()).tags());
    assertEquals(5, array.length);
    assertEquals("m_under", ((TimeSeriesStringId) array[0].id()).metric());
    assertEquals("m_0_250", ((TimeSeriesStringId) array[1].id()).metric());
    assertEquals("m_250_500", ((TimeSeriesStringId) array[2].id()).metric());
    assertNull(array[3]);
    assertEquals("m_over", ((TimeSeriesStringId) array[4].id()).metric());
    
    assertNull(result.finalSeries()[1]);
    assertTrue(warning[0].contains("Removing time series with 2 missing metrics "
        + "out of 5: alias=null, namespace=null, metric=m_under, "
        + "tags={host=web01, dc=DEN}"));
  }
  
  @Test
  public void getInOrder() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        generateArrayTimeSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        generateArrayTimeSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(6, result.size()); // we had 3 percentiles but two tag sets.
    
    Iterator<TimeSeries> iterator = result.timeSeries().iterator();
    TimeSeries ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.000", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.900", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
  
    ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.000", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.900", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
  
    ts = iterator.next();
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    assertFalse(iterator.hasNext());
  }
  
  @Test
  public void getOutOfOrder() throws Exception {
    BucketQuantileResult result = new BucketQuantileResult(node);
    
    // receiving order doesn't matter, we'll sort on the proper bucket order.
    QueryResult mock_result = mockResult(Const.TS_STRING_ID);
    List<TimeSeries> series = Lists.newArrayList(
        generateArrayTimeSeries("m_over", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_over", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);

    series = Lists.newArrayList(
        generateArrayTimeSeries("m_500_1000", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_500_1000", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_250_500", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_250_500", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_under", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_under", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    series = Lists.newArrayList(
        generateArrayTimeSeries("m_0_250", ImmutableMap.of("host", "web01", "dc", "DEN")),
        generateArrayTimeSeries("m_0_250", ImmutableMap.of("host", "web02", "dc", "DEN"))
    );
    when(mock_result.timeSeries()).thenReturn(series);
    result.addResult(mock_result);
    
    assertEquals(2, result.map().size());
    // finish
    result.finishSetup();
    assertEquals(6, result.size()); // we had 3 percentiles but two tag sets.
    
    TimeSeries ts = result.get(2);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    ts = result.get(0);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.000", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
  
    ts = result.get(4);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.900", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    // again
    ts = result.get(0);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.000", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());

    ts = result.get(1);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.900", "host", "web02", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
    
    ts = result.get(5);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.990", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
  
    ts = result.get(3);
    assertTimeSeriesId(ts.id(), config.getAs(), ImmutableMap.of(
        BucketQuantileFactory.QUANTILE_TAG, "99.000", "host", "web01", "dc", "DEN"));
    assertTrue(ts.iterator(NumericArrayType.TYPE).get().hasNext());
  }
  
  QueryResult mockResult(final TypeToken<? extends TimeSeriesId> id_type) {
    QueryResult mock_result = mock(QueryResult.class);
    when(mock_result.idType()).thenAnswer(new Answer<TypeToken<?>>() {
      @Override
      public TypeToken<?> answer(InvocationOnMock invocation) throws Throwable {
        return id_type;
      }
    });
    return mock_result;
  }
  
  TimeSeries mockStringSeries(final String metric, final Map<String, String> tags) {
    TimeSeries series = mock(TimeSeries.class);
    TimeSeriesStringId id = BaseTimeSeriesStringId.newBuilder()
        .setMetric(metric)
        .setTags(tags)
        .build();
    when(series.id()).thenReturn(id);
    return series;
  }
  
  TimeSeries generateArrayTimeSeries(final String metric, final Map<String, String> tags) {
    TimeSeries ts1 = new NumericArrayTimeSeries(
        BaseTimeSeriesStringId.newBuilder()
        .setMetric(metric)
        .setTags(tags)
        .build(), new MillisecondTimeStamp(1000));
      ((NumericArrayTimeSeries) ts1).add(1);
      ((NumericArrayTimeSeries) ts1).add(1);
      ((NumericArrayTimeSeries) ts1).add(1);
      ((NumericArrayTimeSeries) ts1).add(1);
    return ts1;
  }
}
