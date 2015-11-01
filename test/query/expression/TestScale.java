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
package net.opentsdb.query.expression;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import net.opentsdb.core.DataPoint;
import net.opentsdb.core.DataPoints;
import net.opentsdb.core.SeekableView;
import net.opentsdb.core.SeekableViewsForTest;
import net.opentsdb.core.TSQuery;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSQuery.class })
public class TestScale {

  private static long START_TIME = 1356998400000L;
  private static int INTERVAL = 60000;
  private static int NUM_POINTS = 5;
  private static String METRIC = "sys.cpu";
  
  private TSQuery data_query;
  private SeekableView view;
  private DataPoints dps;
  private DataPoints[] group_bys;
  private List<DataPoints[]> query_results;
  private List<String> params;
  private Scale func;
  
  @Before
  public void before() throws Exception {
    view = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 1, 1);
    data_query = mock(TSQuery.class);
    when(data_query.startTime()).thenReturn(START_TIME);
    when(data_query.endTime()).thenReturn(START_TIME + (INTERVAL * NUM_POINTS));
    
    dps = PowerMockito.mock(DataPoints.class);
    when(dps.iterator()).thenReturn(view);
    when(dps.metricName()).thenReturn(METRIC);
    
    group_bys = new DataPoints[] { dps };
    
    query_results = new ArrayList<DataPoints[]>(1);
    query_results.add(group_bys);
    
    params = new ArrayList<String>(1);
    func = new Scale();
  }
  
  @Test
  public void evaluateFactor1GroupByLong() throws Exception {
    params.add("1");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    long v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1;
    }
    ts = START_TIME;
    v = 10;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1;
    }
  }
  
  @Test
  public void evaluateFactor1GroupByDouble() throws Exception {
    params.add("1");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, false, 10, 1.5);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    double v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals((long)v, dp.longValue());
      ts += INTERVAL;
      v += 1;
    }
    ts = START_TIME;
    v = 10;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      v += 1.5;
    }
  }
  
  @Test
  public void evaluateFactor1point5GroupBy() throws Exception {
    params.add("1.5");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    double v = 1.5;
    for (DataPoint dp : results[0]) {
      System.out.println(dp.timestamp() + " : " + dp.toDouble());
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      v += 1.5;
    }
    ts = START_TIME;
    v = 15;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      v += 1.5;
    }
  }
  
  @Test
  public void evaluateFactor1024GroupBy() throws Exception {
    params.add("1024");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    long v = 1024;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1024;
    }
    ts = START_TIME;
    v = 10240;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1024;
    }
  }
  
  @Test
  public void evaluateFactor1SubQuerySeries() throws Exception {
    params.add("1");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    long v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1;
    }
    ts = START_TIME;
    v = 10;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v += 1;
    }
  }
  
  @Test
  public void evaluateFactor0GroupByLong() throws Exception {
    params.add("0");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(0, dp.longValue());
      ts += INTERVAL;
    }
    ts = START_TIME;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(0, dp.longValue());
      ts += INTERVAL;
    }
  }
  
  @Test
  public void evaluateFactorNegative1GroupByLong() throws Exception {
    params.add("-1");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    group_bys = new DataPoints[] { dps, dps2 };
    query_results.clear();
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    long v = -1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v -= 1;
    }
    ts = START_TIME;
    v = -10;
    for (DataPoint dp : results[1]) {
      assertEquals(ts, dp.timestamp());
      assertTrue(dp.isInteger());
      assertEquals(v, dp.longValue());
      ts += INTERVAL;
      v -= 1;
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateNullQuery() throws Exception {
    params.add("1");
    func.evaluate(null, query_results, params);
  }
  
  @Test
  public void evaluateNullResults() throws Exception {
    params.add("1");
    final DataPoints[] results = func.evaluate(data_query, null, params);
    assertEquals(0, results.length);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateNullParams() throws Exception {
    func.evaluate(data_query, query_results, null);
  }

  @Test
  public void evaluateEmptyResults() throws Exception {
    params.add("1");
    final DataPoints[] results = func.evaluate(data_query, 
        Collections.<DataPoints[]>emptyList(), params);
    assertEquals(0, results.length);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateEmptyParams() throws Exception {
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateScaleNull() throws Exception {
    params.add(null);
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateScaleEmpty() throws Exception {
    params.add("");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateScaleNotaNumber() throws Exception {
    params.add("not a number");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test
  public void writeStringField() throws Exception {
    params.add("1");
    assertEquals("scale(inner_expression)", 
        func.writeStringField(params, "inner_expression"));
    assertEquals("scale(null)", func.writeStringField(params, null));
    assertEquals("scale()", func.writeStringField(params, ""));
    assertEquals("scale(inner_expression)", 
        func.writeStringField(null, "inner_expression"));
  }
}
