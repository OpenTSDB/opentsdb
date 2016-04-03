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
import static org.junit.Assert.assertNotNull;
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

import com.stumbleupon.async.Deferred;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSQuery.class })
public class TestAbsolute {

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
  private Absolute func;
  
  @Before
  public void before() throws Exception {
    view = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 1, 1);
    data_query = mock(TSQuery.class);
    when(data_query.startTime()).thenReturn(START_TIME);
    when(data_query.endTime()).thenReturn(START_TIME + (INTERVAL * NUM_POINTS));
    
    dps = PowerMockito.mock(DataPoints.class);
    when(dps.iterator()).thenReturn(view);
    when(dps.metricNameAsync()).thenReturn(Deferred.fromResult(METRIC));
    
    group_bys = new DataPoints[] { dps };
    
    query_results = new ArrayList<DataPoints[]>(1);
    query_results.add(group_bys);
    
    params = new ArrayList<String>(1);
    func = new Absolute();
  }
  
  @Test
  public void evaluatePositiveGroupByLong() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
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
  public void evaluatePositiveGroupByDouble() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, false, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
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
      v += 1;
    }
  }
  
  @Test
  public void evaluateFactorNegativeGroupByLong() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, -10, -1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
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
  public void evaluateNegativeGroupByDouble() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, false, -10, -1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
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
      v += 1;
    }
  }
  
  @Test
  public void evaluateNegativeSubQuerySeries() throws Exception {
    params.add("1");
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, -10, -1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
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
  
  @Test
  public void evaluateNullParams() throws Exception {
    assertNotNull(func.evaluate(data_query, query_results, null));
  }

  @Test
  public void evaluateEmptyResults() throws Exception {
    final DataPoints[] results = func.evaluate(data_query, 
        Collections.<DataPoints[]>emptyList(), params);
    assertEquals(0, results.length);
  }
  
  @Test
  public void evaluateEmptyParams() throws Exception {
    assertNotNull(func.evaluate(data_query, query_results, null));
  }
  
  @Test
  public void writeStringField() throws Exception {
    params.add("1");
    assertEquals("absolute(inner_expression)", 
        func.writeStringField(params, "inner_expression"));
    assertEquals("absolute(null)", func.writeStringField(params, null));
    assertEquals("absolute()", func.writeStringField(params, ""));
    assertEquals("absolute(inner_expression)", 
        func.writeStringField(null, "inner_expression"));
  }
}
