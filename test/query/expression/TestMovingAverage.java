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
import static org.junit.Assert.fail;
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
public class TestMovingAverage {
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
  private MovingAverage func;
  
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
    func = new MovingAverage();
  }
  
  @Test
  public void evaluateWindow1dps() throws Exception {
    params.add("1");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      v += 1;
    }
  }
  
  @Test
  public void evaluateWindow2dps() throws Exception {
    params.add("2");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (v < 1) {
        v = 1.5;
      } else {
        v += 1;
      }
    }
  }

  @Test
  public void evaluateWindow5dps() throws Exception {
    params.add("5");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (ts == 1356998640000L) {
        v = 3.0;
      }
    }
  }
  
  @Test
  public void evaluateWindow6dps() throws Exception {
    params.add("6");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
    }
  }

  @Test
  public void evaluateWindow1min() throws Exception {
    params.add("'1min'");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (v < 1) {
        v = 2;
      } else {
        v += 1;
      }
    }
  }
  
  @Test
  public void evaluateWindow2min() throws Exception {
    params.add("'2min'");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (ts == 1356998520000L) {
        v = 2.5;
      } else if (v > 0) {
        v += 1;
      }
    }
  }
  
  @Test
  public void evaluateWindow3min() throws Exception {
    params.add("'3min'");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      System.out.println(dp.timestamp() + " : " + dp.doubleValue());
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (ts == 1356998580000L) {
        v = 3;
      } else if (v > 0) {
        v += 1;
      }
    }
  }
  
  @Test
  public void evaluateWindow4min() throws Exception {
    params.add("'4min'");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
      if (ts == 1356998640000L) {
        v = 3.5;
      }
    }
  }
  
  @Test
  public void evaluateWindow5min() throws Exception {
    params.add("'5min'");
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(1, results.length);
    assertEquals(METRIC, results[0].metricName());
    
    long ts = START_TIME;
    double v = 0;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
      ts += INTERVAL;
    }
  }

  @Test
  public void evaluateGroupBy() throws Exception {
    params.add("1");
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
    double v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
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
  public void evaluateSubQuery() throws Exception {
    params.add("1");
    
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricNameAsync()).thenReturn(Deferred.fromResult("sys.mem"));
    DataPoints[] group_bys2 = new DataPoints[] { dps2 };
    query_results.add(group_bys2);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);
    
    assertEquals(2, results.length);
    assertEquals(METRIC, results[0].metricName());
    assertEquals("sys.mem", results[1].metricName());
    
    long ts = START_TIME;
    double v = 1;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertFalse(dp.isInteger());
      assertEquals(v, dp.doubleValue(), 0.001);
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
  public void evaluateWindowIsZeroDataPoints() throws Exception {
    params.add("0");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowIsZeroTime() throws Exception {
    params.add("'0sec'");
    func.evaluate(data_query, query_results, params);
  }

  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowTimedMissingQuotes() throws Exception {
    params.add("60sec");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowNull() throws Exception {
    params.add(null);
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowEmpty() throws Exception {
    params.add("");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowUnknown() throws Exception {
    params.add("somethingelse");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void evaluateWindowNotFirstParam() throws Exception {
    params.add("somethingelse");
    params.add("60");
    func.evaluate(data_query, query_results, params);
  }
  
  @Test
  public void writeStringField() throws Exception {
    params.add("1");
    assertEquals("movingAverage(inner_expression)", 
        func.writeStringField(params, "inner_expression"));
    assertEquals("movingAverage(null)", func.writeStringField(params, null));
    assertEquals("movingAverage()", func.writeStringField(params, ""));
    assertEquals("movingAverage(inner_expression)", 
        func.writeStringField(null, "inner_expression"));
  }

  @Test
  public void parseParam() throws Exception {
    // second
    assertEquals(1000, func.parseParam("'1sec'"));
    assertEquals(1000, func.parseParam("'1s'"));
    assertEquals(5000, func.parseParam("'5sec'"));
    assertEquals(5000, func.parseParam("'5s'"));
    
    // minute
    assertEquals(60000, func.parseParam("'1min'"));
    assertEquals(60000, func.parseParam("'1m'"));
    assertEquals(300000, func.parseParam("'5min'"));
    assertEquals(300000, func.parseParam("'5m'"));
    
    // hour
    assertEquals(3600000, func.parseParam("'1hr"));
    assertEquals(3600000, func.parseParam("'1h'"));
    assertEquals(3600000, func.parseParam("'1hour'"));
    assertEquals(18000000, func.parseParam("'5hr'"));
    assertEquals(18000000, func.parseParam("'5h'"));
    assertEquals(18000000, func.parseParam("'5hour'"));
    
    // day
    assertEquals(86400000, func.parseParam("'1day'"));
    assertEquals(86400000, func.parseParam("'1d'"));
    assertEquals(432000000, func.parseParam("'5day'"));
    assertEquals(432000000, func.parseParam("'5d'"));
    
    // TODO - fix it, closing with a 1 seems to work instead of a '
    assertEquals(1000, func.parseParam("'1sec1"));
    
    // missing quotes
    try {
      func.parseParam("'1sec");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      func.parseParam("1sec'");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      func.parseParam("1sec");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // no numbers or units
    try {
      func.parseParam("'sec'");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      func.parseParam("'60'");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // null or empty or short
    try {
      func.parseParam(null);
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      func.parseParam("");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    try {
      func.parseParam("'");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
    
    // floating point
    try {
      func.parseParam("'1.5sec'");
      fail("Expected an IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
