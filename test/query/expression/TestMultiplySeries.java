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

import com.sun.java_cup.internal.runtime.Scanner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({ TSQuery.class, Scanner.class })
public class TestMultiplySeries extends BaseTimeSyncedIteratorTest {

  private static long START_TIME = 1356998400000L;
  private static int INTERVAL = 60000;
  private static int NUM_POINTS = 5;
  
  private TSQuery data_query;
  private SeekableView view;
  private DataPoints dps;
  private DataPoints[] group_bys;
  private List<DataPoints[]> query_results;
  private List<String> params;
  private MultiplySeries func;
  
  @Before
  public void beforeLocal() throws Exception {
    view = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 1, 1);
    data_query = mock(TSQuery.class);
    when(data_query.startTime()).thenReturn(START_TIME);
    when(data_query.endTime()).thenReturn(START_TIME + (INTERVAL * NUM_POINTS));
    
    dps = PowerMockito.mock(DataPoints.class);
    when(dps.iterator()).thenReturn(view);
    when(dps.metricName()).thenReturn(METRIC_STRING);
    when(dps.metricUID()).thenReturn(new byte[] {0,0,1});
    
    group_bys = new DataPoints[] { dps };
    
    query_results = new ArrayList<DataPoints[]>(1);
    query_results.add(group_bys);
    
    params = new ArrayList<String>(1);
    func = new MultiplySeries(tsdb);
  }
  
  @Test
  public void multiplyOneSeriesEach() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    when(dps2.metricUID()).thenReturn(new byte[] {0,0,2});
    group_bys = new DataPoints[] { dps2 };
    query_results.add(group_bys);
    
    final DataPoints[] results = func.evaluate(data_query, query_results, params);

    assertEquals(1, results.length);
    assertEquals(METRIC_STRING, results[0].metricName());
    final int[] vals = new int[] { 10, 22, 36, 52, 70 };
    int i = 0;
    long ts = START_TIME;
    for (DataPoint dp : results[0]) {
      assertEquals(ts, dp.timestamp());
      assertEquals(vals[i++], dp.toDouble(), 0.001);
      ts += INTERVAL;
    }
  }
  
  @Test
  public void multiplyMultipleSeriesEach() throws Exception {
    oneExtraSameE();
    queryAB_Dstar();
    
    query_results.clear();
    query_results.add(results.get("1").getValue());
    query_results.add(results.get("0").getValue());
    final DataPoints[] results = func.evaluate(data_query, 
        query_results, params);

    assertEquals(3, results.length);
    
    double[][] vals = new double[2][];
    vals[0] = new double[] { 11, 24, 39 };
    vals[1] = new double[] { 56, 75, 96 };
    for (int i = 0; i < results.length; i++) {
      long ts = 1431561600000l;
      final SeekableView it = results[i].iterator();
      int x = 0;
      while (it.hasNext()) {
        final DataPoint dp = it.next();
        assertEquals(ts, dp.timestamp());
        if (i < 2) {
          assertEquals(vals[i][x++], dp.toDouble(), 0.0001);
        } else {
          assertEquals(0, dp.toDouble(), 0.0001);
        }
        ts += INTERVAL;
      }
    }
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void multiplyOneResultSet() throws Exception {    
    func.evaluate(data_query, query_results, params);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void multiplyTooManyResultSets() throws Exception {
    SeekableView view2 = SeekableViewsForTest.generator(START_TIME, INTERVAL, 
        NUM_POINTS, true, 10, 1);
    DataPoints dps2 = PowerMockito.mock(DataPoints.class);
    when(dps2.iterator()).thenReturn(view2);
    when(dps2.metricName()).thenReturn("sys.mem");
    when(dps2.metricUID()).thenReturn(new byte[] {0,0,2});
    group_bys = new DataPoints[] { dps2 };
    // doesn't matter what they are
    for (int i = 0; i < 100; i++) {
      query_results.add(group_bys);
    }
    
    func.evaluate(data_query, query_results, params);
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
  
  @Test
  public void writeStringField() throws Exception {
    params.add("1");
    assertEquals("multiplySeries(inner_expression)", 
        func.writeStringField(params, "inner_expression"));
    assertEquals("multiplySeries(null)", func.writeStringField(params, null));
    assertEquals("multiplySeries()", func.writeStringField(params, ""));
    assertEquals("multiplySeries(inner_expression)", 
        func.writeStringField(null, "inner_expression"));
  }
}
