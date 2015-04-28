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
package net.opentsdb.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

/**
 * Integration testing for the various aggregators. We write data points to 
 * MockBase and then pull them out, following the full path for a TSDB query.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ Scanner.class })
public class TestTsdbQueryAggregators extends BaseTsdbTest {
  protected TsdbQuery query = null;

  @Before
  public void beforeLocal() throws Exception {
    query = new TsdbQuery(tsdb);
  }

  @Test
  public void runZimSum() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(301, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runZimSumFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(76.25, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runZimSumOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runZimSumFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runZimSumWithMissingData() throws Exception {
    storeLongTimeSeriesWithMissingData();

    HashMap<String, String> tags = new HashMap<String, String>(0);
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.ZIMSUM, false);
    final DataPoints[] dps = query.run();
    assertNotNull(dps);
    assertEquals(METRIC_STRING, dps[0].metricName());
    assertEquals(TAGK_STRING, dps[0].getAggregatedTags().get(0));
    assertNull(dps[0].getAnnotations());
    assertTrue(dps[0].getTags().isEmpty());

    /* INPUT:
     *        t0  t1  t2  t3  t4  t5 ...
     * web01   X   2   3   X   5   6 ...
     * web02   X 299   X 297   X 295 ...
     *
     * OUTPUT:
     * zimsum  X 301   3 297   5 301 ...
     */

    int i = 0;
    long ts = 1356998400000L;
    for (final DataPoint dp : dps[0]) {
      // Every sixth position, both elements are missing, so the aggregation
      // will have a gap.
      int offset = i % 6;
      if (0 == offset) {
        // We have skipped a timestamp, so we should update the state.
        ts += 10000;
        ++i;
        ++offset;
      }

      if (1 == offset || 5 == offset) {
        // The second and last elements in each cycle should be the expected
        // value, which is 301.
        assertEquals(301, dp.longValue());
      } else if (2 == offset || 4 == offset) {
        // The third and fifth elements in each cycle should be taken from the
        // ascending series.
        assertEquals(i + 1, dp.longValue());
      } else {
        // Otherwise, the element should be taken from the descending series.
        assertEquals(300 - i, dp.longValue());
      }

      assertEquals(ts, dp.timestamp());
      ts += 10000;
      ++i;
    }

    assertEquals(250, dps[0].size());
  }
  
  @Test
  public void runMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 1;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      
      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }
      
      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 1;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (counter % 2 != 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      } else if (v == 151){
        v = 150;
        decrement = true;
        counter--; // hack since the hump is 150 150 151 150 150
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.001);
      if (decrement) {
        v -= 0.125;
      } else {
        v += 0.125;
      }
      
      if (v > 38.125){
        v = 38.125;
        decrement = true;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 300;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 75.0;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 1;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (v == 1) {
        v = 300;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 1;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
      } 
      
      if (v == 150){
        v = 151;
        decrement = false;
        counter--; // hack since the hump is 151 151 151
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), .0001);
      if (v == 1.25) {
        v = 75.0;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0.25;
      } else {
        if (decrement) {
          v -= .125;
        } else {
          v += .125;
        }
        
        if (v < 38.25){
          v = 38.25;
          decrement = false;
        }
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runAvg() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(150, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runAvgFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(38.125, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runAvgOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 1;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (v == 1) {
        v = 150;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 1;
      } else if (v == 150) {
        v = 151;
      } else {
        v = 150;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runAvgFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.AVG, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 1.25;
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (v == 1.25) {
        v = 38.1875;
      } else if (dp.timestamp() == 1357007400000L) {
        v = .25;
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runDev() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 149;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v < 0){
        v = 0;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runDevFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 36.875;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);
      
      if (decrement) {
        v -= 0.25;
      } else {
        v += 0.25;
      }
      
      if (v < 0.125){
        v = 0.125;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runDevOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 0;
    long ts = 1356998430000L;
    int counter = 0;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.longValue());
      if (dp.timestamp() == 1356998430000L) {
        v = 149;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0;
      } else if (counter % 2 == 0) {
        if (decrement) {
          v--;
        } else {
          v++;
        }
        if (v < 0) {
          v = 0;
          decrement = false;
          counter++;
        }
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runDevFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.DEV, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 0;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      if (dp.timestamp() == 1356998430000L) {
        v = 36.8125;
      } else if (dp.timestamp() == 1357007400000L) {
        v = 0;
      } else {
        if (decrement) {
          v -= 0.125;
        } else {
          v += 0.125;
        }
        if (v < 0.0625) {
          v = 0.0625;
          decrement = false;
        }
      }
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMin() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 1;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());
      
      if (decrement) {
        v--;
      } else {
        v++;
      }
      
      if (v == 151){
        v = 150;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMinOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMinFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 1.25;
    long ts = 1356998430000L;
    boolean decrement = false;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.0001);
      
      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }
      
      if (v > 38){
        v = 38.0;
        decrement = true;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMinFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMIN, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMax() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v = 300;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.longValue());

      if (decrement) {
        v--;
      } else {
        v++;
      }

      if (v == 150){
        v = 151;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMaxFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v = 75.0;
    long ts = 1356998430000L;
    boolean decrement = true;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(v, dp.doubleValue(), 0.001);

      if (decrement) {
        v -= .25;
      } else {
        v += .25;
      }

      if (v < 38.25){
        v = 38.25;
        decrement = false;
      }
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runMimMaxOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long v1 = 1;
    long v2 = 300;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      
      if (counter % 2 == 0) {        
        assertEquals(v1, dp.longValue());
        v1++;
      } else {
        assertEquals(v2, dp.longValue());
        v2--;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  @Test
  public void runMimMaxFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.MIMMAX, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    double v1 = 1.25;
    double v2 = 75.0;
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter % 2 == 0) {
        assertEquals(v1, dp.doubleValue(), 0.001);
        v1 += 0.25;
      } else {
        assertEquals(v2, dp.doubleValue(), 0.001);
        v2 -= 0.25;
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }

  @Test
  public void runPercentiles() throws Exception {
    storeLongTimeSeriesSeconds(false, true);

    // These are not accurate at all when data points only contain 2 values
    //   so we are just testing constructor logic, rather than precision
    testPercentile(Aggregators.p50, 150, 150);
    testPercentile(Aggregators.p75, 150, 150);
    testPercentile(Aggregators.p90, 150, 150);
    testPercentile(Aggregators.p95, 150, 150);
    testPercentile(Aggregators.p99, 150, 150);
    testPercentile(Aggregators.p999, 150, 150);
    testPercentile(Aggregators.ep50r3, 150, 150);
    testPercentile(Aggregators.ep75r3, 150, 150);
    testPercentile(Aggregators.ep90r3, 150, 150);
    testPercentile(Aggregators.ep95r3, 150, 150);
    testPercentile(Aggregators.ep99r3, 150, 150);
    testPercentile(Aggregators.ep999r3, 150, 150);
    testPercentile(Aggregators.ep50r7, 150, 150);
    testPercentile(Aggregators.ep75r7, 150, 150);
    testPercentile(Aggregators.ep90r7, 150, 150);
    testPercentile(Aggregators.ep95r7, 150, 150);
    testPercentile(Aggregators.ep99r7, 150, 150);
    testPercentile(Aggregators.ep999r7, 150, 150);
  }
  
  public void runCount() throws Exception {
    storeLongTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.COUNT, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(2, dp.longValue());
    }
    assertEquals(300, dps[0].size());
  }
  
  @Test
  public void runCountFloat() throws Exception {
    storeFloatTimeSeriesSeconds(false, false);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.COUNT, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 30000;
      assertEquals(2, dp.doubleValue(), 0.001);
    }
    assertEquals(300, dps[0].size());
  }
  
  // TODO - The count agg is inaccurate until we implement NaNs. 
  @Test
  public void runCountOffset() throws Exception {
    storeLongTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.COUNT, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter == 0 || counter == 599) {
        assertEquals(1, dp.longValue());
      } else {
        assertEquals(2, dp.longValue());
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  // TODO - The count agg is inaccurate until we implement NaNs. 
  @Test
  public void runCountFloatOffset() throws Exception {
    storeFloatTimeSeriesSeconds(false, true);
    
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, Aggregators.COUNT, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);
    
    long ts = 1356998430000L;
    int counter = 0;
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      if (counter == 0 || counter == 599) {
        assertEquals(1, dp.doubleValue(), 0.0001);
      } else {
        assertEquals(2, dp.doubleValue(), 0.0001);
      }
      counter++;
    }
    assertEquals(600, dps[0].size());
  }
  
  /**
   * Helper to test the various percentiles
   * @param agg The aggregator
   * @param value The value to expect
   * @param delta The variance to expect 
   */
  private void testPercentile(final Aggregator agg, final long value, 
      final double delta) {
    tags.clear();
    query.setStartTime(1356998400);
    query.setEndTime(1357041600);
    query.setTimeSeries(METRIC_STRING, tags, agg, false);
    final DataPoints[] dps = query.run();
    assertMeta(dps, 0, true);

    long ts = 1356998430000L;
    int counter = 0;
    int size = dps[0].size();
    for (DataPoint dp : dps[0]) {
      assertEquals(ts, dp.timestamp());
      ts += 15000;
      assertEquals("counter " + counter, value, dp.longValue(), delta);
      counter++;
    }
    assertEquals(600, size);
  }
}
