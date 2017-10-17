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
package net.opentsdb.storage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Iterator;
import java.util.Map.Entry;

import org.junit.Before;
import org.junit.Test;

import com.stumbleupon.async.Deferred;

import net.opentsdb.core.Registry;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.BaseTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryContextBuilder;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QuerySink;
import net.opentsdb.query.QueryMode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.query.pojo.Timespan;
import net.opentsdb.storage.MockDataStore.MockRow;
import net.opentsdb.storage.MockDataStore.MockSpan;
import net.opentsdb.utils.Config;

public class TestMockDataStore {

  private TSDB tsdb;
  private Config config;
  private Registry registry;
  private MockDataStore mds;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    config = new Config(false);
    registry = mock(Registry.class);
    when(tsdb.getConfig()).thenReturn(config);
    when(tsdb.getRegistry()).thenReturn(registry);
    
    config.overrideConfig("MockDataStore.timestamp", "1483228800000");
    config.overrideConfig("MockDataStore.threadpool.enable", "true");
    config.overrideConfig("MockDataStore.sysout.enable", "true");
    mds = new MockDataStore();
    mds.initialize(tsdb).join();
    when(registry.getQueryNodeFactory(anyString())).thenReturn(mds);
  }

  
  @Test
  public void initialize() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    for (final Entry<TimeSeriesId, MockSpan> series : mds.getDatabase().entrySet()) {
      assertEquals(24, series.getValue().rows().size());
      
      long ts = 1483228800000L;
      for (MockRow row : series.getValue().rows()) {
        assertEquals(ts, row.base_timestamp);
        
        Iterator<TimeSeriesValue<?>> it = row.iterator(NumericType.TYPE).get();
        int count = 0;
        while (it.hasNext()) {
          assertEquals(ts + (count * 60000), it.next().timestamp().msEpoch());
          count++;
        }
        ts += MockDataStore.ROW_WIDTH;
        assertEquals(60, count);
      }
      assertEquals(1483315200000L, ts);
    }
  }
  
  @Test
  public void write() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    TimeSeriesId id = BaseTimeSeriesId.newBuilder()
        .setMetric("unit.test")
        .addTags("dc", "lga")
        .addTags("host", "db01")
        .build();
    MutableNumericType dp = new MutableNumericType();
    TimeStamp ts = new MillisecondTimeStamp(1483228800000L);
    dp.reset(ts, 42.5);
    mds.write(id, dp, null, null);
    assertEquals((4 * 4 * 4) + 1, mds.getDatabase().size());
    
    ts.updateMsEpoch(1483228800000L + 60000L);
    dp.reset(ts, 24.5);
    mds.write(id, dp, null, null);
    
    // no out-of-order timestamps per series for now. at least within a "row".
    ts.updateMsEpoch(1483228800000L + 30000L);
    dp.reset(ts, -1);
    try {
      mds.write(id, dp, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }

}
