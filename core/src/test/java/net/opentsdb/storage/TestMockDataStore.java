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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import com.google.common.reflect.TypeToken;

import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.IteratorStatus;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
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
  private QueryContext context;
  
  @Before
  public void before() throws Exception {
    tsdb = mock(TSDB.class);
    config = new Config(false);
    context = mock(QueryContext.class);
    when(tsdb.getConfig()).thenReturn(config);
    
    config.overrideConfig("MockDataStore.timestamp", "1483228800000");
  }
  
  @Test
  public void initialize() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    for (Map<TypeToken<?>, MockSpan<?>> types : mds.getDatabase().values()) {
      MockSpan<?> spans = types.get(NumericType.TYPE);
      assertEquals(24, spans.rows().size());
      
      long ts = 1483228800000L;
      for (MockRow<?> row : spans.rows()) {
        assertEquals(ts, row.base_timestamp);
        
        TimeSeriesIterator<NumericType> iterator = 
            (TimeSeriesIterator<NumericType>) row.iterator();
        int count = 0;
        while (iterator.status() == IteratorStatus.HAS_DATA) {
          assertEquals(ts + (count * 60000), iterator.next().timestamp().msEpoch());
          count++;
        }
        ts += MockDataStore.ROW_WIDTH;
        assertEquals(60, count);
      }
      assertEquals(1483315200000L, ts);
    }
  }
  
  @Test
  public void query() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    TimeSeriesQuery query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483228800000")
            .setEnd("1483236000000"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addMetric(Metric.newBuilder()
            .setMetric("web.requests")
            .setFilter("f1")
            .setId("m2"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            )
        .build();
    
    QueryExecution<IteratorGroups> ex = mds.runTimeSeriesQuery(context, query, null);
    IteratorGroups results = ex.deferred().join();
    assertEquals(8, results.flattenedIterators().size());
    
    IteratorGroup group = results.group(new SimpleStringGroupId("m1"));
    List<TimeSeriesIterator<?>> iterators = group.flattenedIterators();
    assertEquals(4, iterators.size());
    for (final TimeSeriesIterator<?> it : iterators) {
      TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) it;
      long ts = 1483228800000L;
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(ts, v.timestamp().msEpoch());
        ts += 60000;
      }
      assertEquals(1483239600000L, ts);
    }
    
    query = TimeSeriesQuery.newBuilder()
        .setTime(Timespan.newBuilder()
            .setStart("1483228800000")
            .setEnd("1483236000000"))
        .addMetric(Metric.newBuilder()
            .setMetric("sys.cpu.user")
            .setFilter("f1")
            .setId("m1"))
        .addFilter(Filter.newBuilder()
            .setId("f1")
            .addFilter(TagVFilter.newBuilder()
                .setFilter("web01")
                .setType("literal_or")
                .setTagk("host")
                )
            .addFilter(TagVFilter.newBuilder()
                .setFilter("PHX")
                .setType("literal_or")
                .setTagk("dc")
                )
            )
        .build();
    
    mds = new MockDataStore();
    mds.initialize(tsdb).join();
    
    ex = mds.runTimeSeriesQuery(context, query, null);
    results = ex.deferred().join();
    iterators = results.flattenedIterators();
    assertEquals(1, iterators.size());
    for (final TimeSeriesIterator<?> it : iterators) {
      TimeSeriesIterator<NumericType> iterator = (TimeSeriesIterator<NumericType>) it;
      long ts = 1483228800000L;
      while (iterator.status() == IteratorStatus.HAS_DATA) {
        TimeSeriesValue<NumericType> v = iterator.next();
        assertEquals(ts, v.timestamp().msEpoch());
        ts += 60000;
      }
      assertEquals(1483239600000L, ts);
    }
  }
  
  @Test
  public void write() throws Exception {
    MockDataStore mds = new MockDataStore();
    mds.initialize(tsdb).join();
    assertEquals(4 * 4 * 4, mds.getDatabase().size());
    
    TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
        .addMetric("unit.test")
        .addTags("dc", "lga")
        .addTags("host", "db01")
        .build();
    MutableNumericType dp = new MutableNumericType(id);
    TimeStamp ts = new MillisecondTimeStamp(1483228800000L);
    dp.reset(ts, 42.5, 1);
    mds.write(dp, null, null);
    assertEquals((4 * 4 * 4) + 1, mds.getDatabase().size());
    
    ts.updateMsEpoch(1483228800000L + 60000L);
    dp.reset(ts, 24.5, 1);
    mds.write(dp, null, null);
    
    // no out-of-order timestamps per series for now. at least within a "row".
    ts.updateMsEpoch(1483228800000L + 30000L);
    dp.reset(ts, -1, 1);
    try {
      mds.write(dp, null, null);
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException e) { }
  }
}
