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

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import io.opentracing.Span;
import net.opentsdb.common.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.SimpleStringGroupId;
import net.opentsdb.data.SimpleStringTimeSeriesId;
import net.opentsdb.data.TimeSeriesDataType;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesValue;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.SlicedTimeSeriesIterator;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.data.types.numeric.MutableNumericType;
import net.opentsdb.data.types.numeric.NumericMillisecondShard;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.query.execution.QueryExecution;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Metric;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.stats.TsdbTrace;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSON;

/**
 * A simple store that generates a set of time series to query as well as stores
 * new values (must be written in time order) all in memory. It's meant for
 * testing pipelines and benchmarking.
 * 
 * @since 3.0
 */
public class MockDataStore extends TimeSeriesDataStore {
  public static final long ROW_WIDTH = 3600000;
  public static final long HOSTS = 4;
  public static final long INTERVAL = 60000;
  public static final long HOURS = 24;
  public static final List<String> DATACENTERS = Lists.newArrayList(
      "PHX", "LGA", "LAX", "DEN");
  public static final List<String> METRICS = Lists.newArrayList(
      "sys.cpu.user", "sys.if.out", "sys.if.in", "web.requests");

  /** <ID, <type, list of hourly buckets>> */
  private Map<TimeSeriesId, Map<TypeToken<?>, MockSpan<?>>> database;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    database = Maps.newHashMap();
    generateMockData(tsdb);
    return Deferred.fromResult(null);
  }
  
  @SuppressWarnings("unchecked")
  @Override
  public Deferred<Object> write(final TimeSeriesValue<?> value, 
                                final TsdbTrace trace,
                                final Span upstream_span) {
    Map<TypeToken<?>, MockSpan<?>> type = database.get(value.id());
    if (type == null) {
      type = Maps.newHashMap();
      database.put(value.id(), type);
    }
    
    MockSpan<?> span = type.get(value.type());
    if (span == null) {
      if (value.type() == NumericType.TYPE) {
        span = new MockSpan<NumericType>();
      }
      
      type.put(value.type(), span);
    }
    
    if (value.type() == NumericType.TYPE) {
      ((MockSpan<NumericType>) span).addValue((TimeSeriesValue<NumericType>) value);
    }
    return Deferred.fromResult(null);
  }

  @Override
  public QueryExecution<IteratorGroups> runTimeSeriesQuery(final QueryContext context,
                                                           final TimeSeriesQuery query, 
                                                           final Span upstream_span) {
    class Ex extends QueryExecution<IteratorGroups> {

      public Ex(TimeSeriesQuery query) {
        super(query);
      }

      @Override
      public void cancel() {
        
      }
      
      public void execute() {
        setSpan(context, 
                MockDataStore.class.getSimpleName(), 
                upstream_span,
                TsdbTrace.addTags(
                    "order", Integer.toString(query.getOrder()),
                    "query", JSON.serializeToString(query),
                    "startThread", Thread.currentThread().getName()));
        
        int matches = 0;
        final IteratorGroups results = new DefaultIteratorGroups(); 
        for (final Entry<TimeSeriesId, Map<TypeToken<?>, MockSpan<?>>> entry : 
              database.entrySet()) {
          for (Metric m : query.getMetrics()) {
            if (Bytes.memcmp(m.getMetric().getBytes(Const.UTF8_CHARSET), 
                             entry.getKey().metrics().get(0)) != 0) {
              continue;
            }
            
            if (!Strings.isNullOrEmpty(m.getFilter())) {
              Filter f = null;
              for (Filter filter : query.getFilters()) {
                if (filter.getId().equals(m.getFilter())) {
                  f = filter;
                  break;
                }
              }
              
              if (f == null) {
                // WTF? Shouldn't happen at this level.
                continue;
              }
              
              boolean matched = true;
              for (TagVFilter tf : f.getTags()) {
                byte[] tagv = entry.getKey().tags().get(
                    tf.getTagk().getBytes(Const.UTF8_CHARSET));
                if (tagv == null) {
                  matched = false;
                  break;
                }
                
                try {
                  if (!tf.match(ImmutableMap.of(tf.getTagk(), 
                      new String(tagv, Const.UTF8_CHARSET))).join()) {
                    matched = false;
                    break;
                  }
                } catch (Exception e) {
                  callback(e);
                  return;
                }
              }
              
              if (!matched) {
                continue;
              }
            }
            
            // matched the filters
            final MockSpan<?> span = entry.getValue().get(NumericType.TYPE);
            if (span == null) {
              continue;
            }
            
            SlicedTimeSeriesIterator<NumericType> iterator = 
                new SlicedTimeSeriesIterator<NumericType>();
            int rows = 0;
            for (final MockRow<NumericType> row : ((MockSpan<NumericType>) span).rows) {
              if (row.base_timestamp >= query.getTime().startTime().msEpoch() && 
                  row.base_timestamp <= query.getTime().endTime().msEpoch()) {
                iterator.addIterator(row.iterator());
                rows++;
              }
            }
            
            if (rows > 0) {
              results.addIterator(new SimpleStringGroupId(
                  Strings.isNullOrEmpty(m.getId()) ? m.getMetric() : m.getId()), iterator);
              matches++;
            }
          }
        }
        
        callback(results, TsdbTrace.successfulTags(
            "matchedSeries", Integer.toString(matches)));
      }
      
    }
    
    Ex result = new Ex(query);
    result.execute();
    return result;
  }

  @Override
  public String id() {
    return "MockDataStore";
  }

  @Override
  public String version() {
    return "0.0.0";
  }

  class MockSpan<T extends TimeSeriesDataType> {
    private List<MockRow<T>> rows = Lists.newArrayList();
    
    public void addValue(TimeSeriesValue<T> value) {
      
      long base_time = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      //System.out.println("BASE TIME: " + base_time);
      for (final MockRow<T> row : rows) {
        if (row.base_timestamp == base_time) {
          row.addValue((TimeSeriesValue<T>) value);
          return;
        }
      }
      
      if (value.type() == NumericType.TYPE) {
        MockRow<NumericType> row = new NumericMockRow(
            (TimeSeriesValue<NumericType>) value);
        rows.add((MockRow<T>) row);
      }
    }
  
    List<MockRow<T>> rows() {
      return rows;
    }
  }
  
  abstract class MockRow<T extends TimeSeriesDataType> {
    public long base_timestamp;
    
    public abstract void addValue(TimeSeriesValue<T> value);
    
    public abstract TimeSeriesIterator<T> iterator();
  }
  
  class NumericMockRow extends MockRow<NumericType> {

    NumericMillisecondShard shard;
    
    public NumericMockRow(final TimeSeriesValue<NumericType> value) {
      base_timestamp = value.timestamp().msEpoch() - 
          (value.timestamp().msEpoch() % ROW_WIDTH);
      shard = new NumericMillisecondShard(value.id(), 
          new MillisecondTimeStamp(base_timestamp), 
          new MillisecondTimeStamp(base_timestamp + ROW_WIDTH));
      addValue(value);
    }
    
    @Override
    public void addValue(TimeSeriesValue<NumericType> value) {
      if (value.value().isInteger()) {
        shard.add(value.timestamp().msEpoch(), value.value().longValue(),
            value.realCount());
      } else {
        shard.add(value.timestamp().msEpoch(), value.value().doubleValue(), 
            value.realCount());
      }
    }

    @Override
    public TimeSeriesIterator<NumericType> iterator() {
      return shard.getShallowCopy(null);
    }
    
  }

  private void generateMockData(final TSDB tsdb) {
    long start_timestamp = DateTime.currentTimeMillis() - 2 * ROW_WIDTH;
    start_timestamp = start_timestamp - start_timestamp % ROW_WIDTH;
    if (tsdb.getConfig().hasProperty("MockDataStore.timestamp")) {
      start_timestamp = tsdb.getConfig().getLong("MockDataStore.timestamp");
    }
    
    long hours = HOURS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hours")) {
      hours = tsdb.getConfig().getLong("MockDataStore.hours");
    }
    
    long hosts = HOSTS;
    if (tsdb.getConfig().hasProperty("MockDataStore.hosts")) {
      hosts = tsdb.getConfig().getLong("MockDataStore.hosts");
    }
    
    long interval = INTERVAL;
    if (tsdb.getConfig().hasProperty("MockDataStore.interval")) {
      interval = tsdb.getConfig().getLong("MockDataStore.interval");
      if (interval <= 0) {
        throw new IllegalStateException("Interval can't be 0 or less.");
      }
    }
    
    for (int t = 0; t < hours; t++) {
      for (final String metric : METRICS) {
        for (final String dc : DATACENTERS) {
          for (int h = 0; h < hosts; h++) {
            TimeSeriesId id = SimpleStringTimeSeriesId.newBuilder()
                .addMetric(metric)
                .addTags("dc", dc)
                .addTags("host", String.format("web%02d", h + 1))
                .build();
            MutableNumericType dp = new MutableNumericType(id);
            TimeStamp ts = new MillisecondTimeStamp(0);
            for (long i = 0; i < (ROW_WIDTH / interval); i++) {
              ts.updateMsEpoch(start_timestamp + (i * interval) + (t * ROW_WIDTH));
              dp.reset(ts, t + h + i, 1);
              write(dp, null, null);
            }
          }
        }

      }
    }
  }

  Map<TimeSeriesId, Map<TypeToken<?>, MockSpan<?>>> getDatabase() {
    return database;
  }
}
