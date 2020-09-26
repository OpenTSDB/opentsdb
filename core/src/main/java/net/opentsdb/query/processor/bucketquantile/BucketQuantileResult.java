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

import java.io.IOException;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.map.hash.TLongObjectHashMap;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesList;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericArrayType;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.DefaultQueryResultId;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.rollup.RollupConfig;
import net.opentsdb.utils.XXHash;
import net.opentsdb.utils.Deferreds;

/**
 * The result that joins the time series on tag set hashes so we can compute the
 * quantiles.
 * <p>
 * Indexing is fun here in order to avoid creating tons and tons of arrays.
 * <p>
 * The final list has an array of the series for each tag set.
 * When we iterate or fetch from the "List" we consider the number of quantiles
 * requested so that the "size" of the time series collection is the number of 
 * quantiles times the cardinality. When we get an index into the list (or 
 * iterate) then we consider it as:
 * real1                real2
 * p1_1   p2_1   p3_1   p2_1   p2_2   p2_3
 * 0      1      2      3      4      5
 * 
 * @since 3.0
 */
public class BucketQuantileResult extends BaseTimeSeriesList implements QueryResult {
  private static final Logger LOG = LoggerFactory.getLogger(BucketQuantileResult.class);
  
  private final BucketQuantile node;
  private final List<Double> quantiles;
  private final QueryResultId result_id;
  private TLongObjectMap<TimeSeries[]> map;
  private TLongObjectMap<TimeSeriesId> resolved_ids;
  private QueryResult initial_result;
  private TimeSeries[][] final_series;
  private TimeSeriesId[] final_ids;
  private BucketQuantileProcessor last_ts;
  
  BucketQuantileResult(final BucketQuantile node) {
    this.node = node;
    quantiles = ((BucketQuantileConfig) node.config()).getQuantiles();
    map = new TLongObjectHashMap<TimeSeries[]>();
    result_id = new DefaultQueryResultId(node.config().getId(), 
        ((BucketQuantileConfig) node.config()).getAs());
  }
  
  /**
   * Join the result on tags with the other buckets for a time series.
   * 
   * @param result The non-null result to add.
   * @return A deferred in case we need to resolve the ID to strings for proper
   * joining.
   */
  Deferred<Void> addResult(final QueryResult result) {
    if (initial_result == null || 
        initial_result.timeSeries().isEmpty() && !result.timeSeries().isEmpty()) {
      initial_result = result;
    }
    
    if (result.timeSeries() == null || 
        result.timeSeries().size() < 1) {
      // nothing to do.
      return Deferred.fromResult(null);
    }
    
    if (result.idType() == Const.TS_STRING_ID) {
      for (int i = 0; i < result.timeSeries().size(); i++) {
        final TimeSeries series = result.timeSeries().get(i);
        final long hash = hashStringId((TimeSeriesStringId) series.id());
        
        int bucket_index = -1;
        TimeSeriesStringId id = (TimeSeriesStringId) series.id();
        for (int x = 0; x < node.buckets().length; x++) {
          if (node.buckets()[x].metric.equals(id.metric())) {
            bucket_index = x;
            break;
          }
        }
        
        if (bucket_index < 0) {
          LOG.error("??? The result set with metric: " + id.metric() 
            + " didn't match a bucket?");
          continue;
        }
        
        TimeSeries[] ts = map.get(hash);
        if (ts == null) {
          ts = new TimeSeries[node.buckets().length];
          map.put(hash, ts);
        }
        ts[bucket_index] = series;
      }
      return Deferred.fromResult(null);
    }
    
    // bytes so a bit more work
    if (resolved_ids == null) {
      resolved_ids = new TLongObjectHashMap<TimeSeriesId>();
    }
    List<Deferred<Void>> deferreds = Lists.newArrayList();
    for (int i = 0; i < result.timeSeries().size(); i++) {
      final TimeSeries series = result.timeSeries().get(i);
      final TimeSeriesByteId id = (TimeSeriesByteId) series.id();
      
      class ResolveCB implements Callback<Deferred<Void>, TimeSeriesStringId> {

        @Override
        public Deferred<Void> call(final TimeSeriesStringId id) throws Exception {
          final long hash = hashStringId(id);
          int bucket_index = -1;
          for (int x = 0; x < node.buckets().length; x++) {
            if (node.buckets()[x].metric.equals(id.metric())) {
              bucket_index = x;
              break;
            }
          }
          if (bucket_index < 0) {
            LOG.error("The result set with metric: " + id.metric() 
              + " didn't match a bucket?");
            return Deferred.fromResult(null);
          }
          
          synchronized (map) { 
            TimeSeries[] ts = map.get(hash);
            if (ts == null) {
              ts = new TimeSeries[node.buckets().length];
              map.put(hash, ts);
            }
            ts[bucket_index] = series;
            resolved_ids.putIfAbsent(hash, id);
          }
          return Deferred.fromResult(null);
        }
        
      }
      
      deferreds.add(id.decode(false, null).addCallbackDeferring(new ResolveCB()));
    }
    return Deferred.group(deferreds).addCallback(Deferreds.VOID_GROUP_CB);
  }
  
  /**
   * Walks the map to find out if we have any data, tossing out everything if
   * the missing metric threshold has been configured and exceeded.
   * 
   */
  void finishSetup() {
    final_series = new TimeSeries[map.size()][];
    if (resolved_ids != null) {
      final_ids = new TimeSeriesId[map.size()];
    }
    final TLongObjectIterator<TimeSeries[]> iterator = map.iterator();
    int final_index = 0;
    final double threshold = ((BucketQuantileConfig) node.config())
        .getMissingMetricThreshold();
    while (iterator.hasNext()) {
      iterator.advance();
      if (threshold > 0) {
        int nulls = 0;
        final TimeSeries[] array = iterator.value();
        for (int i = 0; i < array.length; i++) {
          if (array[i] == null) {
            nulls++;
          }
        }
        double missing = ((double) nulls / (double) array.length) * 100;
        if (missing >= threshold) {
          // boot it.
          if (node.pipelineContext().query().isWarnEnabled()) {
            TimeSeriesId id = null;
            for (int i = 0; i < array.length; i++) {
              if (array[i] != null) {
                id = array[i].id();
                break;
              }
            }
            if (id != null) {
              node.pipelineContext().queryContext().logWarn(node, 
                  "Removing time series with " + nulls 
                  + " missing metrics out of " + array.length + ": " + id);
            } else {
              node.pipelineContext().queryContext().logWarn(node, 
                  "Removing time series with " + nulls 
                  + " missing metrics out of " + array.length);
            }
          }
          continue;
        }
      }
      final_series[final_index] = iterator.value();
      if (final_ids != null) {
        final_ids[final_index] = resolved_ids.get(iterator.key());
      }
      final_index++;
    }
    size = final_index * quantiles.size();
    map = null;
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    return initial_result.timeSpecification();
  }

  @Override
  public List<TimeSeries> timeSeries() {
    return this;
  }

  @Override
  public String error() {
    return null;
  }

  @Override
  public Throwable exception() {
    return null;
  }

  @Override
  public long sequenceId() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public QueryResultId dataSource() {
    return result_id;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return initial_result.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return initial_result.rollupConfig();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }

  @Override
  public boolean processInParallel() {
    // TODO - we may need the listerator and thread locals?
    return false;
  }

  @Override
  public Iterator<TimeSeries> iterator() {
    return new LocalIterator();
  }

  @Override
  public TimeSeries get(final int index) {
    int real_idx = index / quantiles.size();
    if (last_ts != null && last_ts.index == real_idx) {
      // cool, re-use!
    } else {
      final TimeSeries[] set = final_series[real_idx];
      // TODO - Yes we assume they're all the same. They better be. Add a check.
      TypeToken<?> type = null;
      for (int i = 0; i < set.length; i++) {
        if (set[i] != null) {
          // TODO - maybe could be more than one... grr
          type = set[i].types().iterator().next();
          break;
        }
      }

      if (last_ts != null) {
        try {
          last_ts.close();
        } catch (IOException e) {
          // don't worry about it.
          e.printStackTrace();
        }
      }
      
      if (type == NumericType.TYPE) {
        last_ts = new BucketQuantileNumericProcessor(real_idx, node, set, 
            final_ids != null ? final_ids[real_idx] : null) ;
      } else if (type == NumericArrayType.TYPE) {
        last_ts = new BucketQuantileNumericArrayProcessor(real_idx, node, set, 
            final_ids != null ? final_ids[real_idx] : null);
      } else if (type == NumericSummaryType.TYPE) {
        last_ts = new BucketQuantileNumericSummaryProcessor(real_idx, node, set, 
            final_ids != null ? final_ids[real_idx] : null);
      } else {
        throw new IllegalStateException("Unhandled type: " + type);
      }
      
      last_ts.run();
    }
    
    int quantile = index - (real_idx * quantiles.size());
    return last_ts.getSeries(quantile);
  }

  /**
   * The iterator for the list implementation.
   */
  class LocalIterator implements Iterator<TimeSeries> {
    int idx = 0;
    
    @Override
    public boolean hasNext() {
      return idx < size;
    }

    @Override
    public TimeSeries next() {
      return get(idx++);
    }
    
  }

  TLongObjectMap<TimeSeries[]> map() {
    return map;
  }
  
  TimeSeries[][] finalSeries() {
    return final_series;
  }
  
  // TODO - we should have a hash method in the IDs that just hashes tags.
  long hashStringId(final TimeSeriesStringId id) {
    // super critically important that we sort the tags.
    final Map<String, String> sorted_tags;
    if (id.tags() == null) {
      sorted_tags = Collections.emptyMap();
    } else if (id.tags() instanceof NavigableMap) {
      sorted_tags = id.tags();
    } else if (!id.tags().isEmpty()) {
      sorted_tags = new TreeMap<String, String>(id.tags());
    } else {
      sorted_tags = Collections.emptyMap();
    }
    
    long hash = 0;
    if (sorted_tags != null) {
      for (final Entry<String, String> entry : sorted_tags.entrySet()) {
        if (hash == 0) {
          hash = XXHash.hash(entry.getKey());
        } else {
          hash = XXHash.updateHash(hash, entry.getKey());
        }
        hash = XXHash.updateHash(hash, entry.getValue());
      }
    }
    return hash;
  }
  
}