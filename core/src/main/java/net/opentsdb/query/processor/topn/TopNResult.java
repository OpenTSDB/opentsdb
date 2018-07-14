// This file is part of OpenTSDB.
// Copyright (C) 2015-2018  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.topn;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.data.types.numeric.NumericSummaryType;
import net.opentsdb.data.types.numeric.NumericType;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.rollup.RollupConfig;

/**
 * Implements top-n functionality by iterating over each of the time series,
 * sorting and returning the top "n" time series with the highest or lowest
 * values depending on the aggregator used.
 * 
 * @since 3.0
 */
public class TopNResult implements QueryResult, Runnable {

  /** The parent node. */
  protected final TopN node;
  
  /** The downstream result received by the group by node. */
  protected final QueryResult next;
  
  /** The ordered results to fill. */
  protected final List<TimeSeries> results;
  
  /**
   * Default ctor.
   * @param node A non-null TopN node we belong to.
   * @param next The non-null results to pull from.
   */
  public TopNResult(final TopN node, final QueryResult next) {
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (next == null) {
      throw new IllegalArgumentException("Result cannot be null.");
    }
    this.node = node;
    this.next = next;
    results = Lists.newArrayList();
  }
  
  @Override
  public void run() {
    try {
      if (next.timeSeries().isEmpty()) {
        node.onNext(this);
        return;
      }
      
      final TreeMap<Number, TimeSeries> sorted_results = 
          new TreeMap<Number, TimeSeries>();
      for (final TimeSeries ts : next.timeSeries()) {
        // TODO - parallelize
        
        final NumericType value;
        if (ts.types().contains(NumericType.TYPE)) {
          final TopNNumericAggregator agg = 
              new TopNNumericAggregator(node, this, ts);
          value = agg.run();
        } else if (ts.types().contains(NumericSummaryType.TYPE)) {
          final TopNNumericSummaryAggregator agg = 
              new TopNNumericSummaryAggregator(node, this, ts);
          value = agg.run();
        } else {
          continue;
        }
        
        if (value == null) {
          continue;
        }
        
        if (value.isInteger()) {
          sorted_results.put(value.longValue(), ts);
        } else {
          sorted_results.put(value.doubleValue(), ts);
        }
      }
      
      final Iterator<Entry<Number, TimeSeries>> iterator = 
          ((TopNConfig) node.config()).getTop() ?
              sorted_results.descendingMap().entrySet().iterator() :
                sorted_results.entrySet().iterator();
      
      for (int i = 0; i < ((TopNConfig) node.config()).getCount(); i++) {
        if (!iterator.hasNext()) {
          break;
        }
        
        results.add(iterator.next().getValue());
      }
      node.onNext(this);
    } catch (Exception e) {
      node.onError(e);
    }
  }

  @Override
  public TimeSpecification timeSpecification() {
    return next.timeSpecification();
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return results;
  }

  @Override
  public long sequenceId() {
    return next.sequenceId();
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return next.idType();
  }

  @Override
  public ChronoUnit resolution() {
    return next.resolution();
  }

  @Override
  public RollupConfig rollupConfig() {
    return next.rollupConfig();
  }

  @Override
  public void close() {
    // TODO Auto-generated method stub
    
  }
 
}
