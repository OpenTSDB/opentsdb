// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.processor.groupby;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;

import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesDataSource;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.query.AbstractQueryNode;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryNodeFactory;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.processor.downsample.Downsample;
import net.opentsdb.query.processor.downsample.DownsampleConfig;
import net.opentsdb.query.processor.groupby.GroupByConfig;
import net.opentsdb.stats.Span;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Performs the time series grouping aggregation by sorting time series according
 * to tag keys and merging the results into single time series using an 
 * aggregation function.
 * <p>
 * For each result returned to {@link #onNext(QueryResult)}, a new set of time 
 * series is generated containing a collection of source time series from the 
 * incoming result set. The actual arithmetic is performed when upstream sources
 * fetch an iterator and being the iteration.
 * 
 * @since 3.0
 */
public class GroupBy extends AbstractQueryNode {

  private static final Logger LOG = LoggerFactory.getLogger(AbstractQueryNode.class);
  
  /** The config for this group by node. */
  private final GroupByConfig config;

  /**
   * An optional downsample config for use by the GroupByArrayIterator to size it's array properly
   * when running in parallel.
   */
  private DownsampleConfig downsampleConfig;

  /**
   * Default ctor.
   * @param factory The non-null factory for generating iterators.
   * @param context The non-null pipeline context we belong to.
   * @param config A non-null group by config to configure the iterators with.
   */
  public GroupBy(final QueryNodeFactory factory, 
                 final QueryPipelineContext context, 
                 final GroupByConfig config) {
    super(factory, context);
    if (config == null) {
      throw new IllegalArgumentException("Group By config cannot be null.");
    }
    this.config = config;
  }

  @Override
  public Deferred<Void> initialize(Span span) {
    return super.initialize(span)
        .addCallback(
            arg -> {
              for (QueryNode node : (Collection<QueryNode>) this.downstream) {
                if (node instanceof TimeSeriesDataSource) {
                  TimeSeriesDataSource timeSeriesDataSource = (TimeSeriesDataSource) node;
                  TimeSeriesDataSourceConfig config = (TimeSeriesDataSourceConfig) timeSeriesDataSource.config();
                  List<QueryNodeConfig> pushDownNodes = config.getPushDownNodes();
                  for (QueryNodeConfig queryNodeConfig : pushDownNodes) {
                      if(queryNodeConfig instanceof DownsampleConfig) {
                        downsampleConfig = (DownsampleConfig) queryNodeConfig;
                        break;
                      }
                  }
                  break;
                }
                if(node instanceof Downsample){
                  Downsample downsample = (Downsample) node;
                  downsampleConfig = (DownsampleConfig) downsample.config();
                  break;
                }
              }
              return null;
            });
  }

  @Override
  public void close() {
    // No-op
  }
  
  @Override
  public void onNext(final QueryResult next) {
    if (next.idType() == Const.TS_BYTE_ID && 
        config.getEncodedTagKeys() == null &&
        config.getTagKeys() != null && 
        !config.getTagKeys().isEmpty()) {
      
      class ResolveCB implements Callback<Object, List<byte[]>> {
        @Override
        public Object call(List<byte[]> arg) throws Exception {
          synchronized (GroupBy.this) {
            config.setEncodedTagKeys(arg);
          }
          try {
            final GroupByResult result = new GroupByResult(GroupBy.this, next);
            sendUpstream(result);
          } catch (Exception e) {
            sendUpstream(e);
          }
          return null;
        }
      }
      
      class ErrorCB implements Callback<Object, Exception> {
        @Override
        public Object call(final Exception ex) throws Exception {
          sendUpstream(ex);
          return null;
        }
      }
      
      final Iterator<TimeSeries> iterator = next.timeSeries().iterator();
      if (iterator.hasNext()) {
        final TimeSeriesDataSourceFactory store = ((TimeSeriesByteId) 
            iterator.next().id()).dataStore();
        if (store == null) {
          throw new RuntimeException("The data store was null for a byte series!");
        }
        store.encodeJoinKeys(Lists.newArrayList(config.getTagKeys()), null /* TODO */)
          .addCallback(new ResolveCB())
          .addErrback(new ErrorCB());
      } else {
        final GroupByResult result = new GroupByResult(this, next);
        sendUpstream(result);
      }
    } else {
      final GroupByResult result = new GroupByResult(this, next);
      sendUpstream(result);
    }
  }
  
  @Override
  public QueryNodeConfig config() {
    return config;
  }
  
  /** @return The number of upstream consumers. */
  protected int upstreams() {
    return upstream.size();
  }

  public DownsampleConfig getDownsampleConfig() {
    return downsampleConfig;
  }

}
