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

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryNodeConfig;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.QueryResultId;
import net.opentsdb.query.plan.QueryPlanner;
import net.opentsdb.query.processor.BaseQueryNodeFactory;

/**
 * Factory for validating the bucket quantile nodes and setting them up by 
 * walking the graph to find the input node IDs and metric names.
 * 
 * @since 3.0
 */
public class BucketQuantileFactory extends BaseQueryNodeFactory<BucketQuantileConfig, BucketQuantile> {

  public static final String TYPE = "BucketQuantile";
  
  public static final String QUANTILE_TAG = "_quantile";

  @Override
  public BucketQuantileConfig parseConfig(final ObjectMapper mapper, 
                                            final TSDB tsdb, 
                                            final JsonNode node) {
    return BucketQuantileConfig.parse(mapper, tsdb, node);
  }

  @Override
  public void setupGraph(final QueryPipelineContext context, 
                         final BucketQuantileConfig config,
                         final QueryPlanner plan) {
    final Set<QueryNodeConfig> downstream = plan.configGraph().successors(config);
    BucketQuantileConfig.Builder builder = config.toBuilder();
    
    for (final QueryNodeConfig ds : downstream) {
      if (builder.underFlowId() == null && !Strings.isNullOrEmpty(config.getUnderflow())) {
        final String metric = plan.getMetricForDataSource(ds, config.getUnderflow());
        if (!Strings.isNullOrEmpty(metric)) {
          builder.setUnderflowId(matchId(ds, config.getUnderflow()))
                 .setUnderflowMetric(metric);
        }
      }
      
      if (builder.overFlowId() == null && !Strings.isNullOrEmpty(config.getOverflow())) {
        final String metric = plan.getMetricForDataSource(ds, config.getOverflow());
        if (!Strings.isNullOrEmpty(metric)) {
          builder.setOverflowId(matchId(ds, config.getOverflow()))
                 .setOverflowMetric(metric);
        }
      }
      
      for (final String histogram : config.getHistograms()) {
        final String metric = plan.getMetricForDataSource(ds, histogram);
        if (!Strings.isNullOrEmpty(metric)) {
          builder.addHistogramId(matchId(ds, histogram))
                 .addHistogramMetric(metric);
        }
      }
    }
    
    // validations now
    BucketQuantileConfig rebuilt = (BucketQuantileConfig) builder.build();
    if (rebuilt.histogramIds() == null || 
        rebuilt.getHistograms().size() != rebuilt.histogramIds().size()) {
      throw new QueryExecutionException(
          "Missing one or more node IDs given the histograms requested. "
              + "Histograms: " + rebuilt.getHistograms() + " IDs: " 
              + rebuilt.histogramIds(), 0, 400);
    }
    if (rebuilt.histogramMetrics() == null || 
        rebuilt.getHistograms().size() != rebuilt.histogramMetrics().size()) {
      throw new QueryExecutionException(
          "Missing one or more histogram metrics given the histograms requested. "
              + "Histograms: " + rebuilt.getHistograms() + " IDs: " 
              + rebuilt.histogramMetrics(), 0, 400);
    }
    
    if (!Strings.isNullOrEmpty(rebuilt.getOverflow())) {
      if (rebuilt.overflowId() == null) {
        throw new QueryExecutionException(
            "Missing the overflow node ID for overflow: " 
                + rebuilt.getOverflow(), 0, 400);
      }
      if (Strings.isNullOrEmpty(rebuilt.overflowMetric())) {
        throw new QueryExecutionException(
            "Missing the overflow metric for overflow: " 
                + rebuilt.getOverflow(), 0, 400);
      }
    }
    
    if (!Strings.isNullOrEmpty(rebuilt.getUnderflow())) {
      if (rebuilt.underflowId() == null) {
        throw new QueryExecutionException(
            "Missing the underflow node ID for overflow: " 
                + rebuilt.getUnderflow(), 0, 400);
      }
      if (Strings.isNullOrEmpty(rebuilt.underflowMetric())) {
        throw new QueryExecutionException(
            "Missing the underflow metric for overflow: " 
                + rebuilt.getUnderflow(), 0, 400);
      }
    }
    plan.replace(config, rebuilt);
  }
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public BucketQuantile newNode(QueryPipelineContext context, BucketQuantileConfig config) {
    return new BucketQuantile(this, context, config);
  }
  
  /**
   * Helper to match the result ID given a list of IDs in the node. 
   * @param config The non-null config read IDs from.
   * @param id The non-null ID to match.
   * @return The ID if found, null if not.
   */
  private QueryResultId matchId(final QueryNodeConfig config, final String id) {
    final List<QueryResultId> ids = config.resultIds();
    for (int i = 0; i < ids.size(); i++) {
      final QueryResultId result_id = ids.get(i);
      if (result_id.dataSource().equals(id)) {
        return result_id;
      }
    }
    return null;
  }
}
