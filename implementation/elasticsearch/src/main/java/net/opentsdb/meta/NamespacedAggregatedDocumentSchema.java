// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.meta;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.*;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.stats.Span;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Run the Meta Query on Meta Store with schema and form the results.
 *
 * @since 3.0
 */
public class NamespacedAggregatedDocumentSchema extends BaseTSDBPlugin implements
        MetaDataStorageSchema {
  public static final String TYPE = "NamespacedAggregatedDocumentSchema";
  public static final String MAX_CARD_KEY = "tsd.meta.max.cardinality";
  public static final String QUERY_TIMEOUT_KEY = "es.query_timeout";
  private static final Logger LOG = LoggerFactory.getLogger(NamespacedAggregatedDocumentSchema.class);
  private TSDB tsdb;

  /** The elastic search client to use */
  private YmsESClient native_client;

  private boolean fallback_on_exception;
  private boolean fallback_on_no_data;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    native_client = (YmsESClient) tsdb.getRegistry().getPlugin(BaseTSDBPlugin
            .class, "YmsESClient");
    if (native_client == null) {
      throw new IllegalStateException("No client found!");
    }

    if (!tsdb.getConfig().hasProperty(MAX_CARD_KEY)) {
      tsdb.getConfig().register(MAX_CARD_KEY, 4096, true,
              "The maximum number of entries to allow for multi-get queries.");
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Deferred<MetaDataStorageResult> runQuery(
          final MetaQuery query,
          final Span span) {

    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
              .start();
    } else {
      child = null;
    }

    SearchSourceBuilder search_source_builder = buildQuery(query.filters(),
            query.aggregate_by(), query.aggregation_field(),
            query.from(), query.to());

    LOG.info("Running Elastic Search query == " + search_source_builder
            .toString());

    class ResultCB2 implements Callback<MetaDataStorageResult,
            List<SearchResponse>> {

      @Override
      public MetaDataStorageResult call(final List<SearchResponse> results)
              throws Exception {
        YmsResult result = new YmsResult();
        final Set<TimeSeriesStringId> ids = Sets.newHashSet();

        int size = query.to() - query.from();
        search_source_builder.size(size);

        for (final SearchResponse response : results) {

          List<String> post_query_filter = new ArrayList<>();
          List<String> metrics_literal_filter = new ArrayList<>();
          List<String> metrics_regex_filter = new ArrayList<>();
          if (query.filters() instanceof ChainFilter) {
            List<QueryFilter> filters = ((ChainFilter) query.filters())
                    .getFilters();
            for (QueryFilter filter : filters) {
              if (filter instanceof AnyFieldRegexFilter) {
                String regex = ((AnyFieldRegexFilter) filter).getFilter();
                post_query_filter.add(NamespacedAggregatedDocumentQuery.convertToLuceneRegex(regex));

              }
              if (filter instanceof MetricLiteralFilter) {
                metrics_literal_filter.addAll(Arrays.asList((
                        (MetricLiteralFilter) filter).getMetric().split
                        ("\\|")));
              }
              if (filter instanceof MetricRegexFilter) {
                String regex = ((MetricRegexFilter) filter).getMetric();
                metrics_regex_filter.add(NamespacedAggregatedDocumentQuery.convertToLuceneRegex(regex));
              }
            }
          }
          LOG.info("Got response in " + response.getTookInMillis());
          long startTime = System.currentTimeMillis();
          long count = 0;
          for (final SearchHit hit : response.getHits().hits()) {
            final Map<String, Object> source = hit.getSource();
            List<Map<String, String>> tags = (List<Map<String, String>>)
                    source.get("tags");
            List<Map<String, String>> metrics = (List<Map<String, String>>)
                    source.get("AM_nested");

            if (postQueryFiltering(tags, post_query_filter)) {
              // it's a match, use all metrics in this response
              for (Map<String, String> metric : metrics) {
                if (result.ids.size() < size) {
                  result.ids.add(buildTimeseries(metric.get("name.raw"), tags));
                }
              }
            }

            for (Map<String, String> metric : metrics) {
              if (result.ids.size() < size) {
                if (postQueryFiltering(metric.get("name.raw"),
                        post_query_filter) && (
                        postQueryFiltering(metric.get("name.raw"),
                                metrics_literal_filter) ||
                                postQueryFiltering(metric.get("name.raw"),
                                        metrics_regex_filter))) {
                  result.ids.add(buildTimeseries(metric.get("name.raw"), tags));

                }
              }
            }
          }

          LOG.info("Time took to filter out results == " + (System
                  .currentTimeMillis() - startTime) + " ms");

          long start_time_agg = System.currentTimeMillis();
          List<Aggregation> aggregations = response.getAggregations().asList();
          for (Aggregation aggregation : aggregations) {
            aggregate(aggregation, result);
          }

          LOG.info("Time took for aggregation == " + (System
                  .currentTimeMillis() - start_time_agg) + " ms");
        }

        result.ids.addAll(ids);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Total meta results: " + result.ids.size());
        }
        if (child != null) {
          child.setSuccessTags()
                  .setTag("result", result.result().toString())
                  .finish();
        }
        return result;
      }

    }

    class ErrorCB implements Callback<MetaDataStorageResult, Exception> {
      @Override
      public MetaDataStorageResult call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags(ex)
                  .finish();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Returning exception from ES", ex);
        }
        return new YmsResult(ex);
      }
    }

    return native_client.runQuery(search_source_builder, query.namespace()
            .toLowerCase(), child)
            .addCallback(new ResultCB2())
            .addErrback(new ErrorCB());


  }

  @Override
  public Deferred<MetaDataStorageResult> runQuery(QueryPipelineContext
                                                            queryPipelineContext, TimeSeriesDataSourceConfig timeSeriesDataSourceConfig, Span span) {
    return null;
  }

  private void aggregate(final Aggregation aggregation, YmsResult result) {
    InternalAggregations nestedMetricsAgg = ((InternalNested) aggregation)
            .getAggregations();
    for (Aggregation aggregation1 : nestedMetricsAgg.asList()) {
      if (aggregation1 instanceof StringTerms) {
        for (Terms.Bucket bucket : ((StringTerms) aggregation1).getBuckets()) {
          if (aggregation1.getName().equalsIgnoreCase("unique_tags.key_keys")) {
            if (!result.tags.containsKey(bucket.getKey())) {
              result.tags.put(bucket.getKey(), null);
            }
          }
          if (aggregation1.getName().equalsIgnoreCase("unique_tags" +
                  ".value_keys")) {
            result.tags.put(bucket.getKey(), new ArrayList<>());
            for (Aggregation tag_values : bucket.getAggregations()) {
              List<Aggregation> unique_tag_values = ((InternalFilter)
                      tag_values).getAggregations().asList();
              for (Aggregation unique_tag_value : unique_tag_values) {
                if (unique_tag_value instanceof StringTerms) {
                  for (Terms.Bucket tag_val_bucket : ((StringTerms)
                          unique_tag_value).getBuckets()) {
                    result.tags.get(bucket.getKey()).add(tag_val_bucket
                            .getKey());
                  }
                }
              }
            }
          }
        }
      } else {
        InternalAggregations filterAggregations = ((InternalFilter)
                aggregation1).getAggregations();
        for (Aggregation aggregation2 : filterAggregations.asList()) {
          for (Terms.Bucket bucket : ((StringTerms) aggregation2).getBuckets
                  ()) {
            if (aggregation2.getName().equalsIgnoreCase("unique_metrics")) {
              result.metrics.add(bucket.getKey());
            } else {
              result.tags.put(bucket.getKey(), null);
              if (aggregation2.getName().equalsIgnoreCase("unique_tags" +
                      ".value_keys")) {
                result.tags.put(bucket.getKey(), new ArrayList<>());
                for (Aggregation tag_values : bucket.getAggregations()) {
                  List<Aggregation> unique_tag_values = ((InternalFilter)
                          tag_values).getAggregations().asList();
                  for (Aggregation unique_tag_value : unique_tag_values) {
                    if (unique_tag_value instanceof StringTerms) {
                      for (Terms.Bucket tag_val_bucket : ((StringTerms)
                              unique_tag_value).getBuckets()) {
                        result.tags.get(bucket.getKey()).add(tag_val_bucket
                                .getKey());
                      }
                    }
                  }
                }
              }
            }

          }
        }
      }
    }
    // }
  }

  private TimeSeriesId buildTimeseries(final String metric, final
  List<Map<String, String>> tags) {
    final BaseTimeSeriesStringId.Builder builder =
            BaseTimeSeriesStringId.newBuilder();
    builder.setMetric(metric);
    for (final Map<String, String> pair : tags) {
      builder.addTags(pair.get("key.raw"), pair.get("value.raw"));
    }
    BaseTimeSeriesStringId timeseries = builder.build();
    return timeseries;
  }


  private boolean postQueryFiltering(final List<Map<String, String>> tags,
                                     final List<String> regexpFilter) {
    List<Boolean> matched = new ArrayList<>();
    if (regexpFilter.size() == 0) {
      return true;
    }
    for (String regex : regexpFilter) {
      for (final Map<String, String> tag : tags) {
        if (tag.get("key.raw").toLowerCase().matches(regex) || tag.get("value" +
                ".raw").toLowerCase().matches(regex)) {
          matched.add(Boolean.TRUE);
        }
      }
    }
    if (matched.size() == regexpFilter.size()) {
      return true;
    } else return false;
  }


  private boolean postQueryFiltering(final String metric, final List<String>
          filters) {

    List<Boolean> matched = new ArrayList<>();
    if (filters.size() == 0) {
      return true;
    }
    for (String filter : filters) {
      if (metric.matches(filter)) {
        matched.add(Boolean.TRUE);
      }
    }
    if (matched.size() == filters.size()) {
      return true;
    } else return false;
  }


  SearchSourceBuilder buildQuery(final QueryFilter filter, MetaQuery
          .AggregationField aggregate, String optAggValue, int from, int to) {
    SearchSourceBuilder search_source_builder = null;
    if (filter instanceof ChainFilter && ((ChainFilter) filter).getOp() ==
            FilterOp.AND) {
      List<QueryFilter> filters = ((ChainFilter) filter).getFilters();
      NamespacedAggregatedDocumentQuery.NamespacedAggregatedDocumentQueryBuilder es_query_builder = NamespacedAggregatedDocumentQuery.newBuilder();
      es_query_builder = es_query_builder.setQuery_filter(filters);
      if (aggregate == MetaQuery.AggregationField.ALL) {
        es_query_builder.addAggregate(filters, MetaQuery.AggregationField
                .METRICS, optAggValue);
        es_query_builder.addAggregate(filters, MetaQuery.AggregationField
                .TAGS_KEYS, optAggValue);
        es_query_builder.addAggregate(filters, MetaQuery.AggregationField
                .TAGS_VALUES, optAggValue);
      } else {
        es_query_builder.addAggregate(filters, aggregate, optAggValue);
      }
      search_source_builder = es_query_builder.build();
      search_source_builder.from(from);
      int size = to - from;
      search_source_builder.size(size);
      search_source_builder.timeout(tsdb.getConfig().getString
              (QUERY_TIMEOUT_KEY));

    } else {
      throw new IllegalArgumentException("Not supported. Please use a " +
              "ChainFilter with AND op");
    }
    return search_source_builder;
  }

  public class YmsResult implements MetaDataStorageResult {

    final List<TimeSeriesId> ids;
    final Throwable throwable;
    final List<String> metrics;
    final Map<String, List<String>> tags;
    MetaResult result;

    YmsResult() {
      ids = Lists.newArrayList();
      metrics = Lists.newArrayList();
      tags = new HashMap<>();
      throwable = null;
    }

    YmsResult(final Throwable throwable) {
      ids = null;
      metrics = null;
      tags = null;
      this.throwable = throwable;
    }

    @Override
    public MetaResult result() {
      if (result != null) {
        return result;
      }

      if (throwable != null) {
        return fallback_on_exception ? MetaResult.EXCEPTION_FALLBACK :
                MetaResult.EXCEPTION;
      }

      if (ids.isEmpty()) {
        return fallback_on_no_data ? MetaResult.NO_DATA_FALLBACK :
                MetaResult.NO_DATA;
      }
      return MetaResult.DATA;
    }

    @Override
    public Throwable exception() {
      return throwable;
    }

    @Override
    public List<TimeSeriesId> timeSeries() {
      return ids;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public List<String> metrics() {
      return metrics;
    }

    @Override
    public Map<String, List<String>> tags() {
      return tags;
    }

  }


}