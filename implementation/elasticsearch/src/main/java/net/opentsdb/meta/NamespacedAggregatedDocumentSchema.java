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
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.meta.MetaDataStorageResult.MetaResult;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.*;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.stats.Span;


import net.opentsdb.utils.UniqueKeyPair;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
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
  private static final Logger LOG = LoggerFactory.getLogger(
      NamespacedAggregatedDocumentSchema.class);

  public static final String TYPE = "NamespacedAggregatedDocumentSchema";

  public static final String MAX_CARD_KEY = "tsd.meta.max.cardinality";
  public static final String QUERY_TIMEOUT_KEY = "es.query_timeout";
  public static final String FALLBACK_ON_EX_KEY = "es.fallback.exception";
  public static final String FALLBACK_ON_NO_DATA_KEY = "es.fallback.nodata";
  public static final String MAX_RESULTS_KEY = "es.query.results.max";
  public static final String TAGS_STRING = "tags";

  private TSDB tsdb;

  /** The elastic search client to use */
  private ESClient client;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    client = tsdb.getRegistry().getPlugin(ESClient.class, null);
    if (client == null) {
      throw new IllegalStateException("No client found!");
    }

    if (!tsdb.getConfig().hasProperty(QUERY_TIMEOUT_KEY)) {
      tsdb.getConfig().register(QUERY_TIMEOUT_KEY, 5000, true,
              "How long, in milliseconds, to wait for responses.");
    }
    if (!tsdb.getConfig().hasProperty(MAX_CARD_KEY)) {
      tsdb.getConfig().register(MAX_CARD_KEY, 4096, true,
              "The maximum number of entries to allow for multi-get queries.");
    }
    if (!tsdb.getConfig().hasProperty(FALLBACK_ON_EX_KEY)) {
      tsdb.getConfig().register(FALLBACK_ON_EX_KEY, true,
          true, "Whether or not to fall back to scans when the meta "
              + "query returns an exception.");
    }
    if (!tsdb.getConfig().hasProperty(FALLBACK_ON_NO_DATA_KEY)) {
      tsdb.getConfig().register(FALLBACK_ON_NO_DATA_KEY, false,
          true, "Whether or not to fall back to scans when the query "
              + "was empty.");
    }
    if (!tsdb.getConfig().hasProperty(MAX_RESULTS_KEY)) {
      tsdb.getConfig().register(MAX_RESULTS_KEY, 4096,
          true, "The maximum number of results to return in a multi-get query.");
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
  public Deferred<Map<String, MetaDataStorageResult>> runQuery(final BatchMetaQuery query,
                                                          final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
              .start();
    } else {
      child = null;
    }

    Map<String, SearchSourceBuilder> search_source_builder =
        NamespacedAggregatedDocumentQueryBuilder.newBuilder(query).build();


    if (LOG.isTraceEnabled()) {
      LOG.trace("Running ES Query: " + search_source_builder);
    }

    class ResultCB implements Callback<Map<String, MetaDataStorageResult>,
            Map<String, MultiSearchResponse>> {

      @Override
      public Map<String, MetaDataStorageResult> call(final Map<String,
              MultiSearchResponse> results) {

        final Map<String, MetaDataStorageResult> final_results = new
                LinkedHashMap<>();

        for (int i = 0; i < query.metaQueries().size(); i++) {
          long max_hits = 0;
          MetaQuery meta_query = query.metaQueries().get(i);
          NamespacedAggregatedDocumentResult result = null;
          int null_results = 0;
          for (final Map.Entry<String, MultiSearchResponse> search_response : results.entrySet()) {
            final MultiSearchResponse.Item[] responses = search_response.getValue().getResponses();
            final SearchResponse response = responses[i].getResponse();
            if (response == null) {
              LOG.warn("Null response from " + search_response.getKey() + " for query " + meta_query);
              tsdb.getStatsCollector().incrementCounter("es.client.query.nullResponse", "colo",
                  search_response.getKey());
              null_results++;
            } else {
            if (response.getHits().getTotalHits() > max_hits) {
              max_hits = response.getHits().getTotalHits();
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("Got response in " + response.getTookInMillis()
                  + "ms from " + search_response.getKey());
            }
            long startTime = System.currentTimeMillis();
            switch (query.type()) {
              case NAMESPACES:
                if (response.getAggregations() == null ||
                    response.getAggregations().get(
                        NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG) == null) {
                  break;
                }
                if (result == null) {
                  result = parseNamespaces(query, meta_query, response
                      .getAggregations()
                      .get(
                          NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG), null);
                } else {
                  parseNamespaces(query, meta_query, response.getAggregations().get(
                      NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG), result);
                }
                break;
              case METRICS:
                if (response.getAggregations() == null ||
                    response.getAggregations().get(
                        NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG) == null) {
                  break;
                }
                if (result == null) {
                  result = parseMetrics(query, meta_query, response
                      .getAggregations().get(
                          NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG), null);
                } else {
                  parseMetrics(query, meta_query, response.getAggregations().get(
                      NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG), result);
                }
                break;
              case TAG_KEYS:
                if (response.getAggregations() == null ||
                    response.getAggregations().get(
                        NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG) == null) {
                  break;
                }
                if (result == null) {
                  result = parseTagKeys(query, meta_query, response
                      .getAggregations().get(
                          NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG), null);
                } else {
                  parseTagKeys(query, meta_query, response.getAggregations().get(
                      NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG), result);
                }
                break;
              case TAG_VALUES:
                if (response.getAggregations() == null ||
                    response.getAggregations().get(
                        NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG) == null) {
                  break;
                }
                if (result == null) {
                  result = parseTagValues(query, meta_query, response
                      .getAggregations().get(
                          NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG), null);
                } else {
                  parseTagValues(query, meta_query, response.getAggregations().get(
                      NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG), result);
                }
                break;
              case TAG_KEYS_AND_VALUES:
                if (response.getAggregations() == null ||
                    response.getAggregations().get(
                        NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG) == null) {
                  break;
                }
                if (result == null) {
                  result = parseTagKeysAndValues(query, meta_query, response
                      .getAggregations
                          ().get(
                          NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG), null);
                } else {
                  parseTagKeysAndValues(query, meta_query, response
                      .getAggregations().get(
                          NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG), result);
                }
              case TIMESERIES:
                if (result == null) {
                  result = parseTimeseries(query, meta_query, response, null);
                } else {
                  parseTimeseries(query, meta_query, response, result);
                }
                break;
              default:
                final_results.put(meta_query.namespace(), new
                    NamespacedAggregatedDocumentResult
                    (MetaResult
                        .NO_DATA,
                        query, meta_query));
                return final_results;
            }

            if (LOG.isTraceEnabled()) {
              LOG.trace("Time took to parse out results == " + (System
                  .currentTimeMillis() - startTime) + " ms from " + search_response.getKey());
            }
          }
        }
          if (null_results == results.size()) {
            final_results.put(meta_query.namespace(), new
                  NamespacedAggregatedDocumentResult
                  (MetaResult
                      .NO_DATA,
                      query, meta_query));
          }
          if (result == null) {
            result = new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
                    query, meta_query);
          }
          result.setTotalHits(max_hits);
          final_results.put(meta_query.namespace(), result);
        }

        if (child != null) {
          child.setSuccessTags()
                  .setTag("result", final_results.toString())
                  .finish();
        }
        return final_results;
      }

    }

    class ErrorCB implements Callback<Map<String, MetaDataStorageResult>,
            Exception> {
      @Override
      public Map<String, MetaDataStorageResult> call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags(ex)
                  .finish();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Returning exception from ES", ex);
        }
        Map<String, MetaDataStorageResult> final_result = new LinkedHashMap<>();
        final_result.put("EXCEPTION",new NamespacedAggregatedDocumentResult
                (MetaResult.EXCEPTION, ex, query));

        return final_result;
      }
    }

    return client.runQuery(search_source_builder,
        null, child)
            .addCallback(new ResultCB())
            .addErrback(new ErrorCB());
  }

  @Override
  public Deferred<MetaDataStorageResult> runQuery(
      final QueryPipelineContext queryPipelineContext,
      final TimeSeriesDataSourceConfig timeSeriesDataSourceConfig,
      final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
          .start();
    } else {
      child = null;
    }

    try {
      // only one metric per query at this point. Strip the namespace.
      String temp = timeSeriesDataSourceConfig.getMetric().getMetric();
      int idx = temp.indexOf(".");
      String namespace = temp.substring(0, idx).toLowerCase();
      final String metric = temp.substring(idx + 1).toLowerCase();

      QueryFilter filter = timeSeriesDataSourceConfig.getFilter();
      if (filter == null &&
          !Strings.isNullOrEmpty(timeSeriesDataSourceConfig.getFilterId())) {
        filter = queryPipelineContext.query().getFilter(
            timeSeriesDataSourceConfig.getFilterId());
      }

      QueryFilter buildFilter;

      final ChainFilter.Builder builder = ChainFilter
          .newBuilder()
          .setOp(FilterOp.AND)
          .addFilter(MetricLiteralFilter.newBuilder().setMetric(metric).build());
      if (filter != null) {
        builder.addFilter(filter);
      }

      if (filter instanceof ExplicitTagsFilter) {
        final ExplicitTagsFilter.Builder explicitTagBuilder =
            ExplicitTagsFilter
                .newBuilder()
                .setFilter(builder.build());
        buildFilter = explicitTagBuilder.build();
      } else {
        buildFilter = builder.build();
      }

      final MetaQuery meta_query = DefaultMetaQuery.newBuilder()
          .setNamespace(namespace)
          .setFilter(buildFilter)
          .build();

      final BatchMetaQuery query = DefaultBatchMetaQuery.newBuilder()
              .setType(QueryType.TIMESERIES)
              .setMetaQuery(Lists.newArrayList(meta_query))
              .build();

      Map<String, SearchSourceBuilder> search = NamespacedAggregatedDocumentQueryBuilder
          .newBuilder(query)
          .build();

      for (final Map.Entry<String, SearchSourceBuilder> search_entry: search.entrySet()){
        search_entry.getValue().size(tsdb.getConfig().getInt(MAX_RESULTS_KEY));
      }

      class ResultCB implements Callback<MetaDataStorageResult, Map<String,
              MultiSearchResponse> > {

        @Override
        public MetaDataStorageResult call(final Map<String, MultiSearchResponse>
                                                          results) throws Exception {
          final NamespacedAggregatedDocumentResult result;
          // quick validation
          long max_hits = 0;
          for (final Map.Entry<String, MultiSearchResponse> response_entry:
                  results.entrySet()) {
            final SearchResponse response = response_entry.getValue().getResponses()[0].getResponse();
            if (response.getHits().getTotalHits() > max_hits) {
              max_hits = response.getHits().getTotalHits();
            }

            // if we have too many results, bail out with a no-data error.
            if (max_hits > tsdb.getConfig().getInt(MAX_CARD_KEY)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Too many hits from ES: " + response.getHits().getTotalHits());
              }
              if (queryPipelineContext.query().isDebugEnabled()) {
                queryPipelineContext.queryContext().logDebug(
                    "Total hits from ES: " + max_hits
                      + " exceeded the configured limit: "
                      + tsdb.getConfig().getInt(MAX_CARD_KEY));
              }
              result = new NamespacedAggregatedDocumentResult(
                tsdb.getConfig().getBoolean(ESClusterClient.FALLBACK_ON_NO_DATA_KEY)
                  ? MetaResult.NO_DATA_FALLBACK : MetaResult.NO_DATA, null,
                query);
              result.setTotalHits(response.getHits().getTotalHits());
              if (child != null) {
                child.setSuccessTags()
                     .setTag("result", result.result().toString())
                     .finish();
              }
              return result;
            }
          }
          result = new NamespacedAggregatedDocumentResult(
              max_hits > 0 ? MetaResult.DATA :
                tsdb.getConfig().getBoolean(ESClusterClient.FALLBACK_ON_NO_DATA_KEY)
                  ? MetaResult.NO_DATA_FALLBACK : MetaResult.NO_DATA, null,
                  query);
          if (max_hits > 0) {
            for (final Map.Entry<String, MultiSearchResponse> response_entry: results.entrySet()) {
              final SearchResponse response = response_entry.getValue().getResponses()[0].getResponse();
              parseTimeseries(
                  query,
                  meta_query,
                  response,
                  timeSeriesDataSourceConfig.getMetric().getMetric(),
                  result);
            }
          }
          result.setTotalHits(max_hits);
          if (LOG.isTraceEnabled()) {
            LOG.trace("Total meta results: " + result.timeSeries().size());
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
          return new NamespacedAggregatedDocumentResult(
              tsdb.getConfig().getBoolean(ESClusterClient.FALLBACK_ON_EX_KEY) ?
                  MetaResult.EXCEPTION_FALLBACK : MetaResult.EXCEPTION, ex, null);
        }
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Running ES Query: " + search.toString());
      }

      return client.runQuery(search, queryPipelineContext, child)
          .addCallback(new ResultCB())
          .addErrback(new ErrorCB());
    } catch (Exception e) {
      LOG.error("Failed to build ES query", e);
      return Deferred.fromError(e);
    } catch (Error e) {
      LOG.error("Major booboo", e);
      throw e;
    }
  }

  NamespacedAggregatedDocumentResult parseNamespaces(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    for (final Terms.Bucket bucket : ((StringTerms) aggregation).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                query, meta_query);
      }
      result.addNamespace(bucket.getKey());
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseMetrics(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    final Aggregation metrics = ((InternalFilter) ((InternalNested)
            aggregation)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG))
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_UNIQUE);

    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                query, meta_query);
      }

      result.addMetric(new UniqueKeyPair<String, Long>(bucket.getKey(),
                bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagKeys(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    final Aggregation tag_keys_filter = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);
    if (tag_keys_filter == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    final Aggregation tag_keys = ((InternalFilter) tag_keys_filter).getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);

    if (tag_keys == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                query, meta_query);
      }

      result.addTagKeyOrValue(new UniqueKeyPair<String, Long>(bucket.getKey(),
                bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagValues(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    final Aggregation metrics = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_UNIQUE);
    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                query, meta_query);
      }

      result.addTagKeyOrValue(new UniqueKeyPair<String, Long>(bucket.getKey(),
                bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagKeysAndValues(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    final Aggregation tag_keys_filter = ((InternalNested) aggregation)
            .getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_UNIQUE);
    if (tag_keys_filter == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }
    final Aggregation tag_keys = ((InternalFilter) tag_keys_filter).getAggregations()
            .get
            (NamespacedAggregatedDocumentQueryBuilder.TAGS_UNIQUE);

    if (tag_keys == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA,
              query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                query, meta_query);
      }
      Aggregation sub = bucket.getAggregations()
          .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_AGG);
      if (sub == null || ((InternalFilter) sub).getDocCount() < 1) {
        result.addTags(new UniqueKeyPair<String, Long>(bucket.getKey(),
                bucket.getDocCount()), null);
      } else {
        sub = ((InternalFilter) sub).getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_UNIQUE);
        if (sub == null) {
          result.addTags(new UniqueKeyPair<String, Long>(bucket.getKey(),
              bucket.getDocCount()), null);
        } else {
          final List<UniqueKeyPair<String, Long>> tag_values = Lists.newArrayList();
          for (final Terms.Bucket sub_bucket : ((StringTerms) sub).getBuckets()) {
            tag_values.add(new UniqueKeyPair<String, Long>(sub_bucket.getKey(),
                sub_bucket.getDocCount()));
          }
          result.addTags(new UniqueKeyPair<String, Long>(bucket.getKey(),
                  bucket.getDocCount()), tag_values);
        }
      }

    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTimeseries(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final SearchResponse response,
      NamespacedAggregatedDocumentResult result) {
    return parseTimeseries(query, meta_query, response, null, result);
  }

  NamespacedAggregatedDocumentResult parseTimeseries(
      final BatchMetaQuery query, final MetaQuery meta_query,
      final SearchResponse response,
      final String metric,
      NamespacedAggregatedDocumentResult result) {
    for (final SearchHit hit : response.getHits().hits()) {
      final Map<String, Object> source = hit.getSource();
      List<Map<String, String>> tags = (List<Map<String, String>>)
              source.get("tags");
      List<Map<String, String>> metrics = (List<Map<String, String>>)
              source.get("AM_nested");
      if (metric == null) {
        if (metrics != null) {
          for (Map<String, String> m : metrics) {
            if (result == null) {
              result = new NamespacedAggregatedDocumentResult(MetaResult
                      .DATA, query, meta_query);
            }
            result.addTimeSeries(buildTimeseries(m.get("name.raw"), tags),
                    meta_query, m.get("name.raw"));
          }
        }
      } else {
        int idx = metric.indexOf(".");
        final String metric_only = metric.substring(idx + 1).toLowerCase();
        if (result == null) {
          result = new NamespacedAggregatedDocumentResult(MetaResult.DATA,
                  query, meta_query);
        }
          result.addTimeSeries(buildTimeseries(metric, tags), meta_query, metric_only);
      }
    }
    return result;
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
}
