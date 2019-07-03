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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.meta.MetaDataStorageResult.MetaResult;
import net.opentsdb.meta.impl.MetaClient;
import net.opentsdb.meta.impl.MetaResponse;
import net.opentsdb.meta.impl.es.ESClusterClient;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.MetricLiteralFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.stats.Span;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.search.SearchHit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

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

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private TSDB tsdb;

  /** The elastic search client to use */
  private MetaClient client;

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.id = Strings.isNullOrEmpty(id) ? TYPE : id;
    this.tsdb = tsdb;
    client = tsdb.getRegistry().getPlugin(MetaClient.class, null);
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

  protected static int countTagValueFilters(final QueryFilter filter, int count) {
    if (filter instanceof MetricFilter) {
      return count;
    } if (filter instanceof ExplicitTagsFilter) {
      return countTagValueFilters(((ExplicitTagsFilter) filter).getFilter(), count);
    } if (filter instanceof TagKeyFilter) {
      return count;
    } if (filter instanceof TagValueFilter) {
      count ++;
      return count;
    } if (filter instanceof NotFilter) {
      return count;
    }
    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        count = countTagValueFilters(sub_filter, count);
      }
    }
    return count;
  }

  @Override
  public Deferred<Map<NamespacedKey, MetaDataStorageResult>> runQuery(final BatchMetaQuery query,
                                                          final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
              .start();
    } else {
      child = null;
    }

    net.opentsdb.meta.impl.MetaQuery metaQuery = client.buildQuery(query);

    if (LOG.isTraceEnabled()) {
      try {
        LOG.trace("Running Meta Query: " + OBJECT_MAPPER.writeValueAsString(metaQuery));
      } catch (JsonProcessingException e) {
        LOG.error("Error parsing meta query");
      }
    }

    class ResultCB implements Callback<Map<NamespacedKey, MetaDataStorageResult>,
        MetaResponse> {

      @Override
      public Map<NamespacedKey, MetaDataStorageResult> call(final MetaResponse results) {
        return results.parse(query, tsdb, child);
      }

    }

    class ErrorCB implements Callback<Map<NamespacedKey, MetaDataStorageResult>,
            Exception> {
      @Override
      public Map<NamespacedKey, MetaDataStorageResult> call(final Exception ex) throws Exception {
        if (child != null) {
          child.setErrorTags(ex)
                  .finish();
        }
        if (LOG.isDebugEnabled()) {
          LOG.debug("Returning exception from ES", ex);
        }
        Map<NamespacedKey, MetaDataStorageResult> final_result = new LinkedHashMap<>();
        final_result.put(new NamespacedKey("EXCEPTION", "-1"),new NamespacedAggregatedDocumentResult
                (MetaResult.EXCEPTION, ex, query));

        return final_result;
      }
    }

    return client.runQuery(metaQuery,
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

      net.opentsdb.meta.impl.MetaQuery metaQuery = client.buildMultiGetQuery(query);

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
        LOG.trace("Running ES Query: " + metaQuery.toString());
      }

      return client.runQuery(metaQuery, queryPipelineContext, child)
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
