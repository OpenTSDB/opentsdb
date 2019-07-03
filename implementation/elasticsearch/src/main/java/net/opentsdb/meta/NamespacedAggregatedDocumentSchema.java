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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
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
import org.elasticsearch.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
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
  public static final String FALLBACK_ON_EX_KEY = "es.fallback.exception";
  public static final String MAX_CARD_KEY = "tsd.meta.max.cardinality";
  public static final String TAGS_STRING = "tags";
  public static final String CLIENT_ID = "client.id";
  public static final String KEY_PREFIX = "meta.schema.";

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

    if (!tsdb.getConfig().hasProperty(getConfigKey(MAX_CARD_KEY))) {
      tsdb.getConfig().register(getConfigKey(MAX_CARD_KEY), 4096, true,
              "The maximum number of entries to allow for multi-get queries.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(FALLBACK_ON_EX_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(FALLBACK_ON_EX_KEY),
              true,
              true,
              "Whether or not to fall back to scans when the meta "
                  + "query returns an exception.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(CLIENT_ID))) {
      tsdb.getConfig().register(getConfigKey(CLIENT_ID), null, false, "Meta client id.");
    }

    String clientID = tsdb.getConfig().getString(getConfigKey(CLIENT_ID));
    client = tsdb.getRegistry().getPlugin(MetaClient.class, clientID);
    if (client == null) {
      throw new IllegalStateException("No client found with id: " + clientID);
    }
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  private String getConfigKey(final String key) {
    return KEY_PREFIX + (id.equals(TYPE) ? "" : id + ".") + key;
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
        return results.parse(query, tsdb, null, false, child);
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

      class ResultCB implements Callback<MetaDataStorageResult, MetaResponse > {

        @Override
        public MetaDataStorageResult call(final MetaResponse results) {

          Map<NamespacedKey, MetaDataStorageResult> parse = results.parse(query, tsdb, null, true, child);
          return parse.entrySet().iterator().next().getValue();
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
              tsdb.getConfig().getBoolean(getConfigKey(ESClusterClient.FALLBACK_ON_EX_KEY)) ?
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

  @Override
  public MetaQuery parse(TSDB tsdb, ObjectMapper mapper, JsonNode jsonNode, QueryType type) {
    return client.parse(tsdb, mapper, jsonNode, type);
  }

}
