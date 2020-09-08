// This file is part of OpenTSDB.
// Copyright (C) 2018-2019  The OpenTSDB Authors.
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
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import java.util.LinkedHashMap;
import java.util.Map;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.meta.MetaDataStorageResult.MetaResult;
import net.opentsdb.meta.impl.MetaClient;
import net.opentsdb.meta.impl.MetaQueryMarker;
import net.opentsdb.meta.impl.MetaResponse;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.*;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.DateTime;
import org.elasticsearch.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public static final String FALLBACK_ON_EX_KEY = "fallback.exception";
  public static final String FALLBACK_ON_NO_DATA_KEY = "fallback.no_data";
  public static final String MAX_CARD_KEY = "max.cardinality";
  public static final String TAGS_STRING = "tags";
  public static final String CLIENT_ID = "client.id";
  public static final String KEY_PREFIX = "meta.schema.";
  public static final String SKIP_META = "skip.meta";

  private static ObjectMapper OBJECT_MAPPER = new ObjectMapper();
  public static final TypeReference<Map<String, String>> NAMESPACE_FILTERS =
      new TypeReference<Map<String, String>>() { };

  private static Exception skip_meta_ex = new RuntimeException("Skipping meta for this namespace");

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
    if (!tsdb.getConfig().hasProperty(getConfigKey(FALLBACK_ON_NO_DATA_KEY))) {
      tsdb.getConfig()
          .register(
              getConfigKey(FALLBACK_ON_NO_DATA_KEY),
              false,
              true,
              "Whether or not to fall back to scans when the meta "
                  + "query returns an empty data set.");
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

    if (!tsdb.getConfig().hasProperty(getConfigKey(SKIP_META))) {
      LOG.info("registering conf " + getConfigKey(SKIP_META));
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
          .setKey(getConfigKey(SKIP_META))
          .setDefaultValue(Maps.newHashMap())
          .setDescription("TODO")
          .setType(NAMESPACE_FILTERS)
          .setSource(getClass().toString())
          .isNullable()
          .isDynamic()
          .build());
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
    } if (filter instanceof AnyFieldRegexFilter) {
      count++;
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
  public Deferred<Map<NamespacedKey, MetaDataStorageResult>> runQuery(
      final BatchMetaQuery query,
      final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
              .start();
    } else {
      child = null;
    }

    MetaQueryMarker metaQuery = client.buildQuery(query);

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
        return results.parse(query,
            tsdb, 
            null, 
            false, 
            tsdb.getConfig().getInt(getConfigKey(MAX_CARD_KEY)),
            tsdb.getConfig().getBoolean(getConfigKey(FALLBACK_ON_NO_DATA_KEY)),
            child);
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

    Map<String, String> namespaces = tsdb.getConfig().getTyped(getConfigKey(SKIP_META), NAMESPACE_FILTERS);
    // only one metric per query at this point. Strip the namespace.
    String temp = timeSeriesDataSourceConfig.getMetric().getMetric();
    int idx = temp.indexOf(".");
    String namespace = temp.substring(0, idx);
    final String metric = temp.substring(idx + 1);

    if (namespaces.containsKey(namespace)) {
      String retention = namespaces.get(namespace);
      long retention_ms = DateTime.parseDuration(retention);
      if (System.currentTimeMillis() - queryPipelineContext.query().startTime().msEpoch() > retention_ms) {
        NamespacedAggregatedDocumentResult result = new NamespacedAggregatedDocumentResult(
            MetaResult.NO_DATA_FALLBACK, skip_meta_ex, null);
        return Deferred.fromResult(result);
      }
    }
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
          .start();
    } else {
      child = null;
    }

    try {

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
          .addFilter(MetricLiteralFilter.newBuilder()
              .setMetric(metric)
              .build());
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

      MetaQueryMarker metaQuery = client.buildMultiGetQuery(query);

      class ResultCB implements Callback<MetaDataStorageResult, MetaResponse > {

        @Override
        public MetaDataStorageResult call(final MetaResponse results) {
          Map<NamespacedKey, MetaDataStorageResult> parse = results.parse(
              query, 
              tsdb, 
              null, 
              true, 
              tsdb.getConfig().getInt(getConfigKey(MAX_CARD_KEY)),
              tsdb.getConfig().getBoolean(getConfigKey(FALLBACK_ON_NO_DATA_KEY)),
              child);
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
              tsdb.getConfig().getBoolean(getConfigKey(FALLBACK_ON_EX_KEY)) ?
                  MetaResult.EXCEPTION_FALLBACK : MetaResult.EXCEPTION, ex, null);
        }
      }

      if (LOG.isTraceEnabled()) {
        LOG.trace("Running Meta Query: " + metaQuery.toString());
      }

      return client.runQuery(metaQuery, queryPipelineContext, child)
          .addCallback(new ResultCB())
          .addErrback(new ErrorCB());
    } catch (Exception e) {
      LOG.error("Failed to build Meta query", e);
      return Deferred.fromError(e);
    } catch (Error e) {
      LOG.error("Major booboo", e);
      throw e;
    }
  }

  @Override
  public MetaQuery parse(final TSDB tsdb, 
                         final ObjectMapper mapper, 
                         final JsonNode jsonNode,
                         final QueryType type) {
    return client.parse(tsdb, mapper, jsonNode, type);
  }

}
