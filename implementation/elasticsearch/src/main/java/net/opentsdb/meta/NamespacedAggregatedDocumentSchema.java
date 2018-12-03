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
import net.opentsdb.meta.MetaQuery.QueryType;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.*;
import net.opentsdb.query.filter.ChainFilter.FilterOp;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.Pair;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.base.Strings;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
  public Deferred<MetaDataStorageResult> runQuery(final MetaQuery query,
                                                  final Span span) {
    final Span child;
    if (span != null) {
      child = span.newChild(getClass().getSimpleName() + ".runQuery")
              .start();
    } else {
      child = null;
    }
    
    SearchSourceBuilder search_source_builder = 
        NamespacedAggregatedDocumentQueryBuilder.newBuilder(query).build();
    search_source_builder.timeout(tsdb.getConfig().getString
        (QUERY_TIMEOUT_KEY));

    if (LOG.isDebugEnabled()) {
      LOG.debug("Running Elastic Search =" + search_source_builder.toString());
    }

    class ResultCB implements Callback<MetaDataStorageResult,
            List<SearchResponse>> {

      @Override
      public MetaDataStorageResult call(final List<SearchResponse> results)
              throws Exception {
        NamespacedAggregatedDocumentResult result = null;
        int size = query.to() - query.from();
        search_source_builder.size(size);
        
        long max_hits = 0;
        for (final SearchResponse response : results) {
          if (response.getHits().getTotalHits() > max_hits) {
            max_hits = response.getHits().getTotalHits();
          }

          if (LOG.isTraceEnabled()) {
            LOG.trace("Got response in " + response.getTookInMillis() + "ms");
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
              result = parseNamespaces(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG), null);
            } else {
              parseNamespaces(query, response.getAggregations().get(
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
              result = parseMetrics(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG), null);
            } else {
              parseMetrics(query, response.getAggregations().get(
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
              result = parseTagKeys(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG), null);
            } else {
              parseTagKeys(query, response.getAggregations().get(
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
              result = parseTagValues(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG), null);
            } else {
              parseTagValues(query, response.getAggregations().get(
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
              result = parseTagKeysAndValues(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG), null);
            } else {
              parseTagKeysAndValues(query, response.getAggregations().get(
                  NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG), result);
            }
          case TIMESERIES:
            if (result == null) {
              result = parseTimeseries(query, response, null);
            } else {
              parseTimeseries(query, response, result);
            }
            break;
          default:
            return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
          }
          
          if (LOG.isTraceEnabled()) {
            LOG.trace("Time took to parse out results == " + (System
                    .currentTimeMillis() - startTime) + " ms");
          }
        }

        if (result == null) {
          result = new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query); 
        }
        result.setTotalHits(max_hits);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Total meta results: " + max_hits);
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
        return new NamespacedAggregatedDocumentResult(MetaResult.EXCEPTION, ex, query);
      }
    }

    return client.runQuery(search_source_builder, 
        query.type() == QueryType.NAMESPACES ? "all_namespace" : query.namespace()
            .toLowerCase(), child)
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
      String metric = timeSeriesDataSourceConfig.getMetric().getMetric();
      int idx = metric.indexOf(".");
      String namespace = metric.substring(0, idx).toLowerCase();
      metric = metric.substring(idx + 1);
      
      // build query
      final QueryBuilder metric_nested_query = QueryBuilders.
          nestedQuery("AM_nested", QueryBuilders.boolQuery()          
              .must(QueryBuilders.termQuery("AM_nested.name.lowercase", 
                  metric)));
      BoolQueryBuilder bool_query = QueryBuilders.boolQuery()
          .must(metric_nested_query);
      
      QueryFilter filter = timeSeriesDataSourceConfig.getFilter();
      if (filter == null && 
          !Strings.isNullOrEmpty(timeSeriesDataSourceConfig.getFilterId())) {
        filter = queryPipelineContext.query().getFilter(
            timeSeriesDataSourceConfig.getFilterId());
      }
      
      if (filter != null) {
        bool_query.must(buildQuery(filter));
      }
      
      class ResultCB implements Callback<MetaDataStorageResult, List<SearchResponse>> {
        
        @Override
        public MetaDataStorageResult call(final List<SearchResponse> results) throws Exception {
          NamespacedAggregatedDocumentResult result;
          // quick validation
          for (final SearchResponse response : results) {
            // if we have too many results, bail out with a no-data error.
            if (response.getHits().getTotalHits() > tsdb.getConfig().getInt(MAX_CARD_KEY)) {
              if (LOG.isTraceEnabled()) {
                LOG.trace("Too many hits from ES: " + response.getHits().getTotalHits());
              }
              result = new NamespacedAggregatedDocumentResult(
                  tsdb.getConfig().getBoolean(ESClusterClient.FALLBACK_ON_NO_DATA_KEY) 
                    ? MetaResult.NO_DATA_FALLBACK : MetaResult.NO_DATA, null);
              result.setTotalHits(response.getHits().getTotalHits());
              if (child != null) {
                child.setSuccessTags()
                     .setTag("result", result.result().toString())
                     .finish();
              }
              return result;
            }
          }
          
          result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, null);
          long max_hits = 0;
          for (final SearchResponse response : results) {
            if (response.getHits().getTotalHits() > max_hits) {
              max_hits = response.getHits().getTotalHits();
            }
            for (final SearchHit hit : response.getHits().hits()) {
              final Map<String, Object> source = hit.getSource();
              final List<Map<String, String>> tags = 
                  (List<Map<String, String>>) source.get("tags");
              final BaseTimeSeriesStringId.Builder builder = 
                  BaseTimeSeriesStringId.newBuilder()
                  . setMetric(timeSeriesDataSourceConfig.getMetric().getMetric());
              for (final Map<String, String> pair : tags) {
                builder.addTags(pair.get("key.raw"), pair.get("value.raw"));
              }
              result.addTimeSeries(builder.build());
            }
          }
          result.setTotalHits(max_hits);
          
          //result.ids.addAll(ids);
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
     
      return client.runQuery(bool_query, queryPipelineContext, namespace, child)
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
      final MetaQuery query,
      final Aggregation aggregation, 
      NamespacedAggregatedDocumentResult result) {
    for (Terms.Bucket bucket : ((StringTerms) aggregation).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
      }
      result.addNamespace(bucket.getKey());
    }
    return result;
  }
  
  NamespacedAggregatedDocumentResult parseMetrics(
      final MetaQuery query,
      final Aggregation aggregation, 
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    final Aggregation metrics = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_UNIQUE);
    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    for (Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
      }
      
      result.addMetric(new Pair<String, Long>(bucket.getKey(), 
                bucket.getDocCount()));
    }
    return result;
  }
  
  NamespacedAggregatedDocumentResult parseTagKeys(
      final MetaQuery query,
      final Aggregation aggregation, 
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    final Aggregation metrics = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);
    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    for (Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
      }
      
      result.addTagKeyOrValue(new Pair<String, Long>(bucket.getKey(), 
                bucket.getDocCount()));
    }
    return result;
  }
  
  NamespacedAggregatedDocumentResult parseTagValues(
      final MetaQuery query,
      final Aggregation aggregation, 
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    final Aggregation metrics = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_UNIQUE);
    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    for (Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
      }
      
      result.addTagKeyOrValue(new Pair<String, Long>(bucket.getKey(), 
                bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagKeysAndValues(
      final MetaQuery query,
      final Aggregation aggregation, 
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    final Aggregation tag_keys = ((InternalNested) aggregation).getAggregations()
        .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_UNIQUE);
    if (tag_keys == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(MetaResult.NO_DATA, query);
    }
    
    for (Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      if (result == null) {
        result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
      }
      Aggregation sub = bucket.getAggregations()
          .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_AGG);
      if (sub == null || ((InternalFilter) sub).getDocCount() < 1) {
        result.addTags(new Pair<String, Long>(bucket.getKey(), 
                bucket.getDocCount()), null);
      } else {
        sub = ((InternalFilter) sub).getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_UNIQUE);
        if (sub == null) {
          result.addTags(new Pair<String, Long>(bucket.getKey(), 
              bucket.getDocCount()), null);
        } else {
          final List<Pair<String, Long>> tag_values = Lists.newArrayList();
          for (final Terms.Bucket sub_bucket : ((StringTerms) sub).getBuckets()) {
            tag_values.add(new Pair<String, Long>(sub_bucket.getKey(), 
                sub_bucket.getDocCount()));
          }
          result.addTags(new Pair<String, Long>(bucket.getKey(), 
                  bucket.getDocCount()), tag_values);
        }
      }
      
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTimeseries(
      final MetaQuery query,
      final SearchResponse response, 
      NamespacedAggregatedDocumentResult result) {
    for (final SearchHit hit : response.getHits().hits()) {
      final Map<String, Object> source = hit.getSource();
      List<Map<String, String>> tags = (List<Map<String, String>>)
              source.get("tags");
      List<Map<String, String>> metrics = (List<Map<String, String>>)
              source.get("AM_nested");
    
      if (metrics != null) {
        for (Map<String, String> metric : metrics) {
          if (result == null) {
            result = new NamespacedAggregatedDocumentResult(MetaResult.DATA, query);
          }
          result.addTimeSeries(buildTimeseries(metric.get("name.raw"), tags));
        }
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
  
  QueryBuilder buildQuery(final QueryFilter filter) {
    if (filter instanceof TagValueLiteralOrFilter) {
      // handles the range filter as well.
      final List<String> lower_case = Lists.newArrayListWithCapacity(
          ((TagValueLiteralOrFilter) filter).literals().size());
      for (final String tag : ((TagValueLiteralOrFilter) filter).literals()) {
        lower_case.add(tag.toLowerCase());
      }
      return QueryBuilders.nestedQuery(TAGS_STRING, QueryBuilders.boolQuery()
          .must(QueryBuilders.termsQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_VALUE_KEY, lower_case))
          .must(QueryBuilders.termQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_KEY_KEY, 
              ((TagValueLiteralOrFilter) filter).getTagKey())));
    } else if (filter instanceof TagValueRegexFilter) {
      final String regexp = NamespacedAggregatedDocumentQueryBuilder.convertToLuceneRegex(
          ((TagValueRegexFilter) filter).getFilter());
      return QueryBuilders.nestedQuery(TAGS_STRING, QueryBuilders.boolQuery()
          .must(QueryBuilders.regexpQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_VALUE_KEY, regexp))
          .must(QueryBuilders.termQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_KEY_KEY, 
              ((TagValueRegexFilter) filter).getTagKey())));
    } else if (filter instanceof TagValueWildcardFilter) {
      return QueryBuilders.nestedQuery(TAGS_STRING, QueryBuilders.boolQuery()
          .must(QueryBuilders.regexpQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_VALUE_KEY, 
              ((TagValueWildcardFilter) filter).getFilter()
                .toLowerCase().replace("*", ".*")))
          .must(QueryBuilders.termQuery(
              NamespacedAggregatedDocumentQueryBuilder.QUERY_TAG_KEY_KEY, 
              ((TagValueWildcardFilter) filter).getTagKey())));
    } else if (filter instanceof ChainFilter) {
      final ChainFilter chain = (ChainFilter) filter;
      BoolQueryBuilder tags_bool_query = QueryBuilders.boolQuery();
      for (final QueryFilter chained : chain.getFilters()) {
        QueryBuilder sub_query = buildQuery(chained);
        if (chain.getOp() == FilterOp.AND) {
          tags_bool_query.must(sub_query);
        } else {
          tags_bool_query.should(sub_query);
        }
      }
      return tags_bool_query;
    } else if (filter instanceof ExplicitTagsFilter) {
      return QueryBuilders.boolQuery().mustNot(
          buildQuery(((ExplicitTagsFilter) filter).getFilter()));
    } else if (filter instanceof NotFilter) {
      return buildQuery(((NotFilter) filter).getFilter());
    } else {
      throw new UnsupportedOperationException("No code for " 
          + filter.getClass() + " implemented yet.");
    }
  }
  
}