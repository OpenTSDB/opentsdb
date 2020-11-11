// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

package net.opentsdb.meta.impl.es;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.TimeUnit;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.meta.BatchMetaQuery;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.meta.MetaDataStorageResult;
import net.opentsdb.meta.MetaQuery;
import net.opentsdb.meta.NamespacedAggregatedDocumentQueryBuilder;
import net.opentsdb.meta.NamespacedAggregatedDocumentResult;
import net.opentsdb.meta.NamespacedKey;
import net.opentsdb.meta.MetaDataStorageResult.MetaResult;
import net.opentsdb.meta.impl.MetaResponse;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.ExplicitTagsFilter;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.NestedQueryFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.query.filter.TagKeyFilter;
import net.opentsdb.query.filter.TagValueFilter;
import net.opentsdb.stats.Span;
import net.opentsdb.utils.UniqueKeyPair;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.collect.Sets;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.bucket.filter.InternalFilter;
import org.elasticsearch.search.aggregations.bucket.nested.InternalNested;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ESMetaResponse implements MetaResponse {

  private static Logger LOGGER = LoggerFactory.getLogger(ESMetaResponse.class);

  public final Map<String, MultiSearchResponse> response;

  public ESMetaResponse(final Map<String, MultiSearchResponse> response) {
    this.response = response;
  }

  @Override
  public Map<NamespacedKey, MetaDataStorageResult> parse(
      final BatchMetaQuery query,
      final TSDB tsdb,
      final QueryPipelineContext context,
      final boolean isMultiGet,
      final int max_cardinality,
      final boolean fallback_on_no_data,
      final Span child) {
    final Map<NamespacedKey, MetaDataStorageResult> final_results = new LinkedHashMap<>();

    if (isMultiGet) {
      // NOTE: For multigets, case matters so we have to match with the original query.
      final NamespacedAggregatedDocumentResult result;

      MetaQuery metaQuery = query.metaQueries().get(0);
      String metric = findMetric(metaQuery.filter());
      NamespacedKey namespacedKey = new NamespacedKey(metaQuery.namespace(), metaQuery.id());
      // quick validation
      long max_hits = 0;
      for (final Map.Entry<String, MultiSearchResponse> response_entry : response.entrySet()) {
        if (response_entry.getValue() == null) {
          LOGGER.error("Null entry for response: " + response_entry.getKey());
          continue;
        }
        if (response_entry.getValue().getResponses() == null || 
            response_entry.getValue().getResponses().length < 1 || 
            response_entry.getValue().getResponses()[0] == null) {
          LOGGER.error("Null or empty responses for response: " + response_entry.getKey());
          continue;
        }
        
        final SearchResponse response = response_entry.getValue().getResponses()[0].getResponse();
        if (response == null) {
          LOGGER.error("Null SearchResponse at index 0 for response: " + response_entry.getKey());
          continue;
        }
        if (response.getHits() == null) {
          LOGGER.error("Null hits response, something wrong with the client?: " 
              + response_entry.getKey());
          continue;
        }
        if (response.getHits().getTotalHits() > max_hits) {
          max_hits = response.getHits().getTotalHits();
        }
        tsdb.getStatsCollector().incrementCounter("es.cluster.documents.totalHits", 
            response.getHits().getTotalHits(), "cluster", response_entry.getKey());
        // if we have too many results, bail out with a no-data error.
        if (max_hits > max_cardinality) {
          tsdb.getStatsCollector().incrementCounter("es.documents.tooManyHits", (String[]) null);
          if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("Too many hits from ES: " + response.getHits().getTotalHits());
          }
          if (context != null && context.query().isDebugEnabled()) {
            context
                .queryContext()
                .logDebug(
                    "Total hits from ES: "
                        + max_hits
                        + " exceeded the configured limit: "
                        + max_cardinality);
          }
          result =
              new NamespacedAggregatedDocumentResult(fallback_on_no_data
                      ? MetaDataStorageResult.MetaResult.NO_DATA_FALLBACK
                      : MetaDataStorageResult.MetaResult.NO_DATA,
                  null,
                  query);
          result.setTotalHits(response.getHits().getTotalHits());
          if (child != null) {
            child.setSuccessTags().setTag("result", result.result().toString()).finish();
          }
          final_results.put(namespacedKey, result);
          return final_results;
        }
      }
      
      result =
          new NamespacedAggregatedDocumentResult(
              max_hits > 0
                  ? MetaDataStorageResult.MetaResult.DATA
                  : fallback_on_no_data
                      ? MetaDataStorageResult.MetaResult.NO_DATA_FALLBACK
                      : MetaDataStorageResult.MetaResult.NO_DATA,
              null,
              query);
      if (max_hits > 0) {
        for (final Map.Entry<String, MultiSearchResponse> response_entry : response.entrySet()) {
          final SearchResponse response = response_entry.getValue().getResponses()[0].getResponse();
          if (response.getFailedShards() > 0) {
            LOGGER.warn("Failing shards from host " + response_entry.getKey() + ": " + response.getFailedShards());
          }
          parseTimeseries(query, metaQuery, response, metric, isMultiGet, result);
        }
        // if result if empty after we process all our clusters, set to NO_DATA.
        if (isMultiGet && result.timeSeries().isEmpty()) {
          result.resetResult(MetaResult.NO_DATA);
        }

      }
      tsdb.getStatsCollector().incrementCounter("es.documents.totalHits", max_hits, (String[]) null);
      result.setTotalHits(max_hits);
      if (LOGGER.isTraceEnabled()) {
        LOGGER.trace("Total meta results: " + result.timeSeries().size());
      }

      final_results.put(namespacedKey, result);

    } else {
      int i = 0;
      for (MetaQuery meta_query : query.metaQueries()) {
        long max_hits = 0;

        int count = countMetricFilters(meta_query.filter(), 0);

        count = count == 0 ? 1 : count;

        NamespacedAggregatedDocumentResult result = null;
        int null_results = 0;
        for (final Map.Entry<String, MultiSearchResponse> search_response : response.entrySet()) {
          final MultiSearchResponse.Item[] responses = search_response.getValue().getResponses();

          Set<UniqueKeyPair> tag_keys =
              null; // will be initialized if we have multiple metricliteral's
          // doing an AND.
          SearchResponse response = null;
          if (count == 0) {
            response = responses[i].getResponse();
          }
          for (int k = i; k < responses.length; k++) { // we have one query per metric so go through them accordingly
            response = responses[k].getResponse();

            if (response == null) {
              LOGGER.warn(
                  "Null response from " + search_response.getKey() + " for query " + meta_query);
              tsdb.getStatsCollector()
                  .incrementCounter("es.client.query.nullResponse", "colo",
                      search_response.getKey());
              null_results++;
            } else {
              if (response.getHits().getTotalHits() > max_hits) {
                max_hits = response.getHits().getTotalHits();
              }
              tsdb.getStatsCollector().incrementCounter("es.cluster.documents.totalHits", 
                  response.getHits().getTotalHits(), "cluster", search_response.getKey());

              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Got response in "
                        + response.getTookInMillis()
                        + "ms from "
                        + search_response.getKey());
              }

              tsdb.getStatsCollector()
                  .addTime("es.client.query.es.latency", response.getTookInMillis(), ChronoUnit.MILLIS,
                      "es_colo", search_response.getKey(),
                      "namespace", query.type() == QueryType.NAMESPACES ? "all_namespaces": meta_query.namespace(),
                      "type", query.type().toString());

              long startTime = System.currentTimeMillis();
              switch (query.type()) {
                case NAMESPACES:
                  if (response.getAggregations() == null
                      || response
                      .getAggregations()
                      .get(NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG)
                      == null) {
                    break;
                  }
                  if (result == null) {
                    result =
                        parseNamespaces(
                            query,
                            meta_query,
                            response
                                .getAggregations()
                                .get(NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG),
                            null);
                  } else {
                    parseNamespaces(
                        query,
                        meta_query,
                        response
                            .getAggregations()
                            .get(NamespacedAggregatedDocumentQueryBuilder.NAMESPACE_AGG),
                        result);
                  }
                  break;
                case METRICS:
                  if (response.getAggregations() == null
                      || response
                      .getAggregations()
                      .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG)
                      == null) {
                    break;
                  }
                  if (result == null) {
                    result =
                        parseMetrics(
                            query,
                            meta_query,
                            response
                                .getAggregations()
                                .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG),
                            null);
                  } else {
                    parseMetrics(
                        query,
                        meta_query,
                        response
                            .getAggregations()
                            .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG),
                        result);
                  }
                  break;
                case TAG_KEYS:
                  if (response.getAggregations() == null
                      || response
                      .getAggregations()
                      .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG)
                      == null) {
                    break;
                  }
                  if (count > 1) { // we need to do an intersection.
                    Aggregation aggregation =
                        response
                            .getAggregations()
                            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG);
                    if (tag_keys == null) {
                      tag_keys = getTagKeysSet(aggregation);
                    } else {
                      tag_keys.retainAll(getTagKeysSet(aggregation));
                    }

                  } else { // just iterate over the buckets and put them in result
                    if (result == null) {
                      result =
                          parseTagKeys(
                              query,
                              meta_query,
                              response
                                  .getAggregations()
                                  .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG),
                              null);
                    } else {
                      parseTagKeys(
                          query,
                          meta_query,
                          response
                              .getAggregations()
                              .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_AGG),
                          result);
                    }
                  }
                  break;
                case TAG_VALUES:
                  if (response.getAggregations() == null
                      || response
                      .getAggregations()
                      .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG)
                      == null) {
                    break;
                  }
                  if (result == null) {
                    result =
                        parseTagValues(
                            query,
                            meta_query,
                            response
                                .getAggregations()
                                .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG),
                            null);
                  } else {
                    parseTagValues(
                        query,
                        meta_query,
                        response
                            .getAggregations()
                            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_AGG),
                        result);
                  }
                  break;
                case TAG_KEYS_AND_VALUES:
                case BASIC:
                  if (response.getAggregations() == null
                      || response
                      .getAggregations()
                      .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG)
                      == null) {
                    break;
                  }
                  if (result == null) {
                    result =
                        parseTagKeysAndValues(
                            query,
                            meta_query,
                            response
                                .getAggregations()
                                .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG),
                            null);
                  } else {
                    parseTagKeysAndValues(
                        query,
                        meta_query,
                        response
                            .getAggregations()
                            .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_AGG),
                        result);
                  }
                case TIMESERIES:
                  if (result == null) {
                    result = parseTimeseries(query, meta_query, response, isMultiGet, null);
                  } else {
                    parseTimeseries(query, meta_query, response, isMultiGet, result);
                  }
                  break;
                default:
                  final_results.put(
                      new NamespacedKey(meta_query.namespace(), meta_query.id()),
                      new NamespacedAggregatedDocumentResult(
                          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query));
                  return final_results;
              }

              if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(
                    "Time took to parse out results == "
                        + (System.currentTimeMillis() - startTime)
                        + " ms from "
                        + search_response.getKey());
              }
            }
          }
          if (tag_keys != null) {
            for (UniqueKeyPair tag : tag_keys) {
              if (result == null) {
                result =
                    new NamespacedAggregatedDocumentResult(
                        MetaDataStorageResult.MetaResult.DATA, query, meta_query);
              }
              result.addTagKeyOrValue(tag);
            }
          }
        }
        if (null_results == response.size()) {
          final_results.put(
              new NamespacedKey(meta_query.namespace(), meta_query.id()),
              new NamespacedAggregatedDocumentResult(
                  MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query));
        }
        if (result == null) {
          result =
              new NamespacedAggregatedDocumentResult(
                  MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
        }
        result.setTotalHits(max_hits);
        final_results.put(new NamespacedKey(meta_query.namespace(), meta_query.id()), result);
        i = i + count;
      }
    }

    if (child != null) {
      child.setSuccessTags().setTag("result", final_results.toString()).finish();
    }
    return final_results;
  }

  private int countMetricFilters(final QueryFilter filter, int count) {
    if (filter instanceof MetricFilter) {
      count++;
      return count;
    }
    if (filter instanceof ExplicitTagsFilter) {
      return countMetricFilters(((ExplicitTagsFilter) filter).getFilter(), count);
    }
    if (filter instanceof TagKeyFilter) {
      return count;
    }
    if (filter instanceof TagValueFilter) {
      return count;
    }
    if (filter instanceof NotFilter) {
      return count;
    }
    if (filter instanceof ChainFilter) {
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        count = countMetricFilters(sub_filter, count);
      }
    }
    return count;
  }

  private NamespacedAggregatedDocumentResult parseNamespaces(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    for (final Terms.Bucket bucket : ((StringTerms) aggregation).getBuckets()) {
      if (result == null) {
        result =
            new NamespacedAggregatedDocumentResult(
                MetaDataStorageResult.MetaResult.DATA, query, meta_query);
      }
      result.addNamespace(bucket.getKey());
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseMetrics(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    final Aggregation metrics =
        ((InternalFilter)
                ((InternalNested) aggregation)
                    .getAggregations()
                    .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_AGG))
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.METRIC_UNIQUE);

    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result =
            new NamespacedAggregatedDocumentResult(
                MetaDataStorageResult.MetaResult.DATA, query, meta_query);
      }

      result.addMetric(new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagKeys(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    final Aggregation tag_keys_filter =
        ((InternalNested) aggregation)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);
    if (tag_keys_filter == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    final Aggregation tag_keys =
        ((InternalFilter) tag_keys_filter)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);

    if (tag_keys == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      if (result == null) {
        result =
            new NamespacedAggregatedDocumentResult(
                MetaDataStorageResult.MetaResult.DATA, query, meta_query);
      }

      result.addTagKeyOrValue(
          new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()));
    }
    return result;
  }

  Set<UniqueKeyPair> getTagKeysSet(final Aggregation aggregation) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      return new HashSet<>();
    }

    final Aggregation tag_keys_filter =
        ((InternalNested) aggregation)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);
    if (tag_keys_filter == null) {
      return new HashSet<>();
    }

    final Aggregation tag_keys =
        ((InternalFilter) tag_keys_filter)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_KEY_UNIQUE);

    if (tag_keys == null) {
      return new HashSet<>();
    }

    Set<UniqueKeyPair> tag_key = new HashSet<>();
    for (final Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      tag_key.add(new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()));
    }
    return tag_key;
  }

  NamespacedAggregatedDocumentResult parseTagValues(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    final Aggregation metrics =
        ((InternalNested) aggregation)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAG_VALUE_UNIQUE);
    if (metrics == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) metrics).getBuckets()) {
      if (result == null) {
        result =
            new NamespacedAggregatedDocumentResult(
                MetaDataStorageResult.MetaResult.DATA, query, meta_query);
      }

      result.addTagKeyOrValue(
          new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()));
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTagKeysAndValues(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final Aggregation aggregation,
      NamespacedAggregatedDocumentResult result) {
    if (((InternalNested) aggregation).getDocCount() <= 0) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    final Aggregation tag_keys_filter =
        ((InternalNested) aggregation)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_UNIQUE);
    if (tag_keys_filter == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }
    final Aggregation tag_keys =
        ((InternalFilter) tag_keys_filter)
            .getAggregations()
            .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_UNIQUE);

    if (tag_keys == null) {
      if (result != null) {
        return result;
      }
      return new NamespacedAggregatedDocumentResult(
          MetaDataStorageResult.MetaResult.NO_DATA, query, meta_query);
    }

    for (final Terms.Bucket bucket : ((StringTerms) tag_keys).getBuckets()) {
      if (Strings.isNullOrEmpty(query.aggregationField()) ||
              query.aggregationField().equalsIgnoreCase(".*") ||
              bucket.getKey().equals(query.aggregationField())) {
        if (result == null) {
          result =
              new NamespacedAggregatedDocumentResult(
                  MetaDataStorageResult.MetaResult.DATA, query, meta_query);
        }
        Aggregation sub =
            bucket.getAggregations().get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_AGG);
        if (sub == null || ((InternalFilter) sub).getDocCount() < 1) {
          result.addTags(
              new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()), null);
        } else {
          sub =
              ((InternalFilter) sub)
                  .getAggregations()
                  .get(NamespacedAggregatedDocumentQueryBuilder.TAGS_SUB_UNIQUE);
          if (sub == null) {
            result.addTags(
                new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()), null);
          } else {
            final List<UniqueKeyPair<String, Long>> tag_values = Lists.newArrayList();
            for (final Terms.Bucket sub_bucket : ((StringTerms) sub).getBuckets()) {
              tag_values.add(
                  new UniqueKeyPair<String, Long>(sub_bucket.getKey(), sub_bucket.getDocCount()));
            }
            result.addTags(
                new UniqueKeyPair<String, Long>(bucket.getKey(), bucket.getDocCount()), tag_values);
          }
        }
      }
    }
    return result;
  }

  NamespacedAggregatedDocumentResult parseTimeseries(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final SearchResponse response,
      final boolean isMultiGet,
      NamespacedAggregatedDocumentResult result) {
      return parseTimeseries(query, meta_query, response, null, isMultiGet, result);
  }
  
  NamespacedAggregatedDocumentResult parseTimeseries(
      final BatchMetaQuery query,
      final MetaQuery meta_query,
      final SearchResponse response,
      final String metric,
      final boolean isMultiGet,
      NamespacedAggregatedDocumentResult result) {
    for (final SearchHit hit : response.getHits().hits()) {
      final Map<String, Object> source = hit.getSource();
      List<Map<String, String>> tags = (List<Map<String, String>>) source.get("tags");
      List<Map<String, String>> metrics = (List<Map<String, String>>) source.get("AM_nested");
      if (metric == null) {
        if (metrics != null) {
          for (Map<String, String> m : metrics) {
            if (result == null) {
              result =
                  new NamespacedAggregatedDocumentResult(
                      MetaDataStorageResult.MetaResult.DATA, query, meta_query);
            }
            
            final TimeSeriesId id = buildTimeseries(
                meta_query.namespace() + "." + m, tags);
            if (isMultiGet) {
              if (!FilterUtils.matchesTags(meta_query.filter(), 
                  ((TimeSeriesStringId) id).tags(), Sets.newHashSet())) {
                if (LOGGER.isTraceEnabled()) {
                  LOGGER.trace("Dropping ES meta response " + id 
                      + " as it doesn't match our tags.");
                }
                continue;
              }
            }
            result.addTimeSeries(id, meta_query, m.get("name.raw"), true);
          }
        }
      } else {
        if (result == null) {
          result =
              new NamespacedAggregatedDocumentResult(
                  MetaDataStorageResult.MetaResult.DATA, query, meta_query);
        }
        final TimeSeriesId id = buildTimeseries(
            meta_query.namespace() + "." + metric, tags);
        if (isMultiGet) {
          if (!FilterUtils.matchesTags(meta_query.filter(), 
              ((TimeSeriesStringId) id).tags(), Sets.newHashSet())) {
            if (LOGGER.isTraceEnabled()) {
              LOGGER.trace("Dropping ES meta response " + id 
                  + " as it doesn't match our tags.");
            }
            continue;
          }
        }
        result.addTimeSeries(id, meta_query, metric, false);
      }
    }

    return result;
  }

  private String findMetric(final QueryFilter filter) {
    if (filter instanceof NestedQueryFilter) {
      return findMetric(((NestedQueryFilter) filter).getFilter());
    } else if (filter instanceof ChainFilter) {
      for (final QueryFilter child : ((ChainFilter) filter).getFilters()) {
        final String metric = findMetric(child);
        if (!Strings.isNullOrEmpty(metric)) {
          return metric;
        }
      }
    } else if (filter instanceof MetricFilter) {
      // TODO - regex?
      return ((MetricFilter) filter).getMetric();
    }
    return null;
  }
  
  private TimeSeriesId buildTimeseries(final String metric, 
                                       final List<Map<String, String>> tags) {
    final BaseTimeSeriesStringId.Builder builder = 
        BaseTimeSeriesStringId.newBuilder()
          .setMetric(metric);
    for (final Map<String, String> pair : tags) {
      builder.addTags(pair.get("key.raw"), pair.get("value.raw"));
    }
    BaseTimeSeriesStringId timeseries = builder.build();
    return timeseries;
  }
}
