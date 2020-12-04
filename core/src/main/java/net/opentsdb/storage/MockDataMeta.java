// This file is part of OpenTSDB.
// Copyright (C) 2020 The OpenTSDB Authors.
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
package net.opentsdb.storage;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import com.stumbleupon.async.Deferred;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.data.TimeSeriesDatumStringId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.meta.BatchMetaQuery;
import net.opentsdb.meta.DefaultMetaQuery;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.meta.MetaDataStorageResult;
import net.opentsdb.meta.MetaDataStorageSchema;
import net.opentsdb.meta.MetaQuery;
import net.opentsdb.meta.NamespacedKey;
import net.opentsdb.query.QueryPipelineContext;
import net.opentsdb.query.TimeSeriesDataSourceConfig;
import net.opentsdb.query.filter.FilterUtils;
import net.opentsdb.stats.Span;
import net.opentsdb.storage.MockDataStore.MockSpan;
import net.opentsdb.utils.UniqueKeyPair;

/**
 * Ugly hacky class that implements a time series meta data query against 
 * information stored in the MockDataStore. Note that the code is horribly
 * inefficient and probably somewhat buggy so it isn't meant for production use
 * but testing tools and maybe deployments.
 * 
 * @since 3.0
 */
public class MockDataMeta extends BaseTSDBPlugin implements MetaDataStorageSchema {
  
  private volatile MockDataStore data_store;
  private String data_store_id;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    if (!tsdb.getConfig().hasProperty("MockDataStore.meta")) {
      // TODO - named configs
      tsdb.getConfig().register("MockDataStore.meta", null, false, 
          "The ID of the mock data store to query for meta if there are multiple.");
    }
    data_store_id = tsdb.getConfig().getString("MockDataStore.meta");
    return Deferred.fromResult(null);
  }
  
  @Override
  public Deferred<Map<NamespacedKey, MetaDataStorageResult>> runQuery(
        final BatchMetaQuery query, 
        final Span span) {
    if (data_store == null) {
      synchronized (this) {
        if (data_store == null) {
          final TimeSeriesDataSourceFactory factory = 
              tsdb.getRegistry().getPlugin(TimeSeriesDataSourceFactory.class, data_store_id);
          if (factory == null) {
            throw new IllegalStateException("Factory was null for: " 
                + (Strings.isNullOrEmpty(data_store_id) ? "default" : data_store_id));
          }
          if (!(factory instanceof MockDataStoreFactory)) {
            throw new IllegalStateException("Factory was not a MockDataStoreFactory: " 
                + factory.getClass());
          }
          data_store = ((MockDataStoreFactory) factory).mds();
        }
      }
    }
    Map<NamespacedKey, MetaDataStorageResult> results = Maps.newHashMap();
    switch (query.getType()) {
    case NAMESPACES:
      // we'll iterate and pull out the metrics to the first dot.
      // TODO - no filter?
      final Set<String> namespaces = Sets.newHashSet();
      int hits = 0;
      for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : 
            data_store.database().entrySet()) {
        if (!Strings.isNullOrEmpty(entry.getKey().namespace())) {
          namespaces.add(entry.getKey().namespace());
          hits++;
        } else {
          int idx = entry.getKey().metric().indexOf(".");
          if (idx < 0) {
            continue;
          }
          namespaces.add(entry.getKey().metric().substring(0, idx));
          hits++;
        }
      }
      results.put(new NamespacedKey("NA", "MockDataStore"), 
          new MockMetaResult(namespaces, hits));
      break;
    case METRICS:
    case TAG_KEYS:
    case TAG_VALUES:
    case TIMESERIES:
    case TAG_KEYS_AND_VALUES:
      handleSingleTypeMetaQuery(query, results);
      break;
    default:
      return Deferred.fromError(
          new IllegalArgumentException("Unsupported type: " + query.getType()));
    }
    return Deferred.fromResult(results);
  }

  @Override
  public Deferred<MetaDataStorageResult> runQuery(
        final QueryPipelineContext context, 
        final TimeSeriesDataSourceConfig config,
        final Span span) {
    if (data_store == null) {
      synchronized (this) {
        if (data_store == null) {
          final TimeSeriesDataSourceFactory factory = 
              tsdb.getRegistry().getPlugin(TimeSeriesDataSourceFactory.class, data_store_id);
          if (factory == null) {
            throw new IllegalStateException("Factory was null for: " 
                + (Strings.isNullOrEmpty(data_store_id) ? "default" : data_store_id));
          }
          if (!(factory instanceof MockDataStoreFactory)) {
            throw new IllegalStateException("Factory was not a MockDataStoreFactory: " 
                + factory.getClass());
          }
          data_store = ((MockDataStoreFactory) factory).mds();
        }
      }
    }
    // TODO
    return Deferred.fromError(new UnsupportedOperationException("Not implemented"));
  }

  @Override
  public MetaQuery parse(final TSDB tsdb, 
                         final ObjectMapper mapper, 
                         final JsonNode jsonNode, 
                         final QueryType type) {
    return DefaultMetaQuery.parse(tsdb, mapper, jsonNode, type).build();
  }

  @Override
  public String type() {
    return "MockDataMeta";
  }

  /**
   * Works over a single type of query and pulls out the proper information.
   * @param query The non-null query to execute.
   * @param results The non-null result map to populate.
   */
  void handleSingleTypeMetaQuery(final BatchMetaQuery query,
                                 final Map<NamespacedKey, MetaDataStorageResult> results) {
    for (final MetaQuery meta_query : query.getQueries()) {
      NamespacedKey key = null;
      int hits = 0;
      switch (query.getType()) {
      case METRICS:
        Map<String, UniqueKeyPair<String, Long>> metrics = null;
        for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : 
              data_store.database().entrySet()) {
          if (FilterUtils.matchesMetrics(meta_query.getFilter(), entry.getKey(), 
              meta_query.getNamespace())) {
            if (metrics == null) {
              metrics = Maps.newHashMap();
            }
            String m = entry.getKey().metric();
            if (entry.getKey().namespace() == null) {
              int idx = m.indexOf(".");
              if (idx >= 0) {
                m = m.substring(idx + 1);
              }
            }
            UniqueKeyPair<String, Long> extant = metrics.get(m);
            if (extant == null) {
              extant = new UniqueKeyPair<String, Long>(m, 1L);
              metrics.put(m, extant);
            } else {
              extant.setValue(extant.getValue() + 1);
            }
            if (key == null) {
              key = getNamespace(entry.getKey(), meta_query.getId());
            }
            hits++;
          }
        }
        if (metrics != null) {
          results.put(key, new MockMetaResult(metrics.values(), hits));
        }
        break;
        
      case TAG_KEYS:
      case TAG_VALUES:
        Map<String, UniqueKeyPair<String, Long>> tag_keys_or_values = null;
        Set<String> matches = Sets.newHashSet();
        for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : 
            data_store.database().entrySet()) {
          if (FilterUtils.matchesTagKeysOrValues(meta_query.getFilter(), 
                                                 entry.getKey(), 
                                                 meta_query.getNamespace(), 
                                                 matches, 
                                                 query.getType() == QueryType.TAG_KEYS)) {
            for (final String match : matches) {
              if (tag_keys_or_values == null) {
                tag_keys_or_values = Maps.newHashMap();
              }
              UniqueKeyPair<String, Long> extant = tag_keys_or_values.get(matches);
              if (extant == null) {
                extant = new UniqueKeyPair<String, Long>(match, 1L);
                tag_keys_or_values.put(match, extant);
              } else {
                extant.setValue(extant.getValue() + 1);
              }
              if (key == null) {
                key = getNamespace(entry.getKey(), meta_query.getId());
              }
            }
            hits++;
          }
          matches.clear();
        }
        if (tag_keys_or_values != null) {
          results.put(key, new MockMetaResult(tag_keys_or_values.values(), hits, true));
        }
        break;
      case TAG_KEYS_AND_VALUES:
        Map<String, UniqueKeyPair<String, Long>> keys = Maps.newHashMap();
        Map<String, Map<String, UniqueKeyPair<String, Long>>> pairs = Maps.newHashMap();
        Map<String, String> tag_pairs = Maps.newHashMap(); 
        for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : 
              data_store.database().entrySet()) {
          if (FilterUtils.matchesTags(meta_query.getFilter(), entry.getKey(), 
              meta_query.getNamespace(), tag_pairs)) {
            for (final Entry<String, String> tags : tag_pairs.entrySet()) {
              // blech
              UniqueKeyPair<String, Long> extant = keys.get(tags.getKey());
              if (extant == null) {
                extant = new UniqueKeyPair<String, Long>(tags.getKey(), 1L);
                keys.put(tags.getKey(), extant);
              } else {
                extant.setValue(extant.getValue() + 1);
              }
              
              Map<String, UniqueKeyPair<String, Long>> values = pairs.get(tags.getKey());
              if (values == null) {
                values = Maps.newHashMap();
                pairs.put(tags.getKey(), values);
              }
              
              extant = values.get(tags.getValue());
              if (extant == null) {
                extant = new UniqueKeyPair<String, Long>(tags.getValue(), 1L);
                values.put(tags.getValue(), extant);
              } else {
                extant.setValue(extant.getValue() + 1);
              }
              
              if (key == null) {
                key = getNamespace(entry.getKey(), meta_query.getId());
              }
            }
          }
          hits++;
          tag_pairs.clear();
        }
        
        final Map<UniqueKeyPair<String, Long>, 
          Collection<UniqueKeyPair<String, Long>>> final_map = 
            Maps.newHashMapWithExpectedSize(keys.size());
        for (final Entry<String, UniqueKeyPair<String, Long>> entry : keys.entrySet()) {
          final_map.put(entry.getValue(), pairs.get(entry.getKey()).values());
        }
        if (!final_map.isEmpty()) {
          results.put(key, new MockMetaResult(final_map, hits));
        }
        break;
      case TIMESERIES:
        Set<TimeSeriesId> ids = null;
        for (final Entry<TimeSeriesDatumStringId, MockSpan> entry : 
              data_store.database().entrySet()) {
          if (FilterUtils.matches(meta_query.getFilter(), entry.getKey(), 
                meta_query.getNamespace())) {
            if (ids == null) {
              ids = Sets.newHashSet();
            }
            ids.add(BaseTimeSeriesStringId.newBuilder()
                .setMetric(entry.getKey().metric())
                .setTags(entry.getKey().tags())
                .build());
            if (key == null) {
              key = getNamespace(entry.getKey(), meta_query.getId());
            }
          }
        }
        if (ids != null) {
          results.put(key, new MockMetaResult(ids));
        }
        break;
      }
    }
  }
  
  /**
   * Pulls out a "namespace" from the metric, the string up to the first .
   * @param id The non-null ID.
   * @param query_id The ID of a query.
   * @return The key.
   */
  static NamespacedKey getNamespace(final TimeSeriesDatumStringId id, 
                                    final String query_id) {
    if (!Strings.isNullOrEmpty(id.namespace())) {
      return new NamespacedKey(id.namespace(), query_id);
    } else {
      int idx = id.metric().indexOf(".");
      if (idx < 0) {
        return new NamespacedKey(id.metric(), query_id);
      } else {
        return new NamespacedKey(id.metric().substring(0, idx), query_id);
      }
    }
  }
  
  /**
   * Simple result.
   */
  public static class MockMetaResult implements MetaDataStorageResult {
    final MetaResult result;
    final int hits;
    final Set<String> namespaces;
    final Collection<UniqueKeyPair<String, Long>> metrics;
    final Set<TimeSeriesId> ids;
    final Collection<UniqueKeyPair<String, Long>> tag_keys_or_values;
    final Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> pairs;
    
    MockMetaResult(final Set<String> namespaces, final int hits) {
      this.namespaces = namespaces;
      metrics = null;
      this.hits = hits;
      ids = null;
      tag_keys_or_values = null;
      pairs = null;
      result = hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
    }
    
    MockMetaResult(final Collection<UniqueKeyPair<String, Long>> metrics, final int hits) {
      namespaces = null;
      this.metrics = metrics;
      ids = null;
      this.hits = hits;
      tag_keys_or_values = null;
      pairs = null;
      result = hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
    }
    
    MockMetaResult(final Set<TimeSeriesId> ids) {
      namespaces = null;
      metrics = null;
      this.ids = ids;
      this.hits = ids.size();
      tag_keys_or_values = null;
      pairs = null;
      result = hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
    }
    
    MockMetaResult(final Collection<UniqueKeyPair<String, Long>> tag_keys_or_values, 
                   final int hits, 
                   final boolean ignored_to_satisfy_erasure) {
      namespaces = null;
      metrics = null;
      ids = null;
      this.tag_keys_or_values = tag_keys_or_values;
      pairs = null;
      this.hits = hits;
      result = hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
    }
    
    MockMetaResult(final Map<UniqueKeyPair<String, Long>, 
          Collection<UniqueKeyPair<String, Long>>> pairs, final int hits) {
      namespaces = null;
      metrics = null;
      ids = null;
      tag_keys_or_values = null;
      this.pairs = pairs;
      this.hits = hits;
      result = hits > 0 ? MetaResult.DATA : MetaResult.NO_DATA;
    }
    
    @Override
    public String id() {
      return "MockMetaResult";
    }

    @Override
    public long totalHits() {
      return hits;
    }

    @Override
    public MetaResult result() {
      return result;
    }

    @Override
    public Throwable exception() {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Collection<String> namespaces() {
      return namespaces == null ? Collections.emptyList() : namespaces;
    }

    @Override
    public Collection<TimeSeriesId> timeSeries() {
      return ids == null ? Collections.emptyList() : ids;
    }

    @Override
    public TypeToken<? extends TimeSeriesId> idType() {
      return Const.TS_STRING_ID;
    }

    @Override
    public Collection<UniqueKeyPair<String, Long>> metrics() {
      return metrics == null ? Collections.emptyList() : metrics;
    }

    @Override
    public Map<UniqueKeyPair<String, Long>, Collection<UniqueKeyPair<String, Long>>> tags() {
      return pairs == null ? Collections.emptyMap() : pairs;
    }

    @Override
    public Collection<UniqueKeyPair<String, Long>> tagKeysOrValues() {
      return tag_keys_or_values == null ? Collections.emptyList() : tag_keys_or_values;
    }
    
  }
  
}
