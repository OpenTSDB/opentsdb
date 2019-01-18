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

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.meta.MetaQuery.Order;
import net.opentsdb.meta.MetaQuery.QueryType;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.Pair;

/**
 * A meta query result that handles filtering, storing and sorting the results.
 * <b>WARNING:</b> The getters will sort the results on each call so please cache
 * it if you need to check for null or size.
 * 
 * @since 3.0
 */
public class NamespacedAggregatedDocumentResult implements MetaDataStorageResult {
  private long total_hits;
  private Set<String> namespaces;
  private Set<TimeSeriesId> ids;
  private Throwable throwable;
  private Set<Pair<String, Long>> metrics;
  private Map<Pair<String, Long>, List<Pair<String, Long>>> tags;
  private Set<Pair<String, Long>> tag_keys_or_values;
  private MetaResult result;
  private final MetaQuery query;

  /**
   * Package private ctor to construct a good query. Populates the namespaces
   * if the query type is not a namespace query.
   * @param result A non- null result.
   * @param query A non-null query.
   */
  NamespacedAggregatedDocumentResult(final MetaResult result, 
                                     final MetaQuery query) {
    this.result = result;
    this.query = query;
    if (query != null && query.type() != null && 
        query.type() != QueryType.NAMESPACES) {
      namespaces = Sets.newHashSet(query.namespace());
    }
  }

  NamespacedAggregatedDocumentResult(final MetaResult result, 
                                     final Throwable throwable,
                                     final MetaQuery query) {
    this.result = result;
    this.throwable = throwable;
    this.query = query;
  }

  @Override
  public long totalHits() {
    return total_hits;
  }
  
  @Override
  public MetaResult result() {
    return result;
  }

  @Override
  public Throwable exception() {
    return throwable;
  }

  @Override
  public Collection<String> namespaces() {
    if (namespaces == null) {
      return Collections.emptyList(); 
    }
    final List<String> sorted = Lists.newArrayList(namespaces);
    if (query.order() == Order.ASCENDING) {
      Collections.sort(sorted);
    } else {
      Collections.sort(sorted, Collections.reverseOrder());
    }
    return sorted;
  }
  
  @Override
  public Collection<TimeSeriesId> timeSeries() {
    // TODO - sorting?
    return ids == null ? Collections.emptyList() : ids;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_STRING_ID;
  }

  @Override
  public Collection<Pair<String, Long>> metrics() {
    if (metrics == null) {
      return Collections.emptyList();
    }
    final List<Pair<String, Long>> sorted = Lists.newArrayList(metrics);
    if (query.order() == Order.ASCENDING) {
      Collections.sort(sorted, PAIR_CMP);
    } else {
      Collections.sort(sorted, REVERSE_PAIR_CMP);
    }
    return sorted;
  }

  @Override
  public Map<Pair<String, Long>, List<Pair<String, Long>>> tags() {
    if (tags == null) {
      return Collections.emptyMap();
    }
    // sort
    for (final List<Pair<String, Long>> values : tags.values()) {
      if (values == null) {
        continue;
      }
      if (query.order() == Order.ASCENDING) {
        Collections.sort(values, PAIR_CMP);
      } else {
        Collections.sort(values, REVERSE_PAIR_CMP);
      }
    }
    return tags;
  }

  @Override
  public Collection<Pair<String, Long>> tagKeysOrValues() {
    if (tag_keys_or_values == null) {
      return Collections.emptyList();
    }
    final List<Pair<String, Long>> sorted = Lists.newArrayList(tag_keys_or_values);
    if (query.order() == Order.ASCENDING) {
      Collections.sort(sorted, PAIR_CMP);
    } else {
      Collections.sort(sorted, REVERSE_PAIR_CMP);
    }
    return sorted;
  }

  /**
   * Package private way to add a namespace.
   * @param namespace A non-null namespace.
   */
  void addNamespace(final String namespace) {
    if (namespaces == null) {
      namespaces = Sets.newHashSet();
    }
    namespaces.add(namespace);
  }
  
  /**
   * Adds the timeseries and filters on the metric.
   * @param id A non-null time series string id.
   */
  void addTimeSeries(final TimeSeriesId id) {
    if (query != null && 
        !matchMetric(((TimeSeriesStringId) id).metric(), false, query.filter())) {
      return;
    }
    
    if (ids == null) {
      ids = Sets.newHashSet();
    }
    ids.add(id);
  }
  
  /**
   * Adds the given metric, filtering to make sure it matches. 
   * @param metric A non-null metric.
   */
  void addMetric(final Pair<String, Long> metric) {
    if (metrics == null) {
      metrics = Sets.newHashSet();
    }
    metrics.add(metric);
  }
  
  /**
   * Adds the given tag key and list of values.
   * @param key A non-null key.
   * @param values A non-null (possibly empty) list.
   */
  void addTags(final Pair<String, Long> key, final List<Pair<String, Long>> values) {
    if (tags == null) {
      tags = Maps.newHashMap();
    }
    tags.put(key, values);
  }
  
  /**
   * Adds the given tag to the proper list.
   * @param key
   * @param value
   */
  void addTag(final  Pair<String, Long> key, final Pair<String, Long> value) {
    if (tags == null) {
      tags = Maps.newTreeMap(query.order() == Order.ASCENDING ? PAIR_CMP : REVERSE_PAIR_CMP);
    }
    List<Pair<String, Long>> values = tags.get(key);
    if (values == null) {
      values = Lists.newArrayList();
      tags.put(key, values);
    }
    values.add(value);
  }
  
  /**
   * Adds a tag value or key to the list.
   * @param value A non-null pair.
   */
  void addTagKeyOrValue(final Pair<String, Long> value) {
    if (tag_keys_or_values == null) {
      tag_keys_or_values = Sets.newHashSet();
    }
    tag_keys_or_values.add(value);
  }
  
  /**
   * Sets the total hit count.
   * @param total_hits The total hit count.
   */
  void setTotalHits(final long total_hits) {
    this.total_hits = total_hits;
  }
  
  /**
   * Runs through the filter set and evaluates the metric against the set. If
   * no metric filters are present then we just allow it. 
   * @param metric The non-null and non-empty metric. 
   * @param not Whether or not this recursive iteration is in "not" mode.
   * @param filter The optional filter.
   * @return True if the metric satisfied the filter (or there wasn't a metric
   * filter) or false if the metric did not match.
   */
  private boolean matchMetric(final String metric, 
                              final boolean not, 
                              final QueryFilter filter) {
    if (filter == null) {
      return true;
    }
    
    if (filter instanceof MetricFilter) {
      final boolean result = ((MetricFilter) filter).matches(metric);
      if (not) {
        return !result;
      }
      return result;
    }
    
    if (filter instanceof AnyFieldRegexFilter) {
      final boolean result = ((AnyFieldRegexFilter) filter).matches(metric);
      if (not) {
        return !result;
      }
      return result;
    }
    
    if (filter instanceof NotFilter) {
      return matchMetric(metric, !not, ((NotFilter) filter).getFilter());
    }
    
    if (filter instanceof ChainFilter) {
      boolean matched = true;
      for (final QueryFilter sub_filter : ((ChainFilter) filter).getFilters()) {
        if (!matchMetric(metric, not, sub_filter)) {
          matched = false;
          break;
        }
      }
      
      if (not) {
        return !matched;
      }
      return matched;
    }
    
    return true;
  }
  
  /**
   * A comparator for the pair keys in lexical order.
   */
  static class PairComparator implements Comparator<Pair<String, ?>> {

    @Override
    public int compare(final Pair<String, ?> a, final Pair<String, ?> b) {
      return a.getKey().compareTo(b.getKey());
    }
    
  }
  static final PairComparator PAIR_CMP = new PairComparator();
  
  /**
   * A comparator for the pair keys in reverse lexical order.
   */
  static class ReversePairComparator implements Comparator<Pair<String, ?>> {

    @Override
    public int compare(final Pair<String, ?> a, final Pair<String, ?> b) {
      return -a.getKey().compareTo(b.getKey());
    }
    
  }
  static final ReversePairComparator REVERSE_PAIR_CMP = new ReversePairComparator();
}
