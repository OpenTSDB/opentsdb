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
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;
import net.opentsdb.common.Const;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.meta.BatchMetaQuery.Order;
import net.opentsdb.meta.BatchMetaQuery.QueryType;
import net.opentsdb.query.filter.AnyFieldRegexFilter;
import net.opentsdb.query.filter.ChainFilter;
import net.opentsdb.query.filter.MetricFilter;
import net.opentsdb.query.filter.NotFilter;
import net.opentsdb.query.filter.QueryFilter;
import net.opentsdb.utils.UniqueKeyPair;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
  private Map<Integer, UniqueKeyPair<String, Long>> metrics;
  private Map<UniqueKeyPair<String, Long>, List<UniqueKeyPair<String, Long>>> tags;
  private Map<Integer, UniqueKeyPair<String, Long>> tag_keys_or_values;
  private MetaResult result;
  private final BatchMetaQuery query;
  private final MetaQuery meta_query;

  /**
   * Package private ctor to construct a good query. Populates the namespaces
   * if the query type is not a namespace query.
   * @param result A non- null result.
   * @param query A non-null query.
   */
  NamespacedAggregatedDocumentResult(final MetaResult result,
                                     final BatchMetaQuery query,
                                     final MetaQuery meta_query) {
    this.result = result;
    this.query = query;
    if (query != null && query.type() != null &&
        query.type() != QueryType.NAMESPACES) {
      namespaces = Sets.newHashSet(meta_query.namespace());
    }
    this.meta_query = meta_query;
  }

  NamespacedAggregatedDocumentResult(final MetaResult result,
                                     final Throwable throwable,
                                     final BatchMetaQuery query) {
    this.result = result;
    this.throwable = throwable;
    this.query = query;
    this.meta_query = null;
  }

  @Override
  public String id() {
    return meta_query != null ? meta_query.id() : null;
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
  public Collection<UniqueKeyPair<String, Long>> metrics() {
    if (metrics == null) {
      return Collections.emptyList();
    }
    final List<UniqueKeyPair<String, Long>> sorted = Lists.newArrayList(metrics.values());
    if (query.order() == Order.ASCENDING) {
      Collections.sort(sorted, UniqueKeyPair_CMP);
    } else {
      Collections.sort(sorted, REVERSE_UniqueKeyPair_CMP);
    }
    return sorted;
  }

  @Override
  public Map<UniqueKeyPair<String, Long>, List<UniqueKeyPair<String, Long>>> tags() {
    if (tags == null) {
      return Collections.emptyMap();
    }
    // sort
    for (final List<UniqueKeyPair<String, Long>> values : tags.values()) {
      if (values == null) {
        continue;
      }
      if (query.order() == Order.ASCENDING) {
        Collections.sort(values, UniqueKeyPair_CMP);
      } else {
        Collections.sort(values, REVERSE_UniqueKeyPair_CMP);
      }
    }
    return tags;
  }

  @Override
  public Collection<UniqueKeyPair<String, Long>> tagKeysOrValues() {
    if (tag_keys_or_values == null) {
      return Collections.emptyList();
    }
    final List<UniqueKeyPair<String, Long>> sorted = Lists.newArrayList
            (tag_keys_or_values.values());
    if (query.order() == Order.ASCENDING) {
      Collections.sort(sorted, UniqueKeyPair_CMP);
    } else {
      Collections.sort(sorted, REVERSE_UniqueKeyPair_CMP);
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
  void addTimeSeries(final TimeSeriesId id, final MetaQuery meta_query, final
   String metric_only) {
    if (meta_query != null &&
        !matchMetric(metric_only, false, meta_query.filter())) {
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
  void addMetric(final UniqueKeyPair<String, Long> metric) {
    if (metrics == null) {
      metrics = Maps.newHashMap();
    }
    if (metrics.containsKey(metric.hashCode())) {
      if ((long)metrics.get(metric.hashCode()).getValue() < metric.getValue()) {
        metrics.put(metric.hashCode(), metric);
      }
    } else {
      metrics.put(metric.hashCode(), metric);
    }
  }

  /**
   * Adds the given tag key and list of values.
   * @param key A non-null key.
   * @param values A non-null (possibly empty) list.
   */
  void addTags(final UniqueKeyPair<String, Long> key, final List<UniqueKeyPair<String, Long>>
          values) {
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
  void addTag(final  UniqueKeyPair<String, Long> key, final UniqueKeyPair<String, Long> value) {
    if (tags == null) {
      tags = Maps.newTreeMap(query.order() == Order.ASCENDING ? UniqueKeyPair_CMP : REVERSE_UniqueKeyPair_CMP);
    }
    List<UniqueKeyPair<String, Long>> values = tags.get(key);
    if (values == null) {
      values = Lists.newArrayList();
      tags.put(key, values);
    }
    values.add(value);
  }

  /**
   * Adds a tag value or key to the list.
   * @param value A non-null UniqueKeyPair.
   */
  void addTagKeyOrValue(final UniqueKeyPair<String, Long> value) {
    if (tag_keys_or_values == null) {
      tag_keys_or_values = Maps.newHashMap();
    }

    if (tag_keys_or_values.containsKey(value.hashCode())) {
      if ((long)tag_keys_or_values.get(value.hashCode()).getValue() < value.getValue()) {
        tag_keys_or_values.put(value.hashCode(), value);
      }
    } else {
      tag_keys_or_values.put(value.hashCode(), value);
    }
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
   * A comparator for the UniqueKeyPair keys in lexical order.
   */
  static class UniqueKeyPairComparator implements Comparator<UniqueKeyPair<String, ?>> {

    @Override
    public int compare(final UniqueKeyPair<String, ?> a, final UniqueKeyPair<String, ?> b) {
      return a.getKey().compareTo(b.getKey());
    }

  }
  static final UniqueKeyPairComparator UniqueKeyPair_CMP = new UniqueKeyPairComparator();

  /**
   * A comparator for the UniqueKeyPair keys in reverse lexical order.
   */
  static class ReverseUniqueKeyPairComparator implements Comparator<UniqueKeyPair<String, ?>> {

    @Override
    public int compare(final UniqueKeyPair<String, ?> a, final UniqueKeyPair<String, ?> b) {
      return -a.getKey().compareTo(b.getKey());
    }

  }
  static final ReverseUniqueKeyPairComparator REVERSE_UniqueKeyPair_CMP = new ReverseUniqueKeyPairComparator();
}
