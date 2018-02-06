// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.data;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * An ID that can be used to merge multiple time series into one. Use by
 * calling {@link #newBuilder()}, setting an opitonal Alias and calling 
 * {@link Builder#addSeries(TimeSeriesId)} as many times as needed. It has the
 * following logic:
 * <p>
 * <ul>
 * <li>Alias' for the individual IDs are ignored. Use 
 * {@link Builder#setAlias(byte[])}.</li>
 * <li>Namespaces are merged into a unique set of values.</li>
 * <li>Metrics are merged into a unique set of values.</li>
 * <li>Tags, Aggregated Tags and Disjoint Tags are merged in the following way:
 *   <ul>
 *   <li>Tag pairs with the same key and value across all IDs are merged in the
 *   {@link #tags()} map.</li>
 *   <li>Tag keys that appear across all IDs (or in the {@link #aggregatedTags()}
 *   list) are placed in the {@link #aggregatedTags()} list.</li>
 *   <li>Tag keys that do NOT appear consistently across all IDs in the 
 *   {@link #tags()} or {@link #aggregatedTags()} sets are moved to the 
 *   {@link #disjointTags()} list.</li> 
 *   </ul></li>
 * </ul>
 * 
 * @since 3.0
 */
public class MergedTimeSeriesId {
  
  /** @return A builder for the merged ID */
  public static Builder newBuilder() {
    return new Builder();
  }

  public static class Builder {
    private List<TimeSeriesId> ids;
    private String alias;
    private String namespace;
    private String metric;
    
    public Builder setAlias(final String alias) {
      this.alias = alias;
      return this;
    }
    
    public Builder setNamespace(final String namespace) {
      this.namespace = namespace;
      return this;
    }
    
    public Builder setMetric(final String metric) {
      this.metric = metric;
      return this;
    }
    
    public Builder addSeries(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      if (ids == null) {
        ids = Lists.newArrayListWithExpectedSize(2);
      }
      ids.add(id);
      return this;
    }

    public TimeSeriesId build() {
      return merge();
    }
    
    /**
     * Merges the time series into a new ID, promoting tags to aggregated or 
     * disjoint tags and combining namespaces, metrics and unique IDs.
     * @return A non-null time series ID.
     */
    private TimeSeriesId merge() {
      // TODO shortcircuit if there is only a single ID
      
      String first_namespace = null;
      String first_metric = null;
      Map<String, String> tags = null;
      Set<String> aggregated_tags = null;
      Set<String> disjoint_tags = null;
      Set<String> unique_ids = null;
      
      int i = 0;
      for (final TimeSeriesId id : ids) {
        if (Strings.isNullOrEmpty(first_namespace)) {
          first_namespace = id.namespace();
        }
        
        if (Strings.isNullOrEmpty(first_metric)) {
          first_metric = id.metric();
        }
        
        // agg and disjoint BEFORE tags
        if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
          for (final String tag : id.aggregatedTags()) {
            if (!Strings.isNullOrEmpty(tag) && tags != null && tags.containsKey(tag)) {
              tags.remove(tag);
              if (aggregated_tags == null) {
                aggregated_tags = Sets.newHashSetWithExpectedSize(1);
              }
              aggregated_tags.add(tag);
            } else {
              if (disjoint_tags != null && disjoint_tags.contains(tag)) {
                // no-op
              } else {
                if (aggregated_tags == null) {
                  aggregated_tags = Sets.newHashSetWithExpectedSize(1);
                }
                if (i < 1) {
                  aggregated_tags.add(tag);
                } else {
                  if (aggregated_tags != null && !aggregated_tags.contains(tag)) {
                    if (disjoint_tags == null) {
                      disjoint_tags = Sets.newHashSetWithExpectedSize(1);
                    }
                    disjoint_tags.add(tag);
                  }
                }
              }
            }
          }
        }
        
        if (id.disjointTags() != null && !id.disjointTags().isEmpty()) {
          for (final String tag : id.disjointTags()) {
            if (tags != null && tags.containsKey(tag)) {
              tags.remove(tag);
            }
            if (aggregated_tags != null && aggregated_tags.contains(tag)) {
              aggregated_tags.remove(tag);
            }
            if (disjoint_tags == null) {
              disjoint_tags = Sets.newHashSetWithExpectedSize(1);
            }
            disjoint_tags.add(tag);
          }
        }
        
        if (id.tags() != null && !id.tags().isEmpty()) {
          if (tags == null) {
            tags = Maps.newHashMapWithExpectedSize(id.tags().size());
          }
          
          for (final Entry<String, String> pair : id.tags().entrySet()) {
            if (disjoint_tags != null && disjoint_tags.contains(pair.getKey())) {
              continue;
            }
            if (aggregated_tags != null && aggregated_tags.contains(pair.getKey())) {
              continue;
            }
            
            // check tag and values!
            final String existing_value = tags.get(pair.getKey());
            if (existing_value == null) {
              if (i > 0) {
                // disjoint!
                if (disjoint_tags == null) {
                  disjoint_tags = Sets.newHashSetWithExpectedSize(1);
                }
                disjoint_tags.add(pair.getKey());
              } else {
                tags.put(pair.getKey(), pair.getValue());
              }
            } else if (!existing_value.equals(pair.getValue())) {
              // move to agg
              if (aggregated_tags == null) {
                aggregated_tags = Sets.newHashSetWithExpectedSize(1);
              }
              aggregated_tags.add(pair.getKey());
              tags.remove(pair.getKey());
            }
          }
          
          // reverse
          if (tags != null) {
            final Iterator<Entry<String, String>> it = tags.entrySet().iterator();
            while (it.hasNext()) {
              final Entry<String, String> pair = it.next();
              if (!id.tags().containsKey(pair.getKey())) {
                // disjoint!
                if (disjoint_tags == null) {
                  disjoint_tags = Sets.newHashSetWithExpectedSize(1);
                }
                disjoint_tags.add(pair.getKey());
                it.remove();
              }
            }
          }
        } // end id tags check
        
        // reverse agg
        if (aggregated_tags != null && i > 0) {
          final Iterator<String> it = aggregated_tags.iterator();
          while(it.hasNext()) {
            final String tag = it.next();
            if (id.tags() != null && id.tags().containsKey(tag)) {
              // good, keep it
              continue;
            }
            if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
              boolean matched = false;
              for (final String other : id.aggregatedTags()) {
                if (tag.equals(other)) {
                  matched = true;
                  break;
                }
              }
              if (matched) {
                continue;
              }
            }
            
            if (disjoint_tags == null) {
              disjoint_tags = Sets.newHashSetWithExpectedSize(1);
            }
            disjoint_tags.add(tag);
            it.remove();
          }
        } // end reverse agg check
        
        if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
          if (unique_ids == null) {
            unique_ids = Sets.newHashSetWithExpectedSize(1);
          }
          unique_ids.addAll(id.uniqueIds());
        }
        i++;
      }
      
      final BaseTimeSeriesId.Builder builder = BaseTimeSeriesId.newBuilder();
      builder.setAlias(alias);
      if (Strings.isNullOrEmpty(namespace)) {
        builder.setNamespace(first_namespace);
      } else {
        builder.setNamespace(namespace);
      }
      if (Strings.isNullOrEmpty(metric)) {
        builder.setMetric(first_metric);
      } else {
        builder.setMetric(metric);
      }
      if (tags != null && !tags.isEmpty()) {
        builder.setTags(tags);
      }
      if (aggregated_tags != null && !aggregated_tags.isEmpty()) {
        for (final String tag : aggregated_tags) {
          builder.addAggregatedTag(tag);
        }
      }
      if (disjoint_tags != null && !disjoint_tags.isEmpty()) {
        for (final String tag : disjoint_tags) {
          builder.addDisjointTag(tag);
        }
      }
      return builder.build();
    }
  }
  
}
