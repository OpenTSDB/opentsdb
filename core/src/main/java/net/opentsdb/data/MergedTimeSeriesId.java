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
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An ID that can be used to merge multiple time series into one. Use by
 * calling {@link #newBuilder()}, setting an opitonal Alias and calling 
 * {@link Builder#addSeries(TimeSeriesStringId)} as many times as needed. It has the
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
    protected List<TimeSeriesId> ids;
    protected byte[] alias;
    protected byte[] namespace;
    protected byte[] metric;
    protected TypeToken<? extends TimeSeriesId> type;
    protected ReadableTimeSeriesDataStore data_store;
    
    /**
     * Sets the alias override to the given string or null.
     * @param alias A non-empty string or null to clear the override.
     * @return The builder.
     */
    public Builder setAlias(final String alias) {
      if (!Strings.isNullOrEmpty(alias)) {
        this.alias = alias.getBytes(Const.UTF8_CHARSET);
      } else {
        this.alias = null;
      }
      return this;
    }
    
    /**
     * Sets the alias override for byte time series IDs.
     * @param alias A non-empty byte array or null to clear the override.
     * @return The builder.
     */
    public Builder setAlias(final byte[] alias) {
      this.alias = alias;
      return this;
    }
    
    /**
     * Sets the namespace override to the given string or null.
     * @param namespace A non-empty string or null to clear the override.
     * @return The builder.
     */
    public Builder setNamespace(final String namespace) {
      if (!Strings.isNullOrEmpty(namespace)) {
        this.namespace = namespace.getBytes(Const.UTF8_CHARSET);
      } else {
        this.namespace = null;
      }
      return this;
    }
    
    /**
     * Sets the namespace override for byte time series IDs
     * @param namespace A non-empty byte array or null to clear the override.
     * @return The builder.
     */
    public Builder setNamespace(final byte[] namespace) {
      this.namespace = namespace;
      return this;
    }
    
    /**
     * Sets the metric override to the given string or null.
     * @param metric A non-empty string or null to clear the override.
     * @return The builder.
     */
    public Builder setMetric(final String metric) {
      if (!Strings.isNullOrEmpty(metric)) {
        this.metric = metric.getBytes(Const.UTF8_CHARSET);
      } else {
        this.metric = null;
      }
      return this;
    }
    
    /**
     * Sets the metric override for byte time series IDs.
     * @param metric A non-empty string or null to clear the override.
     * @return The builder.
     */
    public Builder setMetric(final byte[] metric) {
      this.metric = metric;
      return this;
    }
    
    /**
     * Adds the given time series to the merge list. The first ID added
     * will set the ID type for the set. If an ID of a different time is
     * passed as an argument, an exception will be thrown. All ID types
     * must be the same.
     * @param id A non-null ID
     * @return The builder.
     * @throws IllegalArgumentException if the ID was null.
     * @throws RuntimeException if an ID of a different type than existing
     * IDs was provided.
     */
    public Builder addSeries(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }
      if (ids == null) {
        ids = Lists.newArrayListWithExpectedSize(2);
      }
      if (type == null) {
        type = id.type();
        if (id instanceof TimeSeriesByteId) {
          data_store = ((TimeSeriesByteId) id).dataStore();
        }
      } else {
        if (!type.equals(id.type())) {
          // TODO - proper exception type
          throw new RuntimeException("Attempted to add an ID of type " 
              + id.type() + " that was not the same as the first "
              + "ID's type: " + type);
        }
        if (id instanceof TimeSeriesByteId) {
          if (!data_store.equals(((TimeSeriesByteId) id).dataStore())) {
            // TODO - proper exception type
            throw new RuntimeException("Attempted to add an ID with a " 
                + "different schema " + ((TimeSeriesByteId) id).dataStore() 
                + " that was not the same as the first "
                + "ID's schema: " + data_store);
          }
        }
      }
      ids.add(id);
      return this;
    }

    /** @return The merged time series ID. */
    public TimeSeriesId build() {
      if (type.equals(Const.TS_STRING_ID)) {
        return mergeStringIds();
      } else if (type.equals(Const.TS_BYTE_ID)) {
        return mergeByteIds();
      }
      // TODO - proper exception
      throw new RuntimeException("Unhandled ID type: " + type);
    }
    
    /**
     * Merges the time series into a new ID, promoting tags to aggregated or 
     * disjoint tags and combining namespaces, metrics and unique IDs.
     * @return A non-null time series ID.
     */
    private TimeSeriesId mergeStringIds() {
      if (ids.size() == 1) {
        return ids.get(0);
      }
      
      String first_namespace = null;
      String first_metric = null;
      Map<String, String> tags = null;
      Set<String> aggregated_tags = null;
      Set<String> disjoint_tags = null;
      Set<String> unique_ids = null;
      
      int i = 0;
      for (final TimeSeriesId raw_id : ids) {
        final TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
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
      
      final BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId.newBuilder();
      if (alias != null && alias.length > 0) {
        builder.setAlias(new String(alias, Const.UTF8_CHARSET));
      }
      if (namespace == null || namespace.length < 1) {
        builder.setNamespace(first_namespace);
      } else {
        builder.setNamespace(new String(namespace, Const.UTF8_CHARSET));
      }
      if (metric == null || metric.length < 1) {
        builder.setMetric(first_metric);
      } else {
        builder.setMetric(new String(metric, Const.UTF8_CHARSET));
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

    /**
     * Merges the time series into a new ID, promoting tags to aggregated or 
     * disjoint tags and combining namespaces, metrics and unique IDs.
     * @return A non-null time series ID.
     */
    private TimeSeriesId mergeByteIds() {
      if (ids.size() == 1) {
        return ids.get(0);
      }
      
      byte[] first_namespace = null;
      byte[] first_metric = null;
      ByteMap<byte[]> tags = null;
      ByteSet aggregated_tags = null;
      ByteSet disjoint_tags = null;
      ByteSet unique_ids = null;
      
      int i = 0;
      for (final TimeSeriesId raw_id : ids) {
        final TimeSeriesByteId id = (TimeSeriesByteId) raw_id;
        if (Bytes.isNullOrEmpty(first_namespace)) {
          first_namespace = id.namespace();
        }
        
        if (Bytes.isNullOrEmpty(first_metric)) {
          first_metric = id.metric();
        }
        
        // agg and disjoint BEFORE tags
        if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
          for (final byte[] tag : id.aggregatedTags()) {
            if (!Bytes.isNullOrEmpty(tag) && tags != null && tags.containsKey(tag)) {
              tags.remove(tag);
              if (aggregated_tags == null) {
                aggregated_tags = new ByteSet();
              }
              aggregated_tags.add(tag);
            } else {
              if (disjoint_tags != null && disjoint_tags.contains(tag)) {
                // no-op
              } else {
                if (aggregated_tags == null) {
                  aggregated_tags = new ByteSet();
                }
                if (i < 1) {
                  aggregated_tags.add(tag);
                } else {
                  if (aggregated_tags != null && !aggregated_tags.contains(tag)) {
                    if (disjoint_tags == null) {
                      disjoint_tags = new ByteSet();
                    }
                    disjoint_tags.add(tag);
                  }
                }
              }
            }
          }
        }
        
        if (id.disjointTags() != null && !id.disjointTags().isEmpty()) {
          for (final byte[] tag : id.disjointTags()) {
            if (tags != null && tags.containsKey(tag)) {
              tags.remove(tag);
            }
            if (aggregated_tags != null && aggregated_tags.contains(tag)) {
              aggregated_tags.remove(tag);
            }
            if (disjoint_tags == null) {
              disjoint_tags = new ByteSet();
            }
            disjoint_tags.add(tag);
          }
        }
        
        if (id.tags() != null && !id.tags().isEmpty()) {
          if (tags == null) {
            tags = new ByteMap<byte[]>();
          }
          
          for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
            if (disjoint_tags != null && disjoint_tags.contains(pair.getKey())) {
              continue;
            }
            if (aggregated_tags != null && aggregated_tags.contains(pair.getKey())) {
              continue;
            }
            
            // check tag and values!
            final byte[] existing_value = tags.get(pair.getKey());
            if (Bytes.isNullOrEmpty(existing_value)) {
              if (i > 0) {
                // disjoint!
                if (disjoint_tags == null) {
                  disjoint_tags = new ByteSet();
                }
                disjoint_tags.add(pair.getKey());
              } else {
                tags.put(pair.getKey(), pair.getValue());
              }
            } else if (Bytes.memcmp(existing_value, pair.getValue()) != 0) {
              // move to agg
              if (aggregated_tags == null) {
                aggregated_tags = new ByteSet();
              }
              aggregated_tags.add(pair.getKey());
              tags.remove(pair.getKey());
            }
          }
          
          // reverse
          if (tags != null) {
            final Iterator<Entry<byte[], byte[]>> it = tags.entrySet().iterator();
            while (it.hasNext()) {
              final Entry<byte[], byte[]> pair = it.next();
              if (!id.tags().containsKey(pair.getKey())) {
                // disjoint!
                if (disjoint_tags == null) {
                  disjoint_tags = new ByteSet();
                }
                disjoint_tags.add(pair.getKey());
                it.remove();
              }
            }
          }
        } // end id tags check
        
        // reverse agg
        if (aggregated_tags != null && i > 0) {
          final Iterator<byte[]> it = aggregated_tags.iterator();
          while(it.hasNext()) {
            final byte[] tag = it.next();
            if (id.tags() != null && id.tags().containsKey(tag)) {
              // good, keep it
              continue;
            }
            if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
              boolean matched = false;
              for (final byte[] other : id.aggregatedTags()) {
                if (Bytes.memcmp(tag, other) == 0) {
                  matched = true;
                  break;
                }
              }
              if (matched) {
                continue;
              }
            }
            
            if (disjoint_tags == null) {
              disjoint_tags = new ByteSet();
            }
            disjoint_tags.add(tag);
            it.remove();
          }
        } // end reverse agg check
        
        if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
          if (unique_ids == null) {
            unique_ids = new ByteSet();
          }
          unique_ids.addAll(id.uniqueIds());
        }
        i++;
      }
      
      final BaseTimeSeriesByteId.Builder builder = 
          BaseTimeSeriesByteId.newBuilder(data_store);
      if (Bytes.isNullOrEmpty(alias)) {
        builder.setAlias(alias);
      }
      if (Bytes.isNullOrEmpty(namespace)) {
        builder.setNamespace(first_namespace);
      } else {
        builder.setNamespace(namespace);
      }
      if (metric == null || metric.length < 1) {
        builder.setMetric(first_metric);
      } else {
        builder.setMetric(metric);
      }
      if (tags != null && !tags.isEmpty()) {
        builder.setTags(tags);
      }
      if (aggregated_tags != null && !aggregated_tags.isEmpty()) {
        for (final byte[] tag : aggregated_tags) {
          builder.addAggregatedTag(tag);
        }
      }
      if (disjoint_tags != null && !disjoint_tags.isEmpty()) {
        for (final byte[] tag : disjoint_tags) {
          builder.addDisjointTag(tag);
        }
      }
      return builder.build();
    }
  }
  
}
