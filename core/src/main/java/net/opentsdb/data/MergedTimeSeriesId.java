// This file is part of OpenTSDB.
// Copyright (C) 2017-2021  The OpenTSDB Authors.
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

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import net.opentsdb.common.Const;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * An ID that can be used to merge multiple time series into one. Use by
 * calling {@link #newBuilder()} and calling
 * {@link Builder#addSeries(TimeSeriesId)} as many times as needed. The metric,
 * alias and namespace are carried over from the FIRST ID added. Future IDs
 * are not checked for similar fields to avoid unnecessary checks. It has the
 * following logic:
 * <p>
 * <ul>
 * <li>Tags, Aggregated Tags and Disjoint Tags are merged in the following way:
 *   <ul>
 *   <li>Tag pairs with the same key and value across all IDs are merged in the
 *   {@link TimeSeriesStringId#tags()} map.</li>
 *   <li>Tag keys that appear across all IDs (or in the
 *   {@link TimeSeriesStringId#aggregatedTags()} list) are placed in the
 *   {@link TimeSeriesStringId#aggregatedTags()} list.</li>
 *   <li>Tag keys that do NOT appear consistently across all IDs in the 
 *   {@link TimeSeriesStringId#tags()} or
 *   {@link TimeSeriesStringId#aggregatedTags()} sets are moved to the
 *   {@link TimeSeriesStringId#disjointTags()} list.</li>
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
    protected boolean full_merge;
    protected MergingTimeSeriesId id;

    /**
     * Adds the given ID to the merged set, demoting tags to the aggregate or
     * disjoint fields if appropriate.
     *
     * @param id A non-null byte or string time series ID
     * @return The builder.
     * @throws IllegalArgumentException if the ID was null or it wasn't a known
     * type of ID (e.g. byte or string).
     */
    public Builder addSeries(final TimeSeriesId id) {
      if (id == null) {
        throw new IllegalArgumentException("ID cannot be null.");
      }

      if (this.id == null) {
        if (id.type() == Const.TS_BYTE_ID) {
          this.id = new MergingByteId(((TimeSeriesByteId) id).dataStore(), full_merge);
        } else if (id.type() == Const.TS_STRING_ID) {
          this.id = new MergingStringId(full_merge);
        } else {
          throw new IllegalArgumentException("Unknown ID data type: " + id.type());
        }
      }

      this.id.addSeries(id);
      return this;
    }

    public Builder setFullMerge(final boolean full_merge) {
      this.full_merge = full_merge;
      return this;
    }
    
    /** @return The merged time series ID. */
    public TimeSeriesId build() {
      return id;
    }

  }

  protected interface MergingTimeSeriesId extends TimeSeriesId {

    /**
     * Adds a non-null ID, merging and promoting tags per the rules.
     *
     * @param id A non-null ID to merge.
     */
    void addSeries(final TimeSeriesId id);

  }

  /**
   * The ByteID version of a mergeing ID.
   */
  protected static class MergingByteId extends BaseTimeSeriesByteId
          implements MergingTimeSeriesId {

    protected boolean fullMerge;
    protected ByteSet aggTags;
    protected ByteSet disjointTags;

    public MergingByteId(final TimeSeriesDataSourceFactory data_store,
                         final boolean fullMerge) {
      super(data_store);
      this.fullMerge = fullMerge;
    }

    @Override
    public List<byte[]> aggregatedTags() {
      if (aggregated_tags == null) {
        aggregated_tags = aggTags == null ? Collections.emptyList() :
                Lists.newArrayList(aggTags);
      }
      return aggregated_tags;
    }

    @Override
    public List<byte[]> disjointTags() {
      if (disjoint_tags == null) {
        disjoint_tags = disjointTags == null ? Collections.emptyList() :
                Lists.newArrayList(disjointTags);
      }
      return disjoint_tags;
    }

    @Override
    public long buildHashCode() {
      if (aggregated_tags == null) {
        aggregated_tags = aggTags == null ? Collections.emptyList() :
                Lists.newArrayList(aggTags);
      }

      if (disjoint_tags == null) {
        disjoint_tags = disjointTags == null ? Collections.emptyList() :
                Lists.newArrayList(disjointTags);
      }

      return super.buildHashCode();
    }

    @Override
    public void addSeries(final TimeSeriesId raw_id) {
      // TODO - validations
      if (metric == null) {
        // first ID
        copyInitial((TimeSeriesByteId) raw_id);
        return;
      }

      // merge!
      // TODO - validations
      TimeSeriesByteId id = (TimeSeriesByteId) raw_id;
      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        for (int i = 0; i < id.aggregatedTags().size(); i++) {
          final byte[] tag = id.aggregatedTags().get(i);

          // migrate from the tags set if present.
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
            if (aggTags == null) {
              aggTags = new ByteSet();
            }
            aggTags.add(tag);
          } else if (fullMerge && disjointTags != null && disjointTags.contains(tag)) {
            // No-op
          } else {
            if (aggTags == null) {
              aggTags = new ByteSet();
              aggTags.add(tag);
            } else if (fullMerge && !aggTags.contains(tag)) {
              if (disjointTags == null) {
                disjointTags = new ByteSet();
              }

              if (!disjointTags.contains(tag)) {
                disjointTags.add(tag);
              }
            }
          }
        }
      }

      // check disjoints.
      if (fullMerge && id.disjointTags() != null && !id.disjointTags().isEmpty()) {
        for (int i = 0; i < id.disjointTags().size(); i++) {
          final byte[] tag = id.disjointTags().get(i);

          // migrate from the tags set.
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
          }

          if (aggTags != null && aggTags.contains(tag)) {
            aggTags.remove(tag);
          }

          if (disjointTags == null) {
            disjointTags = new ByteSet();
          }
          if (!disjointTags.contains(tag)) {
            disjointTags.add(tag);
          }
        }
      }

      // now check tags
      if (id.tags() != null && !id.tags().isEmpty()) {
        if (tags == null) {
          tags = new ByteMap<byte[]>();
        }

        for (Map.Entry<byte[], byte[]> pair : id.tags().entrySet()) {
          final byte[] key = pair.getKey();
          if (fullMerge && disjointTags != null && disjointTags.contains(key)) {
            continue;
          }
          if (aggTags != null && aggTags.contains(key)) {
            continue;
          }

          final byte[] extant = tags.get(key);
          if (Bytes.isNullOrEmpty(extant)) {
            if (fullMerge) {
              // disjoint!
              if (disjointTags == null) {
                disjointTags = new ByteSet();
              }
              disjointTags.add(key);
            }
          } else if (Bytes.memcmp(extant, pair.getValue()) != 0) {
            // different value so we need to move to aggs
            if (aggTags == null) {
              aggTags = new ByteSet();
            }
            aggTags.add(key);
            tags.remove(key);
          }
        }
      }

      // reverse verify the tags
      if (tags != null) {
        final Iterator<Entry<byte[], byte[]>> iterator = tags.entrySet().iterator();
        while (iterator.hasNext()) {
          final Entry<byte[], byte[]> pair = iterator.next();
          final byte[] key = pair.getKey();
          if (!id.tags().containsKey(key)) {
            // disjoint!
            if (fullMerge) {
              if (disjointTags == null) {
                disjointTags = new ByteSet();
              }
              disjointTags.add(key);
            }
            iterator.remove();
          }
        }
      }

      // reverse agg
      if (aggTags != null) {
        final Iterator<byte[]> iterator = aggTags.iterator();
        OuterLoop:
        while (iterator.hasNext()) {
          final byte[] tag = iterator.next();
          if (id.tags() != null && id.tags().containsKey(tag)) {
            // good keep it.
            continue;
          }

          if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
            for (final byte[] other : id.aggregatedTags()) {
              if (Bytes.memcmp(tag, other) == 0) {
                continue OuterLoop;
              }
            }
          }

          // disjoint!
          if (disjointTags == null) {
            disjointTags = new ByteSet();
          }
          disjointTags.add(tag);
          iterator.remove();
        }
      }

      if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
        if (unique_ids == null) {
          unique_ids = new ByteSet();
        }
        unique_ids.addAll(id.uniqueIds());
      }
    }

    void copyInitial(final TimeSeriesByteId id) {
      if (id.alias() != null) {
        alias = id.alias();
      }
      if (id.namespace() != null) {
        namespace = id.namespace();
      }
      if (id.metric() != null) {
        metric = id.metric();
      }

      if (id.tags() != null && !id.tags().isEmpty()) {
        tags = new ByteMap<byte[]>();
        for (Map.Entry<byte[], byte[]> entry : id.tags().entrySet()) {
          tags.put(entry.getKey(), entry.getValue());
        }
      }

      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        aggTags = new ByteSet();
        for (int i = 0; i < id.aggregatedTags().size(); i++) {
          aggTags.add(id.aggregatedTags().get(i));
        }
      }

      if (id.disjointTags() != null && !id.disjointTags().isEmpty()) {
        disjointTags = new ByteSet();
        for (int i = 0; i < id.disjointTags().size(); i++) {
          disjointTags.add( id.disjointTags().get(i));
        }
      }

      if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
        if (unique_ids == null) {
          unique_ids = new ByteSet();
        }
        unique_ids.addAll(id.uniqueIds());
      }
    }
  }

  /**
   * String version of the merging ID.
   */
  protected static class MergingStringId extends BaseTimeSeriesStringId
          implements MergingTimeSeriesId {

    protected boolean fullMerge;
    protected Set<String> aggTags;
    protected Set<String> disjointTags;

    public MergingStringId(final boolean fullMerge) {
      super();
      this.fullMerge = fullMerge;
    }

    @Override
    public List<String> aggregatedTags() {
      if (aggregated_tags == null) {
        aggregated_tags = aggTags == null ? Collections.emptyList() :
                Lists.newArrayList(aggTags);
      }
      return aggregated_tags;
    }

    @Override
    public List<String> disjointTags() {
      if (disjoint_tags == null) {
        disjoint_tags = disjointTags == null ? Collections.emptyList() :
                Lists.newArrayList(disjointTags);
      }
      return disjoint_tags;
    }

    @Override
    public long buildHashCode() {
      if (aggregated_tags == null) {
        aggregated_tags = aggTags == null ? Collections.emptyList() :
                Lists.newArrayList(aggTags);
      }

      if (disjoint_tags == null) {
        disjoint_tags = disjointTags == null ? Collections.emptyList() :
                Lists.newArrayList(disjointTags);
      }

      return super.buildHashCode();
    }

    @Override
    public void addSeries(final TimeSeriesId raw_id) {
      // TODO - validations
      if (metric == null) {
        // first ID
        copyInitial((TimeSeriesStringId) raw_id);
        return;
      }

      // merge!
      // TODO - validations
      TimeSeriesStringId id = (TimeSeriesStringId) raw_id;
      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        for (int i = 0; i < id.aggregatedTags().size(); i++) {
          final String tag = id.aggregatedTags().get(i);

          // migrate from the tags set if present.
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
            if (aggTags == null) {
              aggTags = Sets.newHashSet();
            }
            aggTags.add(tag);
          } else if (fullMerge && disjointTags != null && disjointTags.contains(tag)) {
            // No-op
          } else {
            if (aggTags == null) {
              aggTags = Sets.newHashSet();
              aggTags.add(tag);
            } else if (fullMerge && !aggTags.contains(tag)) {
              if (disjointTags == null) {
                disjointTags = Sets.newHashSet();
              }

              if (!disjointTags.contains(tag)) {
                disjointTags.add(tag);
              }
            }
          }
        }
      }

      // check disjoints.
      if (fullMerge && id.disjointTags() != null && !id.disjointTags().isEmpty()) {
        for (int i = 0; i < id.disjointTags().size(); i++) {
          final String tag = id.disjointTags().get(i);

          // migrate from the tags set.
          if (tags != null && tags.containsKey(tag)) {
            tags.remove(tag);
          }

          if (aggTags != null && aggTags.contains(tag)) {
            aggTags.remove(tag);
          }

          if (disjointTags == null) {
            disjointTags = Sets.newHashSet();
          }
          if (!disjointTags.contains(tag)) {
            disjointTags.add(tag);
          }
        }
      }

      // now check tags
      if (id.tags() != null && !id.tags().isEmpty()) {
        if (tags == null) {
          tags = Maps.newHashMap();
        }

        for (Map.Entry<String, String> pair : id.tags().entrySet()) {
          final String key = pair.getKey();
          if (fullMerge && disjointTags != null && disjointTags.contains(key)) {
            continue;
          }
          if (aggTags != null && aggTags.contains(key)) {
            continue;
          }

          final String extant = tags.get(key);
          if (Strings.isNullOrEmpty(extant)) {
            if (fullMerge) {
              // disjoint!
              if (disjointTags == null) {
                disjointTags = Sets.newHashSet();
              }
              disjointTags.add(key);
            }
          } else if (!extant.equals(pair.getValue())) {
            // different value so we need to move to aggs
            if (aggTags == null) {
              aggTags = Sets.newHashSet();
            }
            aggTags.add(key);
            tags.remove(key);
          }
        }
      }

      // reverse verify the tags
      if (tags != null) {
        final Iterator<Entry<String, String>> iterator = tags.entrySet().iterator();
        while (iterator.hasNext()) {
          final Entry<String, String> pair = iterator.next();
          final String key = pair.getKey();
          if (!id.tags().containsKey(key)) {
            // disjoint!
            if (fullMerge) {
              if (disjointTags == null) {
                disjointTags = Sets.newHashSet();
              }
              disjointTags.add(key);
            }
            iterator.remove();
          }
        }
      }

      // reverse agg
      if (aggTags != null) {
        final Iterator<String> iterator = aggTags.iterator();
        OuterLoop:
        while (iterator.hasNext()) {
          final String tag = iterator.next();
          if (id.tags() != null && id.tags().containsKey(tag)) {
            // good keep it.
            continue;
          }

          if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
            for (final String other : id.aggregatedTags()) {
              if (tag.equals(other)) {
                continue OuterLoop;
              }
            }
          }

          // disjoint!
          if (disjointTags == null) {
            disjointTags = Sets.newHashSet();
          }
          disjointTags.add(tag);
          iterator.remove();
        }
      }

      if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
        if (unique_ids == null) {
          unique_ids = Sets.newHashSet();
        }
        unique_ids.addAll(id.uniqueIds());
      }
    }

    void copyInitial(final TimeSeriesStringId id) {
      if (id.alias() != null) {
        alias = id.alias();
      }
      if (id.namespace() != null) {
        namespace = id.namespace();
      }
      if (id.metric() != null) {
        metric = id.metric();
      }

      if (id.tags() != null && !id.tags().isEmpty()) {
        tags = Maps.newHashMap();
        for (Map.Entry<String, String> entry : id.tags().entrySet()) {
          tags.put(entry.getKey(), entry.getValue());
        }
      }

      if (id.aggregatedTags() != null && !id.aggregatedTags().isEmpty()) {
        aggTags = Sets.newHashSet();
        for (int i = 0; i < id.aggregatedTags().size(); i++) {
          aggTags.add(id.aggregatedTags().get(i));
        }
      }

      if (id.disjointTags() != null && !id.disjointTags().isEmpty()) {
        disjointTags = Sets.newHashSet();
        for (int i = 0; i < id.disjointTags().size(); i++) {
          disjointTags.add( id.disjointTags().get(i));
        }
      }

      if (id.uniqueIds() != null && !id.uniqueIds().isEmpty()) {
        if (unique_ids == null) {
          unique_ids = Sets.newHashSet();
        }
        unique_ids.addAll(id.uniqueIds());
      }
    }
  }
}
