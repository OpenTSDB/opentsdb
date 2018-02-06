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

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import io.opentracing.Tracer.SpanBuilder;
import net.opentsdb.data.iterators.DefaultIteratorGroup;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.DefaultTimeSeriesIterators;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.context.QueryContext;

/**
 * A class that takes shards groups from multiple clusters or multiple runs
 * and merges them into a single response using the implemented strategy.
 * 
 * @since 3.0
 */
public class DataShardMerger implements DataMerger<IteratorGroups> {
  private static final Logger LOG = LoggerFactory.getLogger(
      DataShardMerger.class);
  
  /** The map of strategies per type to merge. */
  protected Map<TypeToken<?>, DataShardMergeStrategy<?>> strategies;
  
  /**
   * Default Ctor
   */
  public DataShardMerger() {
    strategies = Maps.newHashMapWithExpectedSize(1);
  }
  
  @Override
  public TypeToken<?> type()  {
    return IteratorGroups.TYPE;
  }
  
  /**
   * Adds the given strategy to the map, replacing any existing entries.
   * @param strategy A non-null strategy.
   * @throws IllegalArgumentException if the strategy was null.
   */
  public void registerStrategy(final DataShardMergeStrategy<?> strategy) {
    if (strategy == null) {
      throw new IllegalArgumentException("Strategy cannot be null.");
    }
    strategies.put(strategy.type(), strategy);
  }
  
  /**
   * Merge the given shards groups into one result, joining first on the shards
   * group ID (using {@code .equals()}) then on the time series IDs.
   * <p>
   * Invariant: All groups in the array must be null or have the same base
   * time and time spans. Nulled groups are skipped.
   * 
   * @param groups A non-null list of groups to merge.
   * @param context A non-null context to pull info from.
   * @param tracer_span An optional tracer span.
   * @return A non-null list of merged groups. If all groups in the list were
   * null, then result will be empty.
   * @throws IllegalArgumentException if the context or groups were null.
   */
  @Override
  public IteratorGroups merge(final List<IteratorGroups> groups, 
                                     final QueryContext context, 
                                     final Span tracer_span) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (groups == null) {
      throw new IllegalArgumentException("Shards cannot be null.");
    }
    final Span local_span;
    if (context.getTracer() != null) {
      final SpanBuilder builder = context.getTracer()
          .buildSpan(this.getClass().getSimpleName());
      if (tracer_span != null) {
        builder.asChildOf(tracer_span);
      }
      builder.withTag("shardCount", Integer.toString(groups.size()));
      local_span = builder.start();
    } else {
      local_span = null;
    }
    
    final List<IteratorGroup> unpacked = Lists.newArrayList();
    for (final IteratorGroups set : groups) {
      unpacked.addAll(set.groups());
    }
    final IteratorGroups results = new DefaultIteratorGroups();
    
    // first, join by the Group ID.
    final boolean[] completed = new boolean[unpacked.size()];
    for (int i = 0; i < unpacked.size(); i++) {
      if (completed[i]) {
        continue;
      }
      
      final IteratorGroup group = unpacked.get(i);
      completed[i] = true;
      if (group.iterators() == null || group.iterators().isEmpty()) {
        continue;
      }
      final List<IteratorGroup> same_group = 
          Lists.newArrayListWithExpectedSize(2);
      same_group.add(group);
      for (int y = i + 1; y < unpacked.size(); y++) {
        if (unpacked.get(y).id().equals(group.id())) {
          completed[y] = true;
          same_group.add(unpacked.get(y));
        }
      }
      results.addGroup(mergeGroups(group.id(), same_group, context, local_span));
    }
    if (local_span != null) {
      local_span.finish();
    }
    return results;
  }

  /**
   * Takes one or more groups with the same group ID and joins the 
   * {@link DataShards} in each group then calls the merge data function to
   * merge the results using the proper type mergers. 
   * @param group_id A non-null group ID that is common across all groups and 
   * will be used in the resulting group.
   * @param groups A non-null list of 1 or more groups to merge.
   * @param context A non-null query context.
   * @param tracer_span An optional tracer span.
   * @return A non-null group with the merged results.
   */
  @VisibleForTesting
  IteratorGroup mergeGroups(final TimeSeriesGroupId group_id, 
                              final List<IteratorGroup> groups, 
                              final QueryContext context, 
                              final Span tracer_span) {
    final IteratorGroup group = new DefaultIteratorGroup(group_id);
    
    /** Holds the list of matched shard objects to merge */
    TimeSeriesIterators[] merged = new TimeSeriesIterators[groups.size()];
    
    /** An array of arrays to track when we've matched a shard so we can avoid
     * dupe processing and speed things up as we go along. */
    final boolean[][] processed = new boolean[groups.size()][];
    int order = -1;
    for (int i = 0; i < groups.size(); i++) {
      if (groups.get(i) == null || groups.get(i).iterators() == null) {
        processed[i] = new boolean[0];
      } else {
        processed[i] = new boolean[groups.get(i).iterators().size()];
        if (order < 0) {
          order = groups.get(i).order();
        } else {
          if (order != groups.get(i).order()) {
            throw new IllegalStateException("One or more shards in the set was "
                + "for a different order. Expected: " + order + " but got: " 
                + groups.get(i));
          }
        }
      }
    }

    // TODO - There MUST be a better way than this naive quadratic merge O(n^2)
    // so if you're looking at this and can find it, please help us!
    // outer loop start for iterating over each shard set
    for (int i = 0; i < groups.size(); i++) {
      MergedTimeSeriesId.Builder id = null;
      if (groups.get(i) == null) {
        continue;
      }
      
      // inner loop start for iterating over each shard in each set
      for (int x = 0; x < groups.get(i).iterators().size(); x++) {
        if (processed[i][x]) {
          continue;
        }
        id = MergedTimeSeriesId.newBuilder();
        
        // NOTE: Make sure to reset the shards array here
        merged = new TimeSeriesIterators[groups.size()];
        merged[i] = groups.get(i).iterators().get(x);
        
        if (groups.get(i).iterators().get(x).id().alias() != null) {
          id.setAlias(groups.get(i).iterators().get(x).id().alias());
        }
        id.addSeries(groups.get(i).iterators().get(x).id());
        processed[i][x] = true;
                
        // nested outer loop to start searching the other shard groups
        for (int y = 0; y < groups.size(); y++) {
          if (y == i) {
            // der, skip ourselves of course.
            continue;
          }
          if (groups.get(y) == null) {
            continue;
          }
          
          // nexted inner loop to match against a shard in another group
          for (int z = 0; z < groups.get(y).iterators().size(); z++) {
            if (processed[y][z]) {
              continue;
            }
            // temp build
            final TimeSeriesId temp_id = id.build();
            final TimeSeriesId local_id = groups.get(y).iterators().get(z).id();
            
            // alias check first
            if (!Objects.equals(local_id.alias(), temp_id.alias())) {
              continue;
            }
            
            // namespace fail fast
            if (!Objects.equals(local_id.namespace(), temp_id.namespace())) {
              continue;
            }
//            if (temp_id.namespaces().size() != local_id.namespaces().size()) {
//              continue;
//            }
//            for (final String namespace : temp_id.namespaces()) {
//              if (!local_id.namespaces().contains(namespace)) {
//                matched = false;
//                break;
//              }
//            }
//            if (!matched) {
//              continue;
//            }
            
            // metric check fail fast
            if (!Objects.equals(local_id.metric(), temp_id.metric())) {
              continue;
            }
//            if (temp_id.metrics().size() != local_id.metrics().size()) {
//              continue;
//            }
//            for (final String metric : temp_id.metrics()) {
//              if (!local_id.metrics().contains(metric)) {
//                matched = false;
//                break;
//              }
//            }
//            if (!matched) {
//              continue;
//            }
            
            final Set<String> promoted_agg_tags = Sets.newHashSet();
            final Set<String> promoted_disjoint_tags = Sets.newHashSet();
            boolean matched = true;
            for (final Entry<String, String> pair : temp_id.tags().entrySet()) {
              final String tag_v = local_id.tags().get(pair.getKey());
              if (tag_v != null && tag_v.equals(pair.getValue())) {
                // matched!
                continue;
              }
              
              if (!local_id.aggregatedTags().contains(pair.getKey())) {
                promoted_agg_tags.add(pair.getKey());
              } else if (!local_id.disjointTags().contains(pair.getKey())) {
                promoted_disjoint_tags.add(pair.getKey());
              }
            }
            if (!matched) {
              continue;
            }
            
            // reverse tags processing
            for (final Entry<String, String> pair : local_id.tags().entrySet()) {
              final String tag_v = temp_id.tags().get(pair.getKey());
              if (tag_v != null && tag_v.equals(pair.getValue())) {
                continue;
              }
              
              // this one wasn't in the other tag list so check agg/disjoint.
              if (!temp_id.aggregatedTags().contains(pair.getKey()) &&
                  !temp_id.disjointTags().contains(pair.getKey())) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            // aggs and disjoint
            for (final String tagk : local_id.aggregatedTags()) {
              if (temp_id.aggregatedTags().contains(tagk) || 
                  promoted_agg_tags.contains(tagk)) {
                continue;
              }
              
              if (!temp_id.disjointTags().contains(tagk) &&
                  !promoted_disjoint_tags.contains(tagk)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            for (final String tagk : local_id.disjointTags()) {
              if (temp_id.aggregatedTags().contains(tagk) ||
                  promoted_agg_tags.contains(tagk)) {
                continue;
              }
              if (!temp_id.disjointTags().contains(tagk) && 
                  !promoted_disjoint_tags.contains(tagk)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            // matched!!           
            merged[y] = groups.get(y).iterators().get(z);
            id.addSeries(groups.get(y).iterators().get(z).id());
            processed[y][z] = true;
          } // end dupe inner data shard loop
        } // end dupe outer shards loop

        group.addIterators(mergeData(merged, id.build(), context, tracer_span));
      } // end inner data shard loop
    } // end outer shards loop

    return group;
  }
  
  /**
   * Once a set of shards are joined they're passed to this method to merge.
   * @param to_merge A non-null set of shards to merge. May be empty though.
   * @param id A non-null ID.
   * @param context A non-null query context.
   * @param tracer_span An optional tracer span.
   * @return A non-null shards object with merged data types for a single
   * ID.
   * @throws IllegalArgumentException if the shard array, id or context were
   * null.
   */
  protected TimeSeriesIterators mergeData(
                            final TimeSeriesIterators[] to_merge, 
                                 final TimeSeriesId id, 
                                 final QueryContext context, 
                                 final Span tracer_span) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (to_merge == null) {
      throw new IllegalArgumentException("Merge array cannot be null.");
    }
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null.");
    }
    final TimeSeriesIterators merged = new DefaultTimeSeriesIterators(id);
    final Map<TypeToken<?>, List<TimeSeriesIterator<?>>> types = 
        Maps.newHashMapWithExpectedSize(1);
    for (final TimeSeriesIterators shards : to_merge) {
      if (shards == null) {
        continue;
      }
      for (final TimeSeriesIterator<?> shard : shards.iterators()) {
        List<TimeSeriesIterator<?>> list = types.get(shard.type());
        if (list == null) {
          list = Lists.newArrayListWithExpectedSize(1);
          types.put(shard.type(), list);
        }
        list.add(shard);
      }
    }
    
    int dropped_types = 0;
    for (final Entry<TypeToken<?>, List<TimeSeriesIterator<?>>> entry : types.entrySet()) {
      if (entry.getValue().size() == 1) {
        merged.addIterator(entry.getValue().get(0));
      } else {
        final DataShardMergeStrategy<?> strategy = strategies.get(entry.getKey());
        if (strategy == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No merge strategy found for type " + entry.getKey() 
              + ". Dropping type.");
          }
          ++dropped_types;
        } else {
          merged.addIterator(
              strategy.merge(id, entry.getValue(), context, tracer_span));
        }
      }
    }
    if (tracer_span != null) {
      tracer_span.setTag("droppedTypes", dropped_types);
    }
    return merged;
  }
  
  @VisibleForTesting
  Map<TypeToken<?>, DataShardMergeStrategy<?>> strategies() {
    return strategies;
  }
}
