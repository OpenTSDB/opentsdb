// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.data;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import io.opentracing.Span;
import io.opentracing.Tracer.SpanBuilder;
import net.opentsdb.query.context.QueryContext;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;

/**
 * A class that takes shards groups from multiple clusters or multiple runs
 * and merges them into a single response using the implemented strategy.
 * 
 * @since 3.0
 */
public class DataShardMerger implements DataMerger<DataShardsGroup> {
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
    return DataShardsGroup.TYPE;
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
   * Merge the given shards groups into one result.
   * <p>
   * Invariant: All shards in the array must be null or have the same base
   * time and time spans. Nulled shards are skipped.
   * 
   * @param shards A non-null array of shards to merge.
   * @param context A non-null context to pull info from.
   * @param tracer_span An optional tracer span.
   * @return A shards object with the merged results. If all shards in the
   * list were null, the resulting shard will be null.
   * @throws IllegalArgumentException if the context or shards were null.
   */
  @Override
  public DataShardsGroup merge(final List<DataShardsGroup> shards, 
                               final QueryContext context, 
                               final Span tracer_span) {
    if (context == null) {
      throw new IllegalArgumentException("Context cannot be null.");
    }
    if (shards == null) {
      throw new IllegalArgumentException("Shards cannot be null.");
    }
    final Span local_span;
    if (context.getTracer() != null) {
      final SpanBuilder builder = context.getTracer()
          .buildSpan(this.getClass().getSimpleName());
      if (tracer_span != null) {
        builder.asChildOf(tracer_span);
      }
      builder.withTag("shardCount", Integer.toString(shards.size()));
      local_span = builder.start();
    } else {
      local_span = null;
    }
        
    TimeSeriesGroupId group_id = null;
    for (final DataShardsGroup group : shards) {
      if (group != null) {
        group_id = group.id;
        break;
      }
    }
    final DataShardsGroup group = new DefaultDataShardsGroup(group_id);
    
    /** Holds the list of matched shard objects to merge */
    DataShards[] merged = new DataShards[shards.size()];
    
    /** An array of arrays to track when we've matched a shard so we can avoid
     * dupe processing and speed things up as we go along. */
    final boolean[][] processed = new boolean[shards.size()][];
    int order = -1;
    TimeStamp base_time = null;
    for (int i = 0; i < shards.size(); i++) {
      if (shards.get(i) == null) {
        processed[i] = new boolean[0];
      } else {
        processed[i] = new boolean[shards.get(i).data.size()];
        if (order < 0) {
          order = shards.get(i).order();
        } else {
          if (order != shards.get(i).order()) {
            throw new IllegalStateException("One or more shards in the set was "
                + "for a different order. Expected: " + order + " but got: " 
                + shards.get(i));
          }
        }
        if (base_time == null) {
          base_time = shards.get(i).baseTime();
        }
      }
    }

    // TODO - There MUST be a better way than this naive quadratic merge O(n^2)
    // so if you're looking at this and can find it, please help us!
    int nulls = 0;
    // outer loop start for iterating over each shard set
    for (int i = 0; i < shards.size(); i++) {
      MergedTimeSeriesId.Builder id = null;
      if (shards.get(i) == null) {
        ++nulls;
        continue;
      }
      
      // inner loop start for iterating over each shard in each set
      for (int x = 0; x < shards.get(i).data().size(); x++) {
        if (processed[i][x]) {
          continue;
        }
        id = MergedTimeSeriesId.newBuilder();
        
        // NOTE: Make sure to reset the shards array here
        merged = new DataShards[shards.size()];
        merged[i] = shards.get(i).data().get(x);
        
        if (shards.get(i).data().get(x).id().alias() != null) {
          id.setAlias(shards.get(i).data().get(x).id().alias());
        }
        id.addSeries(shards.get(i).data().get(x).id());
        processed[i][x] = true;
                
        // nested outer loop to start searching the other shard groups
        for (int y = 0; y < shards.size(); y++) {
          if (y == i) {
            // der, skip ourselves of course.
            continue;
          }
          if (shards.get(y) == null) {
            continue;
          }
          
          // nexted inner loop to match against a shard in another group
          for (int z = 0; z < shards.get(y).data().size(); z++) {
            if (processed[y][z]) {
              continue;
            }
            // temp build
            final TimeSeriesId temp_id = id.build();
            final TimeSeriesId local_id = shards.get(y).data().get(z).id();
            
            // alias check first
            if (temp_id.alias() != null && temp_id.alias().length > 0) {
              if (Bytes.memcmp(temp_id.alias(), local_id.alias()) != 0) {
                // fail fast
                continue;
              }
            }
            
            boolean matched = true;
            // namespace fail fast
            if (temp_id.namespaces().size() != local_id.namespaces().size()) {
              continue;
            }
            for (final byte[] namespace : temp_id.namespaces()) {
              if (!Bytes.contains(local_id.namespaces(), namespace)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            // metric check fail fast
            matched = true;
            if (temp_id.metrics().size() != local_id.metrics().size()) {
              continue;
            }
            for (final byte[] metric : temp_id.metrics()) {
              if (!Bytes.contains(local_id.metrics(), metric)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            final ByteSet promoted_agg_tags = new ByteSet();
            final ByteSet promoted_disjoint_tags = new ByteSet();
            matched = true;
            for (final Entry<byte[], byte[]> pair : temp_id.tags().entrySet()) {
              final byte[] tag_v = local_id.tags().get(pair.getKey());
              if (Bytes.memcmpMaybeNull(tag_v, pair.getValue()) == 0) {
                // matched!
                continue;
              }
              
              if (!Bytes.contains(local_id.aggregatedTags(), pair.getKey())) {
                promoted_agg_tags.add(pair.getKey());
              } else if (!Bytes.contains(local_id.disjointTags(), pair.getKey())) {
                promoted_disjoint_tags.add(pair.getKey());
              }
            }
            if (!matched) {
              continue;
            }
            
            // reverse tags processing
            for (final Entry<byte[], byte[]> pair : local_id.tags().entrySet()) {
              final byte[] tag_v = temp_id.tags.get(pair.getKey());
              if (Bytes.memcmpMaybeNull(tag_v, pair.getValue()) == 0) {
                continue;
              }
              
              // this one wasn't in the other tag list so check agg/disjoint.
              if (!Bytes.contains(temp_id.aggregatedTags(), pair.getKey()) &&
                  !Bytes.contains(temp_id.disjointTags(), pair.getKey())) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            // aggs and disjoint
            for (final byte[] tagk : local_id.aggregatedTags()) {
              if (Bytes.contains(temp_id.aggregatedTags(), tagk) || 
                  promoted_agg_tags.contains(tagk)) {
                continue;
              }
              
              if (!Bytes.contains(temp_id.disjointTags(), tagk) &&
                  !promoted_disjoint_tags.contains(tagk)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            for (final byte[] tagk : local_id.disjointTags()) {
              if (Bytes.contains(temp_id.aggregatedTags(), tagk) ||
                  promoted_agg_tags.contains(tagk)) {
                continue;
              }
              if (!Bytes.contains(temp_id.disjointTags(), tagk) && 
                  !promoted_disjoint_tags.contains(tagk)) {
                matched = false;
                break;
              }
            }
            if (!matched) {
              continue;
            }
            
            // matched!!           
            merged[y] = shards.get(y).data().get(z);
            id.addSeries(shards.get(y).data().get(z).id());
            processed[y][z] = true;
          } // end dupe inner data shard loop
        } // end dupe outer shards loop

        group.addShards(mergeData(merged, id.build(), context, local_span));
      } // end inner data shard loop
    } // end outer shards loop
    
    if (local_span != null) {
      local_span.setTag("nulledShards", nulls);
      local_span.finish();
    }
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
  protected DataShards mergeData(final DataShards[] to_merge, 
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
    final DataShards merged = new DefaultDataShards(id);
    final Map<TypeToken<?>, List<DataShard<?>>> types = 
        Maps.newHashMapWithExpectedSize(1);
    for (final DataShards shards : to_merge) {
      if (shards == null) {
        continue;
      }
      for (final DataShard<?> shard : shards.data()) {
        List<DataShard<?>> list = types.get(shard.type());
        if (list == null) {
          list = Lists.newArrayListWithExpectedSize(1);
          types.put(shard.type(), list);
        }
        list.add(shard);
      }
    }
    
    int dropped_types = 0;
    for (final Entry<TypeToken<?>, List<DataShard<?>>> entry : types.entrySet()) {
      if (entry.getValue().size() == 1) {
        merged.addShard(entry.getValue().get(0));
      } else {
        final DataShardMergeStrategy<?> strategy = strategies.get(entry.getKey());
        if (strategy == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("No merge strategy found for type " + entry.getKey() 
              + ". Dropping type.");
          }
          ++dropped_types;
        } else {
          merged.addShard(
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
