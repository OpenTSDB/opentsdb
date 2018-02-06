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
package net.opentsdb.query.processor;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.DefaultIteratorGroups;
import net.opentsdb.data.iterators.IteratorGroup;
import net.opentsdb.data.iterators.IteratorGroups;
import net.opentsdb.data.iterators.TimeSeriesIterators;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.query.processor.expressions.ExpressionProcessorConfig;

/**
 * A class that performs a join on time series across multiple groups in the
 * time series source. For each time series, a join key is computed based on the
 * series ID (this the iterators must be initialized before they get here). Then
 * the iterators are sorted by key into a map of maps returned by 
 * {@link #join(IteratorGroups)}. 
 * <p>
 * If the operator is set to {@link SetOperator#UNION} then the map is returned
 * as is and the caller must fill in missing group types with proper values or
 * handle them appropriately.
 * <p>
 * If the operator is set to {@link SetOperator#INTERSECTION} then types and/or
 * entire join keys will be removed if one or more groups are missing iterators.
 * 
 * @since 3.0
 */
public class Joiner {
  private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);
  
  /** A non-null config to pull join information from. */
  final ExpressionProcessorConfig config;
  
  /**
   * Default Ctor.
   * @param config A non-null expression config.
   */
  public Joiner(final ExpressionProcessorConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Expression config cannot be null.");
    }
    this.config = config;
  }

  /**
   * Computes the join across {@link TimeSeriesGroupId}s and potentially kicks 
   * any out that don't fulfill the set operator.
   * @param source A non-null set of grouped iterators where the 
   * {@link TimeSeriesGroupId} is the variable name used in the expression.
   * @return A non-null byte map with join keys as the key.
   */
  public Map<String, IteratorGroups> join(final IteratorGroups source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    
    final Map<String, IteratorGroups> joined = Maps.newHashMap();
    for (final IteratorGroup group : source.groups()) {
      for (final TimeSeriesIterator<?> it : group.flattenedIterators()) {
        try {
          final String join_key = joinKey(it.id());
          if (join_key != null) {
            // find the proper map to dump it in
            IteratorGroups joined_group = joined.get(join_key);
            if (joined_group == null) {
              joined_group = new DefaultIteratorGroups();
              joined.put(join_key, joined_group);
            }
            
            joined_group.addIterator(group.id(), it);
          }
        } catch (IOException e) {
          throw new RuntimeException("Unexpected exception while joining", e);
        }
      }
    }
    
    final SetOperator operator = config.getExpression().getJoin() != null ? 
        config.getExpression().getJoin().getOperator() : SetOperator.UNION;
    
    switch (operator) {
    case UNION:
      // nothing to do here. Let the caller handle fills for missing values.
      return joined;
    case INTERSECTION:
      return computeIntersection(joined);
      // TODO - CROSS
    default:
      throw new UnsupportedOperationException("Join operator " + operator 
          + " is not supported yet.");
    }
  }
  
  /**
   * Computes the intersection on the joined data set, comparing all of the 
   * groups under a join key to make sure each one has at least one value.
   * @param joins A non-null list of joins to work on.
   * @return A non-null list of intersected joins.
   */
  private Map<String, IteratorGroups> computeIntersection(
      final Map<String, IteratorGroups> joins) {
    
    // TODO - join on a specific data type. May not care about annotations or 
    // others when performing an expression so those can be "grouped".
    
    // Ugly but since the iterator sets are immutable, we have to convert to
    // a map, then back.
    final Map<String, Map<TimeSeriesGroupId, Map<TimeSeriesId, 
      Map<TypeToken<?>, TimeSeriesIterator<?>>>>> joined = Maps.newHashMap();
    
    for (final Entry<String, IteratorGroups> join : joins.entrySet()) {
      final Map<TimeSeriesGroupId, Map<TimeSeriesId, 
        Map<TypeToken<?>, TimeSeriesIterator<?>>>> group = Maps.newHashMap();
      joined.put(join.getKey(), group);
      
      for (final Entry<TimeSeriesGroupId, IteratorGroup> iterators : 
          join.getValue()) {
        final Map<TimeSeriesId, Map<TypeToken<?>, 
          TimeSeriesIterator<?>>> timeseries = Maps.newHashMap();
        group.put(iterators.getKey(), timeseries);
        for (final TimeSeriesIterators types : iterators.getValue()) {
          final Map<TypeToken<?>, TimeSeriesIterator<?>> typed_iterators =
              Maps.newHashMapWithExpectedSize(types.iterators().size());
          for (final TimeSeriesIterator<?> iterator : types.iterators()) {
            typed_iterators.put(iterator.type(), iterator);
          }
          timeseries.put(types.id(), typed_iterators);
        }
      }
    }
    
    // now compute the intersections.
    final Iterator<Entry<String, Map<TimeSeriesGroupId, Map<TimeSeriesId, 
      Map<TypeToken<?>, TimeSeriesIterator<?>>>>>> top_iterator = 
        joined.entrySet().iterator();
    while (top_iterator.hasNext()) {
      final Entry<String, Map<TimeSeriesGroupId, Map<TimeSeriesId, 
        Map<TypeToken<?>, TimeSeriesIterator<?>>>>> join = top_iterator.next();
      
      final Iterator<Entry<TimeSeriesGroupId, Map<TimeSeriesId, 
        Map<TypeToken<?>, TimeSeriesIterator<?>>>>> group_iterator = 
          join.getValue().entrySet().iterator();
      while (group_iterator.hasNext()) {
        final Entry<TimeSeriesGroupId, Map<TimeSeriesId, 
          Map<TypeToken<?>, TimeSeriesIterator<?>>>> group = 
          group_iterator.next();
        
        final Iterator<Entry<TimeSeriesId, 
          Map<TypeToken<?>, TimeSeriesIterator<?>>>> ts_iterator =
            group.getValue().entrySet().iterator();
        while (ts_iterator.hasNext()) {
          final Entry<TimeSeriesId, Map<TypeToken<?>, 
            TimeSeriesIterator<?>>> timeseries = ts_iterator.next();
          
          // now check the other groups to see if they have data.
          for (final Entry<TimeSeriesGroupId, Map<TimeSeriesId, 
              Map<TypeToken<?>, TimeSeriesIterator<?>>>> other_group : 
                  join.getValue().entrySet()) {
            if (other_group.getKey().equals(group.getKey())) {
              continue;
            }
            
            final Map<TypeToken<?>, TimeSeriesIterator<?>> other_types = 
                other_group.getValue().get(timeseries.getKey());
            if (other_types == null) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("[" + join.getKey() + "] kicking out series " 
                    + timeseries.getKey() + " for group " + group.getKey() 
                    + " as group " + other_group.getKey() 
                    + " does not have data.");
              }
              ts_iterator.remove();
              break;
            }
            
            final Iterator<Entry<TypeToken<?>, TimeSeriesIterator<?>>> 
              type_iterator = timeseries.getValue().entrySet().iterator();
            while(type_iterator.hasNext()) {
              final Entry<TypeToken<?>, TimeSeriesIterator<?>> type = 
                  type_iterator.next();
              if (!other_types.containsKey(type.getKey())) {
                if (LOG.isDebugEnabled()) {
                  LOG.debug("[" + join.getKey() + "] kicking out type " 
                      + type.getKey() + " for group " + group.getKey() 
                      + " as group " + other_group.getKey() 
                      + " does not have data.");
                }
                type_iterator.remove();
                break;
              }
              
            }
          }
        }
        
        if (group.getValue().isEmpty()) {
          group_iterator.remove();
        }
      }
      
      // finally if nothing was present we kick it out
      if (join.getValue().isEmpty()) {
        top_iterator.remove();
      }
    }
    
    final Map<String, IteratorGroups> final_joins = Maps.newHashMap();
    // reverse
    for (final Entry<String, Map<TimeSeriesGroupId, Map<TimeSeriesId, 
        Map<TypeToken<?>, TimeSeriesIterator<?>>>>> groups : joined.entrySet()) {
      final IteratorGroups final_groups = new DefaultIteratorGroups();
      final_joins.put(groups.getKey(), final_groups);
      for (final Entry<TimeSeriesGroupId, Map<TimeSeriesId, 
          Map<TypeToken<?>, TimeSeriesIterator<?>>>> group : 
            groups.getValue().entrySet()) {
        
        for (final Entry<TimeSeriesId, 
            Map<TypeToken<?>, TimeSeriesIterator<?>>> typed : 
          group.getValue().entrySet()) {
          for (final TimeSeriesIterator<?> iterator : typed.getValue().values()) {
            final_groups.addIterator(group.getKey(), iterator);
          }
        }
      }
    }
    
    return final_joins;
  }
  
  /**
   * Generates a join key based on the ID and join configuration.
   * @param id The ID to use for joining.
   * @return A valid join key, an empty key if the ID didn't have any tags,
   * or a null if the ID lacked tags required via the Join config.
   * @throws IOException If the ID was null.
   */
  @VisibleForTesting
  String joinKey(final TimeSeriesId id) throws IOException {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null");
    }
    final StringBuffer buffer = new StringBuffer();
    
    final List<String> tag_keys = config.getTagKeys();
    final boolean include_agg_tags = config.getExpression().getJoin() != null ?
        config.getExpression().getJoin().getIncludeAggTags() : false;
    final boolean include_disjoint_tags = config.getExpression().getJoin() != null ?
        config.getExpression().getJoin().getIncludeDisjointTags() : false;
    if (tag_keys != null) {
      for (final String tag_key : tag_keys) {
        String tag_value = id.tags().get(tag_key);
        if (tag_value != null) {
          buffer.append(tag_key);
          buffer.append(tag_value);
        } else {
          boolean matched = false;
          if (include_agg_tags) {
            for (final String tag : id.aggregatedTags()) {
              if (tag_key.equals(tag)) {
                matched = true;
                break;
              }
            }
          } if (!matched && include_disjoint_tags) {
            for (final String tag : id.disjointTags()) {
              if (tag_key.equals(tag)) {
                matched = true;
                break;
              }
            }
          }
          if (!matched) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring series " + id + " as it doesn't have the "
                  + "required tags ");
            }
            return null;
          }
          buffer.append(tag_key);
        }
      }
    } else {
      // full join!
      if (!id.tags().isEmpty()) {
        // make sure it's sorted is already sorted
        final Map<String, String> tags;
        if (id instanceof TreeMap) {
          tags = id.tags();
        } else {
          tags = new TreeMap<String, String>(id.tags());
        }
        for (final Entry<String, String> pair : tags.entrySet()) {
          buffer.append(pair.getKey());
          buffer.append(pair.getValue());
        }
      }
      
      if (include_agg_tags && !id.aggregatedTags().isEmpty()) {
        // not guaranteed of sorting
        final List<String> sorted = Lists.newArrayList(id.aggregatedTags());
        Collections.sort(sorted);
        for (final String tag : sorted) {
          buffer.append(tag);
        }
      }
      
      if (include_disjoint_tags && !id.disjointTags().isEmpty()) {
        // not guaranteed of sorting
        final List<String> sorted = Lists.newArrayList(id.disjointTags());
        Collections.sort(sorted);
        for (final String tag : sorted) {
          buffer.append(tag);
        }
      }
    }
    
    return buffer.toString();
  }
}
