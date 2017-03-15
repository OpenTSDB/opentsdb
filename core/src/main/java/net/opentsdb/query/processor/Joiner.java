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
package net.opentsdb.query.processor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.data.TimeSeriesGroupId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.iterators.TimeSeriesIterator;
import net.opentsdb.query.pojo.Filter;
import net.opentsdb.query.pojo.Join;
import net.opentsdb.query.pojo.Join.SetOperator;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A class that performs a join on time series across multiple groups in the
 * time series source. For each time series, a join key is computed based on the
 * series ID (this the iterators must be initialized before they get here). Then
 * the iterators are sorted by key into a map of maps returned by 
 * {@link #join(TimeSeriesProcessor)}. 
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
  
  final JoinConfig config;
  final Join join;
  final Filter filters;
  
  public Joiner(final JoinConfig config) {
    this.config = config;
    join = ((JoinConfig) config).getJoin();
    filters = ((JoinConfig) config).getFilters();
  }

  /**
   * Computes the join across {@link TimeSeriesGroupId}s and potentially kicks 
   * any out that don't fulfill the set operator.
   * @param source A non-null source to pull iterators from.
   * @return A non-null byte map with join keys as the key.
   */
  public ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>>> join(final TimeSeriesProcessor source) {
    if (source == null) {
      throw new IllegalArgumentException("Source cannot be null.");
    }
    // <joinkey, <groupid, <type, Its>>>
    final ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
      List<TimeSeriesIterator<?>>>>> joined = 
        new ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
          List<TimeSeriesIterator<?>>>>>();

    for (final Entry<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>> group : source.iterators().entrySet()) {
      for (final Entry<TypeToken<?>, List<TimeSeriesIterator<?>>> type : 
          group.getValue().entrySet()) {
        for (final TimeSeriesIterator<?> it : type.getValue()) {
          try {
            final byte[] join_key = joinKey(it.id());
            if (join_key != null) {
              // find the proper map to dump it in
              Map<TimeSeriesGroupId, Map<TypeToken<?>, 
                List<TimeSeriesIterator<?>>>> joined_group = joined.get(join_key);
              if (joined_group == null) {
                joined_group = Maps.newHashMap();
                joined.put(join_key, joined_group);
              }
              
              Map<TypeToken<?>, List<TimeSeriesIterator<?>>> joined_type
                = joined_group.get(group.getKey());
              if (joined_type == null) {
                joined_type = Maps.newHashMap();
                joined_group.put(group.getKey(), joined_type);
              }
              List<TimeSeriesIterator<?>> joined_its = joined_type.get(it.type());
              if (joined_its == null) {
                joined_its = Lists.newArrayList();
                joined_type.put(it.type(), joined_its);
              }
              joined_its.add(it);
            }
          } catch (IOException e) {
            throw new RuntimeException("Unexpected exception while joining", e);
          }
        }
      }
    }
    
    switch (join.getOperator()) {
    case UNION:
      // nothing to do here. Let the caller handle fills for missing values.
      return joined;
    case INTERSECTION:
      return computeIntersection(joined);
      // TODO - CROSS
    default:
      throw new UnsupportedOperationException("Join operator " 
          + join.getOperator() + " is not supported yet.");
    }
  }
  
  /**
   * Computes the intersection on the joined data set, comparing all of the 
   * groups under a join key to make sure each one has at least one value.
   * @param joins A non-null list of joins to work on.
   * @return A non-null list of intersected joins.
   */
  private ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
    List<TimeSeriesIterator<?>>>>> computeIntersection(
      final ByteMap<Map<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>>> joins) {
    
    // TODO - join on a specific data type. May not care about annotations or 
    // others when performin an expression so those can be "grouped".
    
    final Iterator<Entry<byte[], Map<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>>>> top_iterator = joins.iterator();
    while (top_iterator.hasNext()) {
      final Entry<byte[], Map<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>>> join = top_iterator.next();
      
      final Iterator<Entry<TimeSeriesGroupId, Map<TypeToken<?>, 
        List<TimeSeriesIterator<?>>>>> group_iterator = 
          join.getValue().entrySet().iterator();
      while (group_iterator.hasNext()) {
        final Entry<TimeSeriesGroupId, Map<TypeToken<?>, 
          List<TimeSeriesIterator<?>>>> group = group_iterator.next();
        
        final Iterator<Entry<TypeToken<?>, List<TimeSeriesIterator<?>>>> type_iterator =
            group.getValue().entrySet().iterator();
        while (type_iterator.hasNext()) {
          final Entry<TypeToken<?>, List<TimeSeriesIterator<?>>> type = 
              type_iterator.next();
          
          // now check the other groups to see if they have data.
          for (final Entry<TimeSeriesGroupId, Map<TypeToken<?>, 
              List<TimeSeriesIterator<?>>>> other_group : join.getValue().entrySet()) {
            if (other_group.getKey().equals(group.getKey())) {
              continue;
            }
            
            if (!other_group.getValue().containsKey(type.getKey())) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("[" + Bytes.pretty(join.getKey()) + "] kicking out type " 
                    + type.getKey() + " for group " + group.getKey() 
                    + " as group " + other_group.getKey() 
                    + " does not have data.");
              }
              type_iterator.remove();
              break;
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
    
    return joins;
  }
  
  /**
   * Generates a join key based on the ID and join configuration.
   * @param id The ID to use for joining.
   * @return A valid join key, an empty key if the ID didn't have any tags,
   * or a null if the ID lacked tags required via the Join config.
   * @throws IOException If the ID was null.
   */
  @VisibleForTesting
  byte[] joinKey(final TimeSeriesId id) throws IOException {
    if (id == null) {
      throw new IllegalArgumentException("ID cannot be null");
    }
    final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
    
    final List<byte[]> tag_keys = ((JoinConfig) config).getTagKeys();
    if (tag_keys != null) {
      for (final byte[] tag_key : tag_keys) {
        byte[] tag_value = id.tags().get(tag_key);
        if (tag_value != null) {
          buffer.write(tag_key);
          buffer.write(tag_value);
        } else {
          boolean matched = false;
          if (join.getIncludeAggTags()) {
            for (final byte[] tag : id.aggregatedTags()) {
              if (Bytes.memcmp(tag_key, tag) == 0) {
                matched = true;
                break;
              }
            }
          } if (!matched && join.getIncludeDisjointTags()) {
            for (final byte[] tag : id.disjointTags()) {
              if (Bytes.memcmp(tag_key, tag) == 0) {
                matched = true;
                break;
              }
            }
          }
          if (!matched) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Ignoring series " + id + " as it doesn't have the "
                  + "required " + 
                    (join.getTags() != null && !join.getTags().isEmpty() ? "Join" : "Query")
                  + " tag from " + 
                  (join.getTags() != null && !join.getTags().isEmpty() ? 
                      join.toString() : filters.toString()));
            }
            return null;
          }
          buffer.write(tag_key);
        }
      }
    } else {
      // full join!
      if (!id.tags().isEmpty()) {
        // ByteMap is already sorted
        for (final Entry<byte[], byte[]> pair : id.tags().entrySet()) {
          buffer.write(pair.getKey());
          buffer.write(pair.getValue());
        }
      }
      
      if (join.getIncludeAggTags() && !id.aggregatedTags().isEmpty()) {
        // not guaranteed of sorting
        final List<byte[]> sorted = Lists.newArrayList(id.aggregatedTags());
        Collections.sort(sorted, Bytes.MEMCMP);
        for (final byte[] tag : sorted) {
          buffer.write(tag);
        }
      }
      
      if (join.getIncludeDisjointTags() && !id.disjointTags().isEmpty()) {
        // not guaranteed of sorting
        final List<byte[]> sorted = Lists.newArrayList(id.disjointTags());
        Collections.sort(sorted, Bytes.MEMCMP);
        for (final byte[] tag : sorted) {
          buffer.write(tag);
        }
      }
    }
    
    buffer.close();
    return buffer.toByteArray();
  }
}
