// This file is part of OpenTSDB.
// Copyright (C) 2017-2020  The OpenTSDB Authors.
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
package net.opentsdb.query.joins;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.NavigableMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.query.processor.expressions.ExpressionParseNode;
import net.opentsdb.query.processor.expressions.TernaryParseNode;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.XXHash;
import net.opentsdb.utils.Bytes.ByteMap;

/**
 * A thread-safe class to perform join operations across time series.
 * It performs a hash join using the XXhash function to compute a hash
 * on the combination of tag values configured in the {@link JoinConfig}.
 * <p>
 * For byte encoded IDs, make sure to convert the config tags and call
 * {@link #setEncodedJoins(ByteMap)}. Also make sure the join key, if it
 * involves namespaces and/or metrics, is encoded. This class will not
 * decode the IDs before joining.
 * 
 * TODO - Add a case-insensitive flag for the config. This requires 
 * resolving byte IDs though.
 * 
 * @since 3.0
 */
public class Joiner {
  private static final Logger LOG = LoggerFactory.getLogger(Joiner.class);
  
  /** The operand of a binary or ternary expression. */
  public static enum Operand {
    LEFT,
    RIGHT,
    CONDITION
  }
  
  /** A non-null config to pull join information from. */
  protected final JoinConfig config;
  
  /** An optional map populated when the series being joined are encoded
   * and need their keys as byte arrays. */
  protected ByteMap<byte[]> encoded_joins;
  
  /**
   * Default Ctor.
   * @param config A non-null expression config.
   */
  public Joiner(final JoinConfig config) {
    if (config == null) {
      throw new IllegalArgumentException("Join config cannot be null.");
    }
    this.config = config;
  }

  /** @return The encoded joins map. Used to determine if we need to 
   * perform the resolution. */
  public ByteMap<byte[]> encodedJoins() {
    return encoded_joins;
  }
  
  /** @param encoded_joins A non-null set of encoded join tags. */
  public void setEncodedJoins(final ByteMap<byte[]> encoded_joins) {
    this.encoded_joins = encoded_joins;
  }
  
  /**
   * Executes the configured join on the result set using the given keys
   * to match results on. The keys can be either the namespace + alias or
   * namespace + metric where the namespace can be null. Joins are 
   * performed by hashing on the IDs and the join tags. Note that if the 
   * the "use_alias" flag is present but the alias fails to match either
   * key, we will fall back to the metric name. This is for use with 
   * sub-expressions.
   * @param results A non-null and non-empty list of results.
   * @param left_key A non-null and non-empty left join key.
   * @param right_key A non-null and non-empty right join key.
   * @param use_alias Whether or not to use the alias when fetching the key.
   * @return An iterator with pairs of left and right joined time series
   * if the join was successful. The iterator may be empty if all series
   * were rejected.
   * @throws IllegalArgumentException if any args were null or invalid.
   */
  public Iterable<TimeSeries[]> join(final Collection<QueryResult> results, 
                                     final ExpressionParseNode expression_config,
                                     final byte[] left_key, 
                                     final byte[] right_key,
                                     final byte[] ternary_key) {
    if (results == null || results.isEmpty()) {
      throw new IllegalArgumentException("Results list can't be null "
          + "or empty.");
    }
    if ((left_key == null || left_key.length < 1) && 
        (right_key == null || right_key.length < 1) && 
        (ternary_key == null || ternary_key.length < 1)) {
      throw new IllegalArgumentException("Must have at least one key.");
    }
    
    if (LOG.isTraceEnabled()) {
      LOG.trace("Joiner Left: " + (left_key != null ? 
              new String(left_key, Const.UTF8_CHARSET) : "null") 
          + "  Right: " + (right_key != null ? 
              new String(right_key, Const.UTF8_CHARSET) : "null") 
          + "  Ternary: " + (ternary_key == null ? "null" : 
              new String(ternary_key, Const.UTF8_CHARSET)));
    }
    
    if (results.iterator().next().idType() == Const.TS_BYTE_ID &&
        encoded_joins == null &&
        config.getJoinType() != JoinType.NATURAL &&
        config.getJoinType() != JoinType.NATURAL_OUTER) {
      throw new IllegalStateException("Received a result with encoded "
          + "IDs but the local encoded tags map was null.");
    }
    
    int expected_sets = left_key != null ? 1 : 0;
    expected_sets += right_key != null ? 1 : 0;
    expected_sets += ternary_key != null ? 1 : 0;
    
    final KeyedHashedJoinSet join_set = ternary_key != null ?
        new TernaryKeyedHashedJoinSet(config.type, expected_sets) :
        new KeyedHashedJoinSet(config.type, expected_sets, ternary_key != null);
    
    // calculate the hash for every series and let the hasher kick out
    // inapplicable series.
    for (final QueryResult result : results) {
      if (result == null) {
        continue;
      }
      
      final Operand operand;
      if (expression_config.getLeftId() != null &&
          expression_config.getLeftId().equals(result.dataSource())) {
        operand = Operand.LEFT;
        if (left_key == null) {
          LOG.warn("Received a result set for the left ID: " 
              + expression_config.getLeftId() + " but the left key was null.");
          continue;
        }
      } else if (expression_config.getRightId() != null &&
          expression_config.getRightId().equals(result.dataSource())) {
        operand = Operand.RIGHT;
        if (right_key == null) {
          LOG.warn("Received a result set for the right ID: " 
              + expression_config.getRightId() + " but the right key was null.");
          continue;
        }
      } else if (expression_config instanceof TernaryParseNode && 
          ((TernaryParseNode) expression_config).getConditionId().equals(
              result.dataSource())) {
        operand = Operand.CONDITION;
        if (ternary_key == null) {
          LOG.warn("Received a result set for the ternary ID: " 
              + ((TernaryParseNode) expression_config).getConditionId() 
              + " but the ternary key was null.");
          continue;
        }
      } else {
        LOG.warn("Result in our set that we didn't want: " + result.dataSource());
        continue;
      }
      
      // TODO - don't do bytes and allocations here. If we drop the namespace
      // field, we can use long hashes!
      for (final TimeSeries ts : result.timeSeries()) {
        if (ts.id().type() == Const.TS_BYTE_ID) {
          final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
          final byte[] key;
          if (id.namespace() == null || id.namespace().length < 1) {
            key = id.metric();
          } else {
            key = com.google.common.primitives.Bytes.concat(
                id.namespace(), id.metric());
          }
          
          switch (operand) {
          case LEFT:
            if (Bytes.memcmp(key, left_key) == 0) {
              hashByteId(operand, ts, join_set);
            } else {
              // TODO - log ejection
              continue;
            }
            break;
          case RIGHT:
            if (Bytes.memcmp(key, right_key) == 0) {
              hashByteId(operand, ts, join_set);
            } else {
              // TODO - log ejection
              continue;
            }
            break;
          case CONDITION:
            if (Bytes.memcmp(key, ternary_key) == 0) {
              hashByteId(operand, ts, join_set);
            } else {
              // TODO - log ejection
              continue;
            }
            break;
          default:
            // TODO - log ejection
            continue;
          }
        } else {
          final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
          final String key;
            key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.metric() :
                  id.namespace() + id.metric();
          final byte[] key_in_bytes = key.getBytes(Const.UTF8_CHARSET);
          if (operand == Operand.LEFT &&
              Bytes.memcmp(key_in_bytes, left_key) == 0) {
            hashStringId(operand, ts, join_set);
          } else if (operand == Operand.RIGHT &&
                     Bytes.memcmp(key_in_bytes, right_key) == 0) {
            hashStringId(operand, ts, join_set);
          } else if (operand == Operand.CONDITION && 
                     Bytes.memcmp(key_in_bytes, ternary_key) == 0) {
            hashStringId(operand, ts, join_set);
          } else {
            // TODO - log ejection
            continue;
          }
        }
      }
    }
    
    return join_set;
  }
  
  /**
   * Executes the given join as a filter, simply checking to see if 
   * each tag matches the given filter (namespace + alias or namespace +
   * metric, allowing nulls for the namespace) and join tags.
   * @param results A non-null and non-empty list of results.
   * @param filter A non-null and non-empty filter.
   * @param left Whether or not to use the left or right tags and to 
   * populate the left or right entry in each pair.
   * @param use_alias Whether or not to use the alias when fetching the key.
   * @return An iterator with pairs of left and right joined time series
   * if the join was successful. The iterator may be empty if all series
   * were rejected.
   * @throws IllegalArgumentException if any args were null or invalid.
   */
  public Iterable<TimeSeries[]> join(
      final Collection<QueryResult> results,
      final byte[] filter,
      final boolean left,
      final boolean use_alias) {
    if (results == null || results.isEmpty()) {
      throw new IllegalArgumentException("Results can't be null or empty.");
    }
    if (filter == null || filter.length < 1) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    
    if (results.iterator().next().idType() == Const.TS_BYTE_ID && 
        encoded_joins == null &&
        config.getJoinType() != JoinType.NATURAL &&
        config.getJoinType() != JoinType.NATURAL_OUTER) {
      throw new IllegalStateException("Received a result with encoded "
          + "IDs but the local encoded tags map was null.");
    }
    
    final List<TimeSeries[]> join_set = Lists.newArrayList();
    
    // calculate the hashes for every time series and joins.
    for (final QueryResult result : results) {
      for (final TimeSeries ts : result.timeSeries()) {
        if (ts.id().type() == Const.TS_BYTE_ID) {
          final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
          final byte[] key;
          if (use_alias) {
            final byte[] local_key;
            if (id.namespace() == null || id.namespace().length < 1) {
              local_key = id.alias();
            } else {
              local_key = com.google.common.primitives.Bytes.concat(
                  id.namespace(), id.alias());
            }
            
            if (Bytes.memcmp(filter, local_key) != 0) {
              // we didn't match on the alias so try the metric.
              if (id.namespace() == null || id.namespace().length < 1) {
                key = id.metric();
              } else {
                key = com.google.common.primitives.Bytes.concat(
                    id.namespace(), id.metric());
              }
            } else {
              key = local_key;
            }
          } else {
            if (id.namespace() == null || id.namespace().length < 1) {
              key = id.metric();
            } else {
              key = com.google.common.primitives.Bytes.concat(
                  id.namespace(), id.metric());
            }
          }
          
          if (Bytes.memcmp(filter, key) == 0) {
            boolean satisfied_joins = true;
            if (encoded_joins != null) {
              for (final Entry<byte[], byte[]> tags : encoded_joins) {
                if (left) {
                  if (!id.tags().containsKey(tags.getKey())) {
                    satisfied_joins = false;
                    break;
                    // TODO - log ejection
                  }
                } else {
                  if (!id.tags().containsKey(tags.getValue())) {
                    satisfied_joins = false;
                    break;
                    // TODO - log ejection
                  }
                }
              }
            }
            
            if (satisfied_joins) {
              join_set.add(new TimeSeries[] {
                  left ? ts : null, left ? null : ts });
            }
          }
          // TODO - log ejection
        } else {
          final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
          final String key;
          if (use_alias) {
            final String local_key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.alias() :
                  id.namespace() + id.alias();
            byte[] key_in_bytes = local_key.getBytes(Const.UTF8_CHARSET);
            if (Bytes.memcmp(filter, key_in_bytes) != 0) {
              // we didn't match on the alias so try the metric.
              key = Strings.isNullOrEmpty(id.namespace()) ? 
                  id.metric() :
                    id.namespace() + id.metric();
            } else {
              key = local_key;
            }
          } else {
            key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.metric() :
                  id.namespace() + id.metric();
          }
          
          if (Bytes.memcmp(filter, key.getBytes(Const.UTF8_CHARSET)) == 0) {
            boolean satisfied_joins = true;
            for (final Entry<String, String> tags : config.joins.entrySet()) {
              if (left) {
                if (!id.tags().containsKey(tags.getKey())) {
                  satisfied_joins = false;
                  break;
                  // TODO - log ejection
                }
              } else {
                if (!id.tags().containsKey(tags.getValue())) {
                  satisfied_joins = false;
                  break;
                  // TODO - log ejection
                }
              }
            }
            
            if (satisfied_joins) {
              join_set.add(new TimeSeries[] {
                  left ? ts : null, left ? null : ts });
            }
          }
        }
      }
    }
    
    return join_set;
  }
  
  /**
   * Joins one or two IDs and applies the given alias. Uses the join 
   * config to determine what data is pulled from the IDs. Either ID 
   * may be null, but not both. Note that tags are promoted to the
   * aggregate or disjoint list if either series has different values. 
   * Because of that, joins can produce more than one series with the
   * same join ID in one join.
   * <p>
   * The resulting time series metric will be the value of as.
   * @param left The left hand ID. May be null.
   * @param right The right hand ID. May be null.
   * @param as A non-null and non-empty name for the metric.
   * @return The joined ID.
   * @throws IllegalArgumentException if the alias was null or empty or
   * all IDs were null.
   */
  public TimeSeriesId joinIds(final TimeSeries left, 
                              final TimeSeries right, 
                              final String as,
                              final JoinType join_type) {
    if (Strings.isNullOrEmpty(as)) {
      throw new IllegalArgumentException("As (new metric name) cannot "
          + "be null or empty.");
    }
    if (left == null && right == null) {
      throw new IllegalArgumentException("At least one ID must not be null.");
    }
    
    if (left != null && right != null) {
      // NOTE: We assume both are of the same type. Need to verify that
      // upstream.
      if (left.id().type() == Const.TS_BYTE_ID) {
        return joinIds((TimeSeriesByteId) left.id(), 
            (TimeSeriesByteId) right.id(), as, join_type);
      } else {
        return joinIds((TimeSeriesStringId) left.id(), 
            (TimeSeriesStringId) right.id(), as, join_type);
      }
    } else if (left == null) {
      if (right.id().type() == Const.TS_BYTE_ID) {
        return new ByteIdOverride((TimeSeriesByteId) right.id(), as);
      } else {
        final TimeSeriesStringId id = (TimeSeriesStringId) right.id();
        return BaseTimeSeriesStringId.newBuilder()
            .setAlias(as)
            .setNamespace(id.namespace())
            .setMetric(as)
            .setTags(id.tags())
            .setAggregatedTags(id.aggregatedTags())
            .setDisjointTags(id.disjointTags())
            .setUniqueId(id.uniqueIds())
            .setHits(id.hits())
            .build();
      }
    } else {
      if (left.id().type() == Const.TS_BYTE_ID) {
        return new ByteIdOverride((TimeSeriesByteId) left.id(), as);
      } else {
        final TimeSeriesStringId id = (TimeSeriesStringId) left.id();
        return BaseTimeSeriesStringId.newBuilder()
            .setAlias(as)
            .setNamespace(id.namespace())
            .setMetric(as)
            .setTags(id.tags())
            .setAggregatedTags(id.aggregatedTags())
            .setDisjointTags(id.disjointTags())
            .setUniqueId(id.uniqueIds())
            .setHits(id.hits())
            .build();
      }
    }
  }
  
  /**
   * Joins the byte IDs.
   * @param left Left ID.
   * @param right Right ID.
   * @param as A non-null and non-empty name to use for the metric.
   * @return The joined ID.
   */
  @VisibleForTesting
  TimeSeriesId joinIds(final TimeSeriesStringId left, 
                       final TimeSeriesStringId right, 
                       final String as,
                       final JoinType join_type) {
    final BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId
        .newBuilder()
        .setNamespace(left.namespace())
        .setAlias(as)
        .setMetric(as);
    
    if (join_type == JoinType.CROSS && 
        (left.tags() == null || left.tags().isEmpty() ||
         right.tags() == null || right.tags().isEmpty())) {
      // we have a cross with a flattened series on one side.
      if (left.tags() != null && !left.tags().isEmpty()) {
        builder.setTags(Maps.newHashMap(left.tags()));
        if (left.aggregatedTags() != null && !left.aggregatedTags().isEmpty()) {
          builder.setAggregatedTags(Lists.newArrayList(left.aggregatedTags()));
        }
        if (left.disjointTags() != null && !left.disjointTags().isEmpty()) {
          builder.setDisjointTags(Lists.newArrayList(left.disjointTags()));
        }
        return builder.build();
      }
      builder.setTags(Maps.newHashMap(right.tags()));
      if (right.aggregatedTags() != null && !right.aggregatedTags().isEmpty()) {
        builder.setAggregatedTags(Lists.newArrayList(right.aggregatedTags()));
      }
      if (right.disjointTags() != null && !right.disjointTags().isEmpty()) {
        builder.setDisjointTags(Lists.newArrayList(right.disjointTags()));
      }
      return builder.build();
    }
    
    final Set<String> agg_tags = Sets.newHashSet();
    final Set<String> disj_tags = Sets.newHashSet();
    agg_tags.addAll(left.aggregatedTags());
    final Iterator<String> it = agg_tags.iterator();
    while (it.hasNext()) {
      final String tag = it.next();
      if (!right.aggregatedTags().contains(tag)) {
        it.remove();
        disj_tags.add(tag);
      }
    }
    for (final String tag : right.disjointTags()) {
      if (agg_tags.contains(tag)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    for (final String tag: right.aggregatedTags()) {
      if (!agg_tags.contains(tag) && !left.tags().containsKey(tag)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    disj_tags.addAll(left.disjointTags());
    disj_tags.addAll(right.disjointTags());
    
    for (final Entry<String, String> entry : left.tags().entrySet()) {
      if (disj_tags.contains(entry.getKey())) {
        continue;
      }
      if (agg_tags.contains(entry.getKey())) {
        continue;
      }
      final String tagv = right.tags().get(entry.getKey());
      if (tagv == null) {
        agg_tags.add(entry.getKey());
      } else if (entry.getValue().equals(tagv)) {
        builder.addTags(entry.getKey(), tagv);
      } else {
        // promote to agg tags
        agg_tags.add(entry.getKey());
      }
    }
    
    // right side now
    for (final Entry<String, String> entry : right.tags().entrySet()) {
      if (disj_tags.contains(entry.getKey())) {
        continue;
      }
      if (agg_tags.contains(entry.getKey())) {
        continue;
      }
      final String tagv = left.tags().get(entry.getKey());
      if (tagv == null) {
        disj_tags.add(entry.getKey());
      } else if (entry.getValue().equals(tagv)) {
        // skip, already added.
      } else {
        // promote to agg tags
        disj_tags.add(entry.getKey());
      }
    }
    
    for (final String tag : agg_tags) {
      builder.addAggregatedTag(tag);
    }
    for (final String tag : disj_tags) {
      builder.addDisjointTag(tag);
    }
    for (final String tsuid : left.uniqueIds()) {
      builder.addUniqueId(tsuid);
    }
    for (final String tsuid : right.uniqueIds()) {
      builder.addUniqueId(tsuid);
    }
    return builder.build();
  }
  
  /**
   * Joins the byte IDs.
   * @param left Left ID.
   * @param right Right ID.
   * @param as A non-null and non-empty name for the metric.
   * @return The joined ID.
   */
  @VisibleForTesting
  TimeSeriesId joinIds(final TimeSeriesByteId left, 
                       final TimeSeriesByteId right, 
                       final String as,
                       final JoinType join_type) {
    final BaseTimeSeriesByteId.Builder builder = BaseTimeSeriesByteId
        .newBuilder(left.dataStore())
        .setNamespace(left.namespace())
        .setAlias(as.getBytes(Const.UTF8_CHARSET))
        .setMetric(as.getBytes(Const.UTF8_CHARSET))
        .setSkipMetric(true);
    
    if (join_type == JoinType.CROSS && 
        (left.tags() == null || left.tags().isEmpty() ||
         right.tags() == null || right.tags().isEmpty())) {
      // we have a cross with a flattened series on one side.
      if (left.tags() != null && !left.tags().isEmpty()) {
        // TODO - May not be a ByteMap and are we safe with ref?
        builder.setTags((ByteMap<byte[]>) left.tags());
        if (left.aggregatedTags() != null && !left.aggregatedTags().isEmpty()) {
          builder.setAggregatedTags(Lists.newArrayList(left.aggregatedTags()));
        }
        if (left.disjointTags() != null && !left.disjointTags().isEmpty()) {
          builder.setDisjointTags(Lists.newArrayList(left.disjointTags()));
        }
        return builder.build();
      }
      // TODO - May not be a ByteMap and are we safe with ref?
      builder.setTags((ByteMap<byte[]>) right.tags());
      if (right.aggregatedTags() != null && !right.aggregatedTags().isEmpty()) {
        builder.setAggregatedTags(Lists.newArrayList(right.aggregatedTags()));
      }
      if (right.disjointTags() != null && !right.disjointTags().isEmpty()) {
        builder.setDisjointTags(Lists.newArrayList(right.disjointTags()));
      }
      return builder.build();
    }
    
    final ByteSet agg_tags = new ByteSet();
    final ByteSet disj_tags = new ByteSet();
    
    agg_tags.addAll(left.aggregatedTags());
    final Iterator<byte[]> it = agg_tags.iterator();
    while (it.hasNext()) {
      final byte[] tag = it.next();
      if (!contains(tag, right.aggregatedTags())) {
        it.remove();
        disj_tags.add(tag);
      }
    }
    for (final byte[] tag : right.disjointTags()) {
      if (contains(tag, agg_tags)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    for (final byte[] tag: right.aggregatedTags()) {
      if (!contains(tag, agg_tags) && !left.tags().containsKey(tag)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    disj_tags.addAll(left.disjointTags());
    disj_tags.addAll(right.disjointTags());
    
    for (final Entry<byte[], byte[]> entry : left.tags().entrySet()) {
      if (contains(entry.getKey(), disj_tags)) {
        continue;
      }
      if (contains(entry.getKey(), agg_tags)) {
        continue;
      }
      final byte[] tagv = right.tags().get(entry.getKey());
      if (tagv == null) {
        agg_tags.add(entry.getKey());
      } else if (Bytes.memcmp(entry.getValue(), tagv) == 0) {
        builder.addTags(entry.getKey(), tagv);
      } else {
        // promote to agg tags
        agg_tags.add(entry.getKey());
      }
    }
    
    // right side now
    for (final Entry<byte[], byte[]> entry : right.tags().entrySet()) {
      if (contains(entry.getKey(), disj_tags)) {
        continue;
      }
      if (contains(entry.getKey(), agg_tags)) {
        continue;
      }
      final byte[] tagv = left.tags().get(entry.getKey());
      if (tagv == null) {
        disj_tags.add(entry.getKey());
      } else if (Bytes.memcmp(entry.getValue(), tagv) == 0) {
        // skip, already added.
      } else {
        // promote to agg tags
        disj_tags.add(entry.getKey());
      }
    }
    
    for (final byte[] tag : agg_tags) {
      builder.addAggregatedTag(tag);
    }
    for (final byte[] tag : disj_tags) {
      builder.addDisjointTag(tag);
    }
    for (final byte[] tsuid : left.uniqueIds()) {
      builder.addUniqueId(tsuid);
    }
    for (final byte[] tsuid : right.uniqueIds()) {
      builder.addUniqueId(tsuid);
    }
    return builder.build();
  }
  
  /**
   * Computes the has on the ID based on the join config and populates
   * the set if the join is successful. Doesn't perform any null checks.
   * @param key The non-null and non-empty key.
   * @param ts The non-null time series.
   * @param join_set The non-null set to populate.
   */
  @VisibleForTesting
  void hashStringId(final Operand operand,
                    final TimeSeries ts,
                    final KeyedHashedJoinSet join_set) {
    final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    long hash = 0;
    
    // super critically important that we sort the tags.
    final Map<String, String> sorted_tags;
    if (id.tags() == null) {
      sorted_tags = Collections.emptyMap();
    } else if (id.tags() instanceof NavigableMap) {
      sorted_tags = id.tags();
    } else if (!id.tags().isEmpty()) {
      sorted_tags = new TreeMap<String, String>(id.tags());
    } else {
      sorted_tags = Collections.emptyMap();
    }
    
    switch (config.type) {
    case NATURAL:
    case NATURAL_OUTER:
    case CROSS:
      // copy all the tag values for natural and cross IF no tags are
      // present.
      if (config.joins.isEmpty() || 
          config.type == JoinType.NATURAL || 
          config.type == JoinType.NATURAL_OUTER) {
        int matched_tags = 0;
        if (sorted_tags != null) {
          for (final Entry<String, String> entry : sorted_tags.entrySet()) {
            if (config.joins != null && config.joins.containsKey(entry.getKey())) {
              matched_tags++;
            }
            if (hash == 0) {
              hash = XXHash.hash(entry.getValue());
            } else {
              hash = XXHash.updateHash(hash, entry.getValue());
            }
          }
        }
        
        if (!config.joins.isEmpty() && 
            matched_tags < config.joins.size()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Ejecting a series that didn't match");
          }
          return;
        }
        
        // break either way. If the series had no tags then we just 
        // hash on the empty string.
        break;
      }
      // NOTE: We're letting the CROSS join fall through here so as to
      // filter on the tags.
    default:
      // handles the other joins where we have to pull from the join config
      if (config.joins != null) {
        //boolean is_left = join_set.left_key.equals(key);
        boolean matched = true;
        for (final Entry<String, String> pair : config.joins.entrySet()) {
          String value = id.tags().get(pair.getKey());
          if (Strings.isNullOrEmpty(value)) {
            value = id.tags().get(pair.getValue());
            if (Strings.isNullOrEmpty(value)) {
              // TODO - log the ejection
              matched = false;
              break;
            }
          }
          if (hash == 0) {
            hash = XXHash.hash(value);
          } else {
            hash = XXHash.updateHash(hash, value);
          }
        }
        if (!matched) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Ejecting a series that didn't match");
          }
          return;
        }
        if (config.getExplicitTags() && 
            id.tags().size() != config.getJoins().size()) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Ejecting a series that didn't match");
          }
          return;
        }
      }
    }
    
    join_set.add(operand, hash, ts);
  }
  
  /**
   * Computes the has on the ID based on the join config and populates
   * the set if the join is successful.
   * @param key The non-null and non-empty key.
   * @param ts The non-null time series.
   * @param join_set The non-null set to populate.
   */
  @VisibleForTesting
  void hashByteId(final Operand operand,
                  final TimeSeries ts,
                  final KeyedHashedJoinSet join_set) {
    final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
    long hash = 0;
    
    final ByteMap<byte[]> tags;
    if (id.tags() == null) { // shouldn't happen.
      tags = new ByteMap<byte[]>();
    } else if (id.tags() instanceof ByteMap) {
      tags = (ByteMap<byte[]>) id.tags();
    } else {
      // TODO - may have other sorted byte map implementations to look for.
      tags = new ByteMap<byte[]>();
      for (final Entry<byte[], byte[]> entry : id.tags().entrySet()) {
        tags.put(entry.getKey(), entry.getValue());
      }
    }
    switch (config.type) {
    case NATURAL:
    case NATURAL_OUTER:
    case CROSS:
      // copy all the tag values for natural and cross and count the matching 
      // join config tag keys present.
      if (encoded_joins == null || 
          config.type == JoinType.NATURAL ||
          config.type == JoinType.NATURAL_OUTER) {
        int matched_tags = 0;
        if (id.tags() != null) {
          for (final Entry<byte[], byte[]> entry : tags.entrySet()) {
            if (encoded_joins != null && encoded_joins.containsKey(entry.getKey())) {
              matched_tags++;
            }
            if (hash == 0) {
              hash = XXHash.hash(entry.getValue());
            } else {
              hash = XXHash.updateHash(hash, entry.getValue());
            }
          }
        }
        
        if (!config.joins.isEmpty() && 
            matched_tags < config.joins.size()) {
            // TODO - log the ejection
          return;
        }
        
        // break either way. If the series had no tags then we just 
        // hash on the empty string.
        break;
      }
      
      // NOTE: We're letting the CROSS join fall through here so as to
      // filter on the tags.
    default:
      if (config.joins != null) {
        boolean matched = true;
        for (final Entry<byte[], byte[]> pair : encoded_joins) {
          byte[] value = tags.get(pair.getKey());
          if (value == null || value.length < 1) {
            value = tags.get(pair.getValue());
            if (value == null || value.length < 1) {
              // TODO - log the ejection
              matched = false;
              break;
            }
          }
          if (hash == 0) {
            hash = XXHash.hash(value);
          } else {
            hash = XXHash.updateHash(hash, value);
          }
        }
        if (!matched) {
          // TODO - log the ejection
          return;
        }
        if (config.getExplicitTags() && 
            tags.size() != config.getJoins().size()) {
          // TODO - log the ejection
          return;
        }
      }
    }
    
    join_set.add(operand, hash, ts);
  }
  
  static boolean contains(final byte[] tag, final Collection<byte[]> tags) {
    for (final byte[] wanted : tags) {
      if (Bytes.memcmp(wanted, tag) == 0) {
        return true;
      }
    }
    return false;
  }
}