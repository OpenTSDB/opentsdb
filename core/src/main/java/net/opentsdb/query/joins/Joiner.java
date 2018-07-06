// This file is part of OpenTSDB.
// Copyright (C) 2017-2018  The OpenTSDB Authors.
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import net.openhft.hashing.LongHashFunction;
import net.opentsdb.common.Const;
import net.opentsdb.data.BaseTimeSeriesByteId;
import net.opentsdb.data.BaseTimeSeriesStringId;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesByteId;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSeriesStringId;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.joins.JoinConfig.JoinType;
import net.opentsdb.utils.ByteSet;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.Pair;
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
   * performed by hashing on the IDs and the join tags.
   * @param results A non-null and non-empty list of results.
   * @param left_key A non-null and non-empty left join key.
   * @param right_key A non-null and non-empty right join key.
   * @param use_alias Whether or not to use the alias when fetching the key.
   * @return An iterator with pairs of left and right joined time series
   * if the join was successful. The iterator may be empty if all series
   * were rejected.
   * @throws IllegalArgumentException if any args were null or invalid.
   */
  public Iterable<Pair<TimeSeries, TimeSeries>> join(
      final List<QueryResult> results, 
      final byte[] left_key, 
      final byte[] right_key,
      final boolean use_alias) {
    if (results == null || results.isEmpty()) {
      throw new IllegalArgumentException("Results list can't be null "
          + "or empty.");
    }
    if (left_key == null || left_key.length < 1) {
      throw new IllegalArgumentException("Left key cannot be null.");
    }
    if (right_key == null || right_key.length < 1) {
      throw new IllegalArgumentException("Right key cannot be null.");
    }
    
    if (results.get(0).idType() == Const.TS_BYTE_ID && 
        encoded_joins == null) {
      throw new IllegalStateException("Received a result with encoded "
          + "IDs but the local encoded tags map was null.");
    }
    
    final KeyedHashedJoinSet join_set = 
        new KeyedHashedJoinSet(config.type, left_key, right_key);
    
    // calculate the hash for every series and let the hasher kick out
    // inapplicable series.
    for (final QueryResult result : results) {
      for (final TimeSeries ts : result.timeSeries()) {
        
        if (ts.id().type() == Const.TS_BYTE_ID) {
          final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
          final byte[] key;
          if (use_alias) {
            if (id.namespace() == null || id.namespace().length < 1) {
              key = id.alias();
            } else {
              key = com.google.common.primitives.Bytes.concat(
                  id.namespace(), id.alias());
            }
          } else {
            if (id.namespace() == null || id.namespace().length < 1) {
              key = id.metric();
            } else {
              key = com.google.common.primitives.Bytes.concat(
                  id.namespace(), id.metric());
            }
          }
          if (Bytes.memcmp(key, left_key) != 0 && 
              Bytes.memcmp(key, right_key) != 0) {
            // TODO - log ejection
            continue;
          }
          
          hashByteId(key, ts, join_set);
        } else {
          final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
          final String key;
          if (use_alias) {
            key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.alias() :
                  id.namespace() + id.alias();
          } else {
            key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.metric() :
                  id.namespace() + id.metric();
          }
          final byte[] key_in_bytes = key.getBytes(Const.UTF8_CHARSET);
          if (Bytes.memcmp(key_in_bytes, left_key) != 0 && 
              Bytes.memcmp(key_in_bytes, right_key) != 0) {
            // TODO - log ejection
            continue;
          }
          hashStringId(key_in_bytes, ts, join_set);
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
  public Iterable<Pair<TimeSeries, TimeSeries>> join(
      final List<QueryResult> results, 
      final byte[] filter,
      final boolean left,
      final boolean use_alias) {
    if (results == null || results.isEmpty()) {
      throw new IllegalArgumentException("Results list can't be null "
          + "or empty.");
    }
    if (filter == null || filter.length < 1) {
      throw new IllegalArgumentException("Filter cannot be null.");
    }
    
    if (results.get(0).idType() == Const.TS_BYTE_ID && 
        encoded_joins == null) {
      throw new IllegalStateException("Received a result with encoded "
          + "IDs but the local encoded tags map was null.");
    }
    
    final List<Pair<TimeSeries, TimeSeries>> join_set = Lists.newArrayList();
    
    // calculate the hashes for every time series and joins.
    for (final QueryResult result : results) {
      for (final TimeSeries ts : result.timeSeries()) {
        
        if (ts.id().type() == Const.TS_BYTE_ID) {
          final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
          final byte[] key;
          if (use_alias) {
            if (id.namespace() == null || id.namespace().length < 1) {
              key = id.alias();
            } else {
              key = com.google.common.primitives.Bytes.concat(
                  id.namespace(), id.alias());
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
            
            if (satisfied_joins) {
              join_set.add(new Pair<TimeSeries, TimeSeries>(
                  left ? ts : null, left ? null : ts));
            }
          }
          // TODO - log ejection
        } else {
          final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
          final String key;
          if (use_alias) {
            key = Strings.isNullOrEmpty(id.namespace()) ? 
                id.alias() :
                  id.namespace() + id.alias();
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
              join_set.add(new Pair<TimeSeries, TimeSeries>(
                  left ? ts : null, left ? null : ts));
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
   * When both IDs share the same metric, that metric is returned. If
   * the metrics are different, the alias is substituted unless the
   * join type is a LEFT or RIGHT, in which case the left or right is
   * chosen.
   * @param left The left hand ID. May be null.
   * @param right The right hand ID. May be null.
   * @param alias A non-null and non-empty alias.
   * @return The joined ID.
   * @throws IllegalArgumentException if the alias was null or empty or
   * all IDs were null.
   */
  public TimeSeriesId joinIds(final TimeSeries left, 
                              final TimeSeries right, 
                              final String alias) {
    if (Strings.isNullOrEmpty(alias)) {
      throw new IllegalArgumentException("Alias cannot be null or empty.");
    }
    if (left == null && right == null) {
      throw new IllegalArgumentException("At least one ID must not be null.");
    }
    
    if (left != null && right != null) {
      // NOTE: We assume both are of the same type. Need to verify that
      // upstream.
      if (left.id().type() == Const.TS_BYTE_ID) {
        return joinIds((TimeSeriesByteId) left.id(), 
            (TimeSeriesByteId) right.id(), alias);        
      } else {
        return joinIds((TimeSeriesStringId) left.id(), 
            (TimeSeriesStringId) right.id(), alias);
      }
    } else if (left == null) {
      if (right.id().type() == Const.TS_BYTE_ID) {
        return new ByteIdOverride((TimeSeriesByteId) right.id(), alias);
      } else {
        return new StringIdOverride((TimeSeriesStringId) right.id(), alias);
      }
    } else {
      if (left.id().type() == Const.TS_BYTE_ID) {
        return new ByteIdOverride((TimeSeriesByteId) left.id(), alias);
      } else {
        return new StringIdOverride((TimeSeriesStringId) left.id(), alias);
      }
    }
  }
  
  /**
   * Joins the byte IDs.
   * @param left Left ID.
   * @param right Right ID.
   * @param alias A non-null and non-empty alias.
   * @return The joined ID.
   */
  @VisibleForTesting
  TimeSeriesId joinIds(final TimeSeriesStringId left, 
                       final TimeSeriesStringId right, 
                       final String alias) {
    final BaseTimeSeriesStringId.Builder builder = BaseTimeSeriesStringId
        .newBuilder()
        .setAlias(alias);
    
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
    
    switch (config.type) {
    case INNER:
    case OUTER:
    case OUTER_DISJOINT:
    case NATURAL:
    case CROSS:
    case LEFT:
    case LEFT_DISJOINT:
      builder.setNamespace(left.namespace());
      if (config.type == JoinType.LEFT || 
          config.type == JoinType.LEFT_DISJOINT) {
        builder.setMetric(left.metric());
      } else {
        if (!left.metric().equals(right.metric())) {
          builder.setMetric(alias);
        } else {
          builder.setMetric(left.metric());
        }
      }
      break;
    case RIGHT:
    case RIGHT_DISJOINT:
      builder.setNamespace(right.namespace())
             .setMetric(right.metric());
      break;
      default:
        throw new UnsupportedOperationException("We do not support: " 
            + config.type + " yet");
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
   * @param alias A non-null and non-empty alias.
   * @return The joined ID.
   */
  @VisibleForTesting
  TimeSeriesId joinIds(final TimeSeriesByteId left, 
                       final TimeSeriesByteId right, 
                       final String alias) {
    final BaseTimeSeriesByteId.Builder builder = BaseTimeSeriesByteId
        .newBuilder(left.dataStore())
        .setAlias(alias.getBytes(Const.UTF8_CHARSET));
    
    final ByteSet agg_tags = new ByteSet();
    final ByteSet disj_tags = new ByteSet();
    agg_tags.addAll(left.aggregatedTags());
    final Iterator<byte[]> it = agg_tags.iterator();
    while (it.hasNext()) {
      final byte[] tag = it.next();
      if (!right.aggregatedTags().contains(tag)) {
        it.remove();
        disj_tags.add(tag);
      }
    }
    for (final byte[] tag : right.disjointTags()) {
      if (agg_tags.contains(tag)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    for (final byte[] tag: right.aggregatedTags()) {
      if (!agg_tags.contains(tag) && !left.tags().containsKey(tag)) {
        agg_tags.remove(tag);
        disj_tags.add(tag);
      }
    }
    disj_tags.addAll(left.disjointTags());
    disj_tags.addAll(right.disjointTags());
    
    for (final Entry<byte[], byte[]> entry : left.tags().entrySet()) {
      if (disj_tags.contains(entry.getKey())) {
        continue;
      }
      if (agg_tags.contains(entry.getKey())) {
        continue;
      }
      final byte[] tagv = right.tags().get(entry.getKey());
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
    for (final Entry<byte[], byte[]> entry : right.tags().entrySet()) {
      if (disj_tags.contains(entry.getKey())) {
        continue;
      }
      if (agg_tags.contains(entry.getKey())) {
        continue;
      }
      final byte[] tagv = left.tags().get(entry.getKey());
      if (tagv == null) {
        disj_tags.add(entry.getKey());
      } else if (entry.getValue().equals(tagv)) {
        // skip, already added.
      } else {
        // promote to agg tags
        disj_tags.add(entry.getKey());
      }
    }
    
    switch (config.type) {
    case INNER:
    case OUTER:
    case OUTER_DISJOINT:
    case NATURAL:
    case CROSS:
    case LEFT:
    case LEFT_DISJOINT:
      builder.setNamespace(left.namespace());
      if (config.type == JoinType.LEFT || 
          config.type == JoinType.LEFT_DISJOINT) {
        builder.setMetric(left.metric());
      } else {
        if (!left.metric().equals(right.metric())) {
          builder.setMetric(alias.getBytes(Const.UTF8_CHARSET));
        } else {
          builder.setMetric(left.metric());
        }
      }
      break;
    case RIGHT:
    case RIGHT_DISJOINT:
      builder.setNamespace(right.namespace())
             .setMetric(right.metric());
      break;
      default:
        throw new UnsupportedOperationException("We do not support: " 
            + config.type + " yet");
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
  void hashStringId(final byte[] key,
                    final TimeSeries ts,
                    final KeyedHashedJoinSet join_set) {
    final TimeSeriesStringId id = (TimeSeriesStringId) ts.id();
    final StringBuilder buf = new StringBuilder();
    
    // super critically important that we sort the tags.
    final Map<String, String> sorted_tags = id.tags() != null && 
        !id.tags().isEmpty() ? 
            new TreeMap<String, String>(id.tags()) : null;
    
    switch (config.type) {
    case NATURAL:
    case CROSS:
      // copy all the tag values for natural and cross IF no tags are
      // present.
      if (config.joins.isEmpty() || config.type == JoinType.NATURAL) {
        int matched_tags = 0;
        if (sorted_tags != null) {
          for (final Entry<String, String> entry : sorted_tags.entrySet()) {
            if (config.joins != null && config.joins.containsKey(entry.getKey())) {
              matched_tags++;
            }
            buf.append(entry.getValue());
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
      // handles the other joins where we have to pull from the join config
      if (config.joins != null) {
        boolean is_left = join_set.left_key.equals(key);
        
        boolean matched = true;
        for (final Entry<String, String> pair : config.joins.entrySet()) {
          final String value = id.tags().get(is_left ? pair.getKey() : pair.getValue());
          if (Strings.isNullOrEmpty(value)) {
            // TODO - log the ejection
            matched = false;
            break;
          }
          buf.append(value);
        }
        if (!matched) {
          // TODO - log the ejection
          return;
        }
        if (config.getExplicitTags() && 
            id.tags().size() != config.getJoins().size()) {
          // TODO - log the ejection
          return;
        }
      }
    }
    
    join_set.add(key, LongHashFunction.xx_r39().hashChars(buf.toString()), ts);
  }
  
  /**
   * Computes the has on the ID based on the join config and populates
   * the set if the join is successful.
   * @param key The non-null and non-empty key.
   * @param ts The non-null time series.
   * @param join_set The non-null set to populate.
   */
  @VisibleForTesting
  void hashByteId(final byte[] key,
                  final TimeSeries ts,
                  final KeyedHashedJoinSet join_set) {
    
    final TimeSeriesByteId id = (TimeSeriesByteId) ts.id();
    final ByteArrayOutputStream buf = new ByteArrayOutputStream();
    
    try {
      switch (config.type) {
      case NATURAL:
      case CROSS:
        // copy all the tag values for natural and cross IF no tags are
        // present.
        if (encoded_joins == null || config.type == JoinType.NATURAL) {
          int matched_tags = 0;
          if (id.tags() != null) {
            for (final Entry<byte[], byte[]> entry : id.tags().entrySet()) {
              if (encoded_joins != null && encoded_joins.containsKey(entry.getKey())) {
                matched_tags++;
              }
              buf.write(entry.getValue());
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
          boolean is_left = join_set.left_key.equals(key);
          
          boolean matched = true;
          for (final Entry<byte[], byte[]> pair : encoded_joins) {
            byte[] value = id.tags().get(is_left ? pair.getKey() : pair.getValue());
            if (value == null || value.length < 1) {
              // TODO - log the ejection
              matched = false;
              break;
            }
            buf.write(value);
          }
          if (!matched) {
            // TODO - log the ejection
            return;
          }
          if (config.getExplicitTags() && 
              id.tags().size() != config.getJoins().size()) {
            // TODO - log the ejection
            return;
          }
        }
        
      }
      
      join_set.add(key, LongHashFunction.xx_r39().hashBytes(buf.toByteArray()), ts);
    } catch (IOException e) {
      throw new QueryExecutionException("Unexpected exception joining results", 0, e);
    }
  }
}