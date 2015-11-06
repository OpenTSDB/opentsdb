// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
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
package net.opentsdb.query.expression;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.opentsdb.core.FillPolicy;
import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.ByteSet;

import org.hbase.async.HBaseClient;
import org.hbase.async.Bytes.ByteMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * An iterator that computes the union of all series in the result sets. This 
 * means we match every series with it's corresponding series in the other sets.
 * If one or more set lacks the matching series, then a {@code null} is stored
 * and when the caller iterates over the results, the need to detect the null
 * and substitute a fill value.
 * @since 2.3
 */
public class UnionIterator implements ITimeSyncedIterator, VariableIterator {
  private static final Logger LOG = LoggerFactory.getLogger(UnionIterator.class);
  
  /** The queries compiled and fetched from storage */
  private final Map<String, ITimeSyncedIterator> queries;
  
  /** A list of the current values for each series post intersection */
  private final Map<String, ExpressionDataPoint[]> current_values;

  /** A map of the sub query index to their names for intersection computation */
  private final String[] index_to_names;
  
  /** Whether or not to intersect on the query tagks instead of the result set
   * tagks */
  private final boolean union_on_query_tagks;
  
  /** Whether or not to include the aggregated tags in the result set */
  private final boolean include_agg_tags;
  
  /** The start/current timestamp for the iterator in ms */
  private long timestamp;
  
  /** Post intersection number of time series */
  private int series_size;
  
  /** The ID of this iterator */
  private final String id;
  
  /** The index of this iterator in a list of iterators */
  private int index;
  
  /** The fill policy to use when a series is missing from one of the sets.
   * Default is zero. */
  private NumericFillPolicy fill_policy;
  
  /** A data point used for filling missing time series */
  private ExpressionDataPoint fill_dp;
  
  /**
   * Default ctor
   * @param id The variable ID for this iterator
   * @param results Upstream iterators
   * @param union_on_query_tagks Whether or not to flatten and join on only
   * the tags from the query or those returned in the results.
   * @param include_agg_tags Whether or not to include the flattened aggregated
   * tag keys in the join.
   */
  public UnionIterator(final String id, final Map<String, ITimeSyncedIterator> results,
      final boolean union_on_query_tagks, final boolean include_agg_tags) {
    this.id = id;
    this.union_on_query_tagks = union_on_query_tagks;
    this.include_agg_tags = include_agg_tags;
    timestamp = Long.MAX_VALUE;
    queries = new HashMap<String, ITimeSyncedIterator>(results.size());
    current_values = new HashMap<String, ExpressionDataPoint[]>(results.size());
    index_to_names = new String[results.size()];
    fill_policy = new NumericFillPolicy(FillPolicy.ZERO);
    fill_dp = new ExpressionDataPoint();
    
    int i = 0;
    for (final Map.Entry<String, ITimeSyncedIterator> entry : results.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding iterator " + entry.getValue());
      }
      queries.put(entry.getKey(), entry.getValue());
      entry.getValue().setIndex(i);
      index_to_names[i] = entry.getKey();
      ++i;
    }

    computeUnion();
    
    // calculate the starting timestamp from the various iterators
    for (final ITimeSyncedIterator it : queries.values()) {
      final long ts = it.nextTimestamp();
      if (ts < timestamp) {
        timestamp = ts;
      }
    }
    
    if (LOG.isDebugEnabled()) {
      LOG.debug("Computed union: " + this);
    }
  }
  
  /**
   * Private copy constructor that copies references and sets up new collections
   * without copying results.
   * @param iterator The iterator to copy from.
   */
  private UnionIterator(final UnionIterator iterator) {
    id = iterator.id;
    union_on_query_tagks = iterator.union_on_query_tagks;
    include_agg_tags = iterator.include_agg_tags;
    timestamp = Long.MAX_VALUE;
    queries = new HashMap<String, ITimeSyncedIterator>(iterator.queries.size());
    current_values = new HashMap<String, ExpressionDataPoint[]>(queries.size());
    index_to_names = new String[queries.size()];
    fill_policy = iterator.fill_policy;
    
    int i = 0;
    for (final Map.Entry<String, ITimeSyncedIterator> entry : iterator.queries.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding iterator " + entry.getValue());
      }
      queries.put(entry.getKey(), entry.getValue());
      entry.getValue().setIndex(i);
      index_to_names[i] = entry.getKey();
      ++i;
    }

    computeUnion();
    
    // calculate the starting timestamp from the various iterators
    for (final ITimeSyncedIterator it : queries.values()) {
      final long ts = it.nextTimestamp();
      if (ts < timestamp) {
        timestamp = ts;
      }
    }
  }
  
  /**
   * Computes the union of all sets, matching on tags and optionally the 
   * aggregated tags across each variable.
   */
  private void computeUnion() {
    // key = flattened tags, array of queries.size()
    final ByteMap<ExpressionDataPoint[]> ordered_union = 
        new ByteMap<ExpressionDataPoint[]>(); 

    final Iterator<ITimeSyncedIterator> it = queries.values().iterator();
    while (it.hasNext()) {
      final ITimeSyncedIterator sub = it.next();
      final ExpressionDataPoint[] dps = sub.values();
      final ByteMap<Integer> local_tags = new ByteMap<Integer>();
      
      for (int i = 0; i < sub.size(); i++) {
        final byte[] key = flattenTags(union_on_query_tagks, include_agg_tags, 
            dps[i], sub);
        local_tags.put(key, i);
        ExpressionDataPoint[] udps = ordered_union.get(key);
        if (udps == null) {
          udps = new ExpressionDataPoint[queries.size()];
          ordered_union.put(key, udps);
        }
        udps[sub.getIndex()] = dps[i];
      }
    }
    
    if (ordered_union.size() < 1) {
      // if no data, just stop here
      return;
    }
    
    setCurrentAndMeta(ordered_union);
  }
  
  /**
   * Takes the resulting union and builds the {@link #current_values}
   * and {@link #meta} maps.
   * @param ordered_union The union to build from.
   */
  private void setCurrentAndMeta(final ByteMap<ExpressionDataPoint[]> 
      ordered_union) {
    for (final String id : queries.keySet()) {
      current_values.put(id, new ExpressionDataPoint[ordered_union.size()]);
    }
    
    int i = 0;
    for (final ExpressionDataPoint[] idps : ordered_union.values()) {
      for (int x = 0; x < idps.length; x++) {
        final ExpressionDataPoint[] current_dps = 
            current_values.get(index_to_names[x]);
        current_dps[i] = idps[x];
      }
      ++i;
    }
    
    // set fills on nulls
    for (final ExpressionDataPoint[] idps : current_values.values()) {
      for (i = 0; i < idps.length; i++) {
        if (idps[i] == null) {
          idps[i] = fill_dp;
        }
      }
    }
    series_size = ordered_union.size();
  }
  
  /**
   * Creates a key based on the concatenation of the tag pairs then the agg
   * tag keys.
   * @param use_query_tags Whether or not to include tags returned with the
   * results or just use those group by'd in the query
   * @param include_agg_tags Whether or not to include the aggregated tags in
   * the identifier
   * @param dp The current expression data point
   * @param sub The sub query iterator
   * @return A byte array with the flattened tag keys and values. Note that
   * if the tags set is empty, this may return an empty array (but not a null
   * array)
   */
  static byte[] flattenTags(final boolean use_query_tags, 
      final boolean include_agg_tags, final ExpressionDataPoint dp, 
      final ITimeSyncedIterator sub) {
    if (dp.tags().isEmpty()) {
      return HBaseClient.EMPTY_ARRAY;
    }
    final int tagk_width = TSDB.tagk_width();
    final int tagv_width = TSDB.tagv_width();
    
    final ByteSet query_tagks;
    // NOTE: We MAY need the agg tags but I'm not sure yet
    final int tag_size;
    if (use_query_tags) {
      int i = 0;
      if (sub.getQueryTagKs() != null && !sub.getQueryTagKs().isEmpty()) {
        query_tagks = sub.getQueryTagKs();
        for (final Map.Entry<byte[], byte[]> pair : dp.tags().entrySet()) {
          if (query_tagks.contains(pair.getKey())) {
            i++;
          }
        }
      } else {
        query_tagks = new ByteSet();
      }
      tag_size = i;
    } else {
      query_tagks = new ByteSet();
      tag_size = dp.tags().size();
    }
    
    final int length = (tag_size * (tagk_width + tagv_width))
        + (include_agg_tags ? (dp.aggregatedTags().size() * tagk_width) : 0);
    final byte[] key = new byte[length];
    int idx = 0;
    for (final Entry<byte[], byte[]> pair : dp.tags().entrySet()) {
      if (use_query_tags && !query_tagks.contains(pair.getKey())) {
        continue;
      }
      System.arraycopy(pair.getKey(), 0, key, idx, tagk_width);
      idx += tagk_width;
      System.arraycopy(pair.getValue(), 0, key, idx, tagv_width);
      idx += tagv_width;
    }
    if (include_agg_tags) {
      for (final byte[] tagk : dp.aggregatedTags()) {
        System.arraycopy(tagk, 0, key, idx, tagk_width);
        idx += tagk_width;
      }
    }
    return key;
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("UnionIterator(id=")
       .append(id)
       .append(", useQueryTags=")
       .append(union_on_query_tagks)
       .append(", includeAggTags=")
       .append(include_agg_tags)
       .append(", index=")
       .append(index)
       .append(", queries=")
       .append(queries);
    return buf.toString();
  }

  // Iterator implementations
  
  @Override
  public boolean hasNext() {
    for (final ITimeSyncedIterator sub : queries.values()) {
      if (sub.hasNext()) {
        return true;
      }
    }
    return false;
  }

  @Override
  public ExpressionDataPoint[] next(long timestamp) {
    throw new NotImplementedException();
  }

  @Override
  public long nextTimestamp() {
    long ts = Long.MAX_VALUE;
    for (final ITimeSyncedIterator sub : queries.values()) {
      if (sub != null) {
        final long t = sub.nextTimestamp();
        if (t < ts) {
          ts = t;
        }
      }
    }
    return ts;
  }

  @Override
  public int size() {
    throw new NotImplementedException();
  }

  @Override
  public ExpressionDataPoint[] values() {
    throw new NotImplementedException();
  }

  @Override
  public void nullIterator(int index) {
    throw new NotImplementedException();
  }

  @Override
  public int getIndex() {
    return index;
  }

  @Override
  public void setIndex(int index) {
    this.index = index;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public ByteSet getQueryTagKs() {
    throw new NotImplementedException();
  }

  @Override
  public void setFillPolicy(NumericFillPolicy policy) {
    this.fill_policy = policy;
  }

  @Override
  public NumericFillPolicy getFillPolicy() {
    return fill_policy;
  }

  @Override
  public ITimeSyncedIterator getCopy() {
    return new UnionIterator(this);
  }

  @Override
  public void next() {
    if (!hasNext()) {
      throw new IllegalDataException("No more data");
    }
    for (final ITimeSyncedIterator sub : queries.values()) {
      sub.next(timestamp);
    }
    // reset the fill data point
    fill_dp.reset(timestamp, fill_policy.getValue());
    timestamp = nextTimestamp();
  }

  @Override
  public Map<String, ExpressionDataPoint[]> getResults() {
    return current_values;
  }

  @Override
  public int getSeriesSize() {
    return series_size;
  }
}
