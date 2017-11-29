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

import net.opentsdb.core.IllegalDataException;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.ByteSet;

import org.hbase.async.Bytes;
import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

/**
 * This class handles taking a set of queries and their results and iterates 
 * over each series in each set with time alignment after computing the 
 * intersection of all sets.
 * <p>
 * The iterator performs the following:
 * - calculates the intersection of all queries based on the tags or query tags
 *   and optionally the aggregated tags.
 * - any series that are not members of ever set are kicked out (and logged).
 * - series are aligned across queries so that expressions can operate over them.
 * - series are also time aligned and maintain alignment during iteration.
 * <p>
 * The {@link #current_values} map will map the expression "variables" to the
 * proper iterator for each serie's array. E.g.
 *   &lt;"A", [1, 2, 3, 4]&gt;
 *   &lt;"B", [1, 2, 3, 4]&gt;
 * <p>
 * So to use it's you simply fetch the result map, call {@link #hasNext()} and
 * {@link #next()} to iterate and in a for loop, iterate {@link #getSeriesSize()}
 * times to get all of the current values.
 * For efficiency, call {@link #getResults()} once before iterating, then on 
 * each call to {@link #next()} you can just iterate over the same result map 
 * again as the values will be updated.
 * @since 2.3
 */
public class IntersectionIterator implements ITimeSyncedIterator, VariableIterator {
  private static final Logger LOG = LoggerFactory.getLogger(IntersectionIterator.class);
  
  /** The queries compiled and fetched from storage */
  private final Map<String, ITimeSyncedIterator> queries;
  
  /** A list of the current values for each series post intersection */
  private final Map<String, ExpressionDataPoint[]> current_values;

  /** A map of the sub query index to their names for intersection computation */
  private final String[] index_to_names;
  
  /** Whether or not to intersect on the query tagks instead of the result set
   * tagks */
  private final boolean intersect_on_query_tagks;
  
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
  
  /**
   * Ctor to create the expression lock-step iterator from a set of query results.
   * If the results map is empty, then the ctor will complete but the results map
   * will be empty and calls to {@link #hasNext()} will always return false.
   * @param results The query results to store
   * @param intersect_on_query_tagks Whether or not to include only the query 
   * specified tags during intersection
   * @param include_agg_tags Whether or not to include aggregated tags during
   * intersection
   * @throws IllegalDataException if, after computing the intersection, no results
   * would be left.
   */
  public IntersectionIterator(final String id, final Map<String, ITimeSyncedIterator> results, 
      final boolean intersect_on_query_tagks, final boolean include_agg_tags) {
    this.id = id;
    this.intersect_on_query_tagks = intersect_on_query_tagks;
    this.include_agg_tags = include_agg_tags;
    timestamp = Long.MAX_VALUE;
    queries = new HashMap<String, ITimeSyncedIterator>(results.size());
    current_values = new HashMap<String, ExpressionDataPoint[]>(results.size());
    index_to_names = new String[results.size()];
    
    int max_series = 0;
    int i = 0;
    for (final Map.Entry<String, ITimeSyncedIterator> entry : results.entrySet()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding iterator " + entry.getValue());
      }
      queries.put(entry.getKey(), entry.getValue());
      entry.getValue().setIndex(i);
      index_to_names[i] = entry.getKey();
      if (entry.getValue().values().length > max_series) {
        max_series = entry.getValue().values().length;
      }
      ++i;
    }
    
    if (max_series < 1) {
      // we don't want to throw an exception here, just set it up so that the
      // call to {@link #hasNext()} will be false.
      LOG.debug("No series in the result sets");
      return;
    }
    
    computeIntersection();
    
    // calculate the starting timestamp from the various iterators
    for (final ITimeSyncedIterator it : queries.values()) {
      final long ts = it.nextTimestamp();
      if (ts < timestamp) {
        timestamp = ts;
      }
    }
  }

  /**
   * A sort of copy constructor that populates the iterator from an existing 
   * iterator, copying all child iterators.
   * @param iterator The iterator to copy from.
   */
  private IntersectionIterator(final IntersectionIterator iterator) {
    id = iterator.id;
    intersect_on_query_tagks = iterator.intersect_on_query_tagks;
    include_agg_tags = iterator.include_agg_tags;
    timestamp = Long.MAX_VALUE;
    queries = new HashMap<String, ITimeSyncedIterator>(iterator.queries.size());
    current_values = new HashMap<String, ExpressionDataPoint[]>(queries.size());
    index_to_names = new String[queries.size()];
    
    int max_series = 0;
    int i = 0;
    for (final Entry<String, ITimeSyncedIterator> entry : iterator.queries.entrySet()) {
      queries.put(entry.getKey(), entry.getValue().getCopy());
      entry.getValue().setIndex(i);
      index_to_names[i] = entry.getKey();
      if (entry.getValue().values().length > max_series) {
        max_series = entry.getValue().values().length;
      }
      ++i;
    }
    
    if (max_series < 1) {
      // we don't want to throw an exception here, just set it up so that the
      // call to {@link #hasNext()} will be false.
      LOG.debug("No series in the result sets");
      return;
    }
    
    computeIntersection();
    
    // calculate the starting timestamp from the various iterators
    for (final ITimeSyncedIterator it : queries.values()) {
      final long ts = it.nextTimestamp();
      if (ts < timestamp) {
        timestamp = ts;
      }
    }
  }
  
  @Override
  public String toString() {
    final StringBuilder buf = new StringBuilder();
    buf.append("IntersectionIterator(id=")
       .append(id)
       .append(", useQueryTags=")
       .append(intersect_on_query_tagks)
       .append(", includeAggTags=")
       .append(include_agg_tags)
       .append(", index=")
       .append(index)
       .append(", queries=")
       .append(queries);
    return buf.toString();
  }
  
  @Override
  public boolean hasNext() {
    for (final ITimeSyncedIterator sub : queries.values()) {
      if (sub.hasNext()) {
        return true;
      }
    }
    return false;
  }
  
  /** fetch the next set of time aligned results for all series */
  @Override
  public void next() {
    if (!hasNext()) {
      throw new IllegalDataException("No more data");
    }
    for (final ITimeSyncedIterator sub : queries.values()) {
      sub.next(timestamp);
    }
    timestamp = nextTimestamp();
  }
  
  /** @return a map of values that will change on each iteration */
  @Override 
  public Map<String, ExpressionDataPoint[]> getResults() {
    return current_values;
  }

  /** @return the number of series in each map of the result set */
  @Override
  public int getSeriesSize() {
    return series_size;
  }
  
  /** @return the next timestamp calculated from all series in the set */
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
  
  /**
   * A super ugly messy way to compute the intersection of the various sets of 
   * time series returned from the sub queries. 
   * <p>
   * The process is:
   * - Iterate over each query set
   * - For the first set, flatten each series' tag and (optionally) aggregated tag
   *   set into a single byte array for use as an ID.
   * - Populate a map with the IDs and references to the series iterator for the
   *   first query set.
   * - For each additional set, flatten the tags and if the tag set ID isn't in
   *   the intersection map, kick it out.
   * - For each key in the intersection map, if it doesn't appear in the current
   *   query set, kick it out.
   * - Once all sets are finished, align the resulting series iterators in the 
   *   {@link #current_values} map which is then prepped for expression processing.
   * @throws IllegalDataException if more than one series was supplied and 
   * the resulting intersection failed to produce any series
   */
  private void computeIntersection() {
    final ByteMap<ExpressionDataPoint[]> ordered_intersection = 
        new ByteMap<ExpressionDataPoint[]>(); 
    final Iterator<ITimeSyncedIterator> it = queries.values().iterator();
    
    // assume we have at least on query in our set
    ITimeSyncedIterator sub = it.next();
    Map<String, ByteMap<Integer>> flattened_tags = 
        new HashMap<String, ByteMap<Integer>>(queries.size()); 
    ByteMap<Integer> tags = new ByteMap<Integer>();
    flattened_tags.put(sub.getId(), tags);
    ExpressionDataPoint[] dps = sub.values();
    
    for (int i = 0; i < sub.size(); i++) {
      final byte[] tagks = flattenTags(intersect_on_query_tagks, include_agg_tags,
          dps[i].tags(), dps[i].aggregatedTags(), sub);
      tags.put(tagks, i);

      final ExpressionDataPoint[] idps = new ExpressionDataPoint[queries.size()];
      idps[sub.getIndex()] = dps[i];
      ordered_intersection.put(tagks, idps);
    }
    
    if (!it.hasNext()) {
      setCurrentAndMeta(ordered_intersection);
      return;
    }
    
    while (it.hasNext()) {
      sub = it.next();
      tags = new ByteMap<Integer>();
      flattened_tags.put(sub.getId(), tags);
      dps = sub.values();
      
      // loop through the series in the sub iterator, compute the flattened tag
      // ids, then kick out any that are NOT in the existing intersection map.
      for (int i = 0; i < sub.size(); i++) {
        final byte[] tagks = flattenTags(intersect_on_query_tagks, include_agg_tags, 
            dps[i].tags(), dps[i].aggregatedTags(), sub);
        tags.put(tagks, i);

        final ExpressionDataPoint[] idps = ordered_intersection.get(tagks);
        if (idps == null) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Kicking out " + Bytes.pretty(tagks) + " from " + sub.getId());
          }
          sub.nullIterator(i);
          continue;
        }
        idps[sub.getIndex()] = dps[i];
      }
      
      // gotta go backwards now to complete the intersection by kicking
      // any series that appear in other sets but not HERE
      final Iterator<Entry<byte[], ExpressionDataPoint[]>> reverse_it = 
          ordered_intersection.iterator();
      while (reverse_it.hasNext()) {
        Entry<byte[], ExpressionDataPoint[]> e = reverse_it.next();
        if (!tags.containsKey(e.getKey())) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Kicking out " + Bytes.pretty(e.getKey()) + 
                " from the main list since the query for " + sub.getId() + 
                " didn't have it");
          }
          
          // null the iterators for the other sets
          for (final Map.Entry<String, ByteMap<Integer>> entry : 
              flattened_tags.entrySet()) {
            if (entry.getKey().equals(sub.getId())) {
              continue;
            }
            final Integer index = entry.getValue().get(e.getKey());
            if (index != null) {
              queries.get(entry.getKey()).nullIterator(index);
            }
          }
          
          reverse_it.remove();
        }
      }
    }
    
    // now set our properly condensed and ordered values
    if (ordered_intersection.size() < 1) {
      // TODO - is it best to toss an exception here or return an empty result?
      throw new IllegalDataException("No intersections found: " + this);
    }
    
    setCurrentAndMeta(ordered_intersection);
  }
  
  /**
   * Takes the resulting intersection and builds the {@link #current_values}
   * and {@link #meta} maps.
   * @param ordered_intersection The intersection to build from.
   */
  private void setCurrentAndMeta(final ByteMap<ExpressionDataPoint[]> 
      ordered_intersection) {
    for (final String id : queries.keySet()) {
      current_values.put(id, new ExpressionDataPoint[ordered_intersection.size()]);
    }
    
    int i = 0;
    for (final ExpressionDataPoint[] idps : ordered_intersection.values()) {
      for (int x = 0; x < idps.length; x++) {
        final ExpressionDataPoint[] current_dps = 
            current_values.get(index_to_names[x]);
        current_dps[i] = idps[x];
      }
      ++i;
    }
    series_size = ordered_intersection.size();
  }
  
  /**
   * Flattens the appropriate tags into a single byte array
   * @param use_query_tags Whether or not to include tags returned with the
   * results or just use those group by'd in the query
   * @param include_agg_tags Whether or not to include the aggregated tags in
   * the identifier
   * @param tags The map of tags from the result set
   * @param agg_tags The list of aggregated tags
   * @param sub The sub query iterator
   * @return A byte array with the flattened tag keys and values. Note that
   * if the tags set is empty, this may return an empty array (but not a null
   * array)
   */
  static byte[] flattenTags(final boolean use_query_tags, 
      final boolean include_agg_tags, final ByteMap<byte[]> tags, 
      final ByteSet agg_tags, final ITimeSyncedIterator sub) {
    if (tags.isEmpty()) {
      return HBaseClient.EMPTY_ARRAY;
    }
    final ByteSet query_tagks;
    // NOTE: We MAY need the agg tags but I'm not sure yet
    final int tag_size;
    if (use_query_tags) {
      int i = 0;
      if (sub.getQueryTagKs() != null && !sub.getQueryTagKs().isEmpty()) {
        query_tagks = sub.getQueryTagKs();
        for (final Map.Entry<byte[], byte[]> pair : tags.entrySet()) {
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
      tag_size = tags.size();
    }
    
    int len = (tag_size * (TSDB.tagk_width() + TSDB.tagv_width())) +
      (include_agg_tags ? (agg_tags.size() * TSDB.tagk_width()) : 0);
    final byte[] tagks = new byte[len];
    int i = 0;
    for (final Map.Entry<byte[], byte[]> pair : tags.entrySet()) {
      if (use_query_tags && !query_tagks.contains(pair.getKey())) {
        continue;
      }
      System.arraycopy(pair.getKey(), 0, tagks, i, TSDB.tagk_width());
      i += TSDB.tagk_width();
      System.arraycopy(pair.getValue(), 0, tagks, i, TSDB.tagv_width());
      i += TSDB.tagv_width();
    }
    if (include_agg_tags) {
      for (final byte[] tagk : agg_tags) {
        System.arraycopy(tagk, 0, tagks, i, TSDB.tagk_width());
        i += TSDB.tagk_width();
      }
    }
    return tagks;
  }

  @Override
  public ExpressionDataPoint[] next(long timestamp) {
    throw new NotImplementedException();
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
    throw new NotImplementedException();
  }

  @Override
  public NumericFillPolicy getFillPolicy() {
    throw new NotImplementedException();
  }

  @Override
  public ITimeSyncedIterator getCopy() {
    return new IntersectionIterator(this);
  }

  @Override
  public boolean hasNext(int index) {
    for (final ITimeSyncedIterator sub : queries.values()) {
      if (sub.hasNext(index)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public void next(int index) {
    if (!hasNext()) {
      throw new IllegalDataException("No more data");
    }
    for (final ITimeSyncedIterator sub : queries.values()) {
      sub.next(index);
    }
  }

}
