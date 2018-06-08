// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.storage.schemas.tsdb1x;

import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.data.TimeSeries;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.data.TimeSpecification;
import net.opentsdb.query.QueryNode;
import net.opentsdb.query.QueryResult;
import net.opentsdb.query.QuerySourceConfig;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.rollup.RollupConfig;

/**
 * The base class for collecting Tsdb1x data fetched from storage.
 * 
 * @since 3.0
 */
public class Tsdb1xQueryResult implements QueryResult {
  
  /** The ID for this sequence. */
  protected final long sequence_id;
  
  /** The parent node for this result. */
  protected final QueryNode node;
  
  /** The schema used for encoding the data in this result. */
  protected final Schema schema;
  
  /** The map of TSUID hashes to time series. */
  protected final Map<Long, TimeSeries> results;
  
  /** The byte limit to determine when we're full. */
  protected final long byte_limit;
  
  /** The data point limit to determine when we're full. */
  protected final long dp_limit;
  
  /** Whether or not the query is operating in time descending order. */
  protected final boolean reversed;
  
  /** Whether or not to keep earlier duplicates or later. */
  protected final boolean keep_earliest;
  
  /** Whether or not the result is full. */
  protected volatile boolean is_full;
  
  /** The number of bytes stored in the result. Rough estimate so far. */
  protected volatile long bytes;
  
  /** The number of values stored in the result. */
  protected volatile long dps;
  
  /** The resolution of the data. */
  protected volatile ChronoUnit resolution;
  
  /**
   * Default ctor. The node is expected to have a {@link QuerySourceConfig}
   * configuration that will give us a {@link Configuration} config to
   * use when determining the byte and dp limits.
   * @param sequence_id The sequence ID.
   * @param node The non-null parent node.
   * @param schema The non-null schema.
   */
  public Tsdb1xQueryResult(final long sequence_id, 
                           final QueryNode node,
                           final Schema schema) {
    if (sequence_id < 0) {
      throw new IllegalArgumentException("Sequence ID cannot be less "
          + "than zero.");
    }
    if (node == null) {
      throw new IllegalArgumentException("Node cannot be null.");
    }
    if (schema == null) {
      throw new IllegalArgumentException("Schema cannot be null.");
    }
    this.sequence_id = sequence_id;
    this.node = node;
    this.schema = schema;
    results = Maps.newConcurrentMap();
    
    final Configuration config = 
        ((QuerySourceConfig) node.config()).configuration();
    final TimeSeriesQuery query = (TimeSeriesQuery) 
        ((QuerySourceConfig) node.config()).query();
    byte_limit = query.getInt(config, Schema.QUERY_BYTE_LIMIT_KEY);
    dp_limit = query.getInt(config, Schema.QUERY_DP_LIMIT_KEY);
    reversed = query.getBoolean(config, Schema.QUERY_REVERSE_KEY);
    keep_earliest = query.getBoolean(config, Schema.QUERY_KEEP_FIRST_KEY);
    resolution = ChronoUnit.SECONDS; // default
  }
  
  @Override
  public TimeSpecification timeSpecification() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Collection<TimeSeries> timeSeries() {
    return results.values();
  }

  @Override
  public long sequenceId() {
    return sequence_id;
  }

  @Override
  public QueryNode source() {
    return node;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_BYTE_ID;
  }

  @Override
  public ChronoUnit resolution() {
    return resolution;
  }
  
  @Override
  public RollupConfig rollupConfig() {
    return schema.rollupConfig();
  }
  
  @Override
  public void close() {
    for (final TimeSeries series : results.values()) {
      series.close();
    }
  }
  
  /** @return True if the byte or data point limit has been exceeded. */
  public boolean isFull() {
    return is_full;
  }
  
  /** @return Whether or not to sort in time descending order. */
  public boolean reversed() {
    return reversed;
  }
  
  /** @return Whether or not to keep the earliest duplicates. */
  public boolean keepEarliest() {
    return keep_earliest;
  }
  
  /**
   * Adds the sequence to the proper time series set.
   * NOTE: Since it's fast path, we aren't performing all the data
   * checks we could. Be careful.
   * @param tsuid_hash A hash of the TSUID.
   * @param tsuid The non-null and non-empty TSUID.
   * @param sequence The non-null row sequence to add.
   * @param resolution The highest resolution of the sequence.
   */
  public void addSequence(final long tsuid_hash,
                          final byte[] tsuid, 
                          final RowSeq sequence,
                          final ChronoUnit resolution) {
    // TODO - figure out the size for the new TimeSeries objects
    TimeSeries series = results.get(tsuid_hash);
    if (series == null) {
      series = new Tsdb1xTimeSeries(tsuid, schema);
      TimeSeries extant = results.putIfAbsent(tsuid_hash, series);
      if (extant != null) {
        // lost the race
        series = extant;
      }
    }
    ((Tsdb1xTimeSeries) series).addSequence(sequence, 
                                            reversed, 
                                            keep_earliest, 
                                            schema);
    
    // NOTE: This can overcount if we have dupes that are dropped when
    // multiple rows are merged in a sequence.
    synchronized (this) {
      bytes += sequence.size();
      dps += sequence.dataPoints();
      if (this.resolution == null || 
          resolution.ordinal() < this.resolution.ordinal()) {
        this.resolution = resolution;
      }
    }
    
    // since it's a best effort we don't need the lock
    if (byte_limit > 0 && bytes > byte_limit) {
      is_full = true;
    }
    if (dp_limit > 0 && dps > dp_limit) {
      is_full = true;
    }
  }

  /** @return An error message based on the byte or dp limit. Note that 
   * this method doesn't check to see if we "are" full. If it's not full
   * then the DP error is returned. */
  public String resultIsFullErrorMessage() {
    if (byte_limit > 0 && bytes > byte_limit) {
      // TODO - properly format in MB or GB or KB...
      final long mbs = (byte_limit / 1024 / 1024);
      return "Sorry, you have attempted to fetch more than our maximum "
          + "amount of " + (mbs > 0 ? mbs : 
            ((double) byte_limit / 1024. / 1024.)) + "MB from storage. " 
          + "Please try filtering using more tags or decrease your time range.";
    }
    
    return "Sorry, you have attempted to fetch more than our limit of " 
      + dp_limit + " data points. Please try filtering "
      + "using more tags or decrease your time range.";
  }

  
}
