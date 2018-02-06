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
package net.opentsdb.query.plan;

import com.google.common.base.Strings;

import net.opentsdb.data.MillisecondTimeStamp;
import net.opentsdb.data.TimeStamp;
import net.opentsdb.query.SliceConfig;
import net.opentsdb.query.SliceConfig.SliceType;
import net.opentsdb.query.pojo.Downsampler;
import net.opentsdb.query.pojo.TimeSeriesQuery;
import net.opentsdb.utils.DateTime;

/**
 * A base class for query planners.
 * TODO - a lot more work on this.
 * 
 * @param <T> The data type returned from the query.
 * 
 * @since 3.0
 */
public abstract class QueryPlanner<T> {
  /** TEMP raw interval in seconds */
  private static final int RAW_INTERVAL = 3600;
  
  /** TEMP The rollup interval in seconds */
  private static final int ROLLUP_INTERVAL = 86400;
  
  /** The original query. */
  protected final TimeSeriesQuery query;
  
  /** The planned query. */
  protected TimeSeriesQuery planned_query;
  
  /** The time range for the query. Will simply hold the start and end times */
  protected TimeStamp[][] query_time_ranges;
  
  /**
   * Default ctor.
   * @param query A non-null query to use.
   * @throws IllegalArgumentException if the query was null.
   */
  public QueryPlanner(final TimeSeriesQuery query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    this.query = query;
  }
  
  /**  @return The original user query */
  public TimeSeriesQuery getOriginalQuery() {
    return query;
  }
  
  /** @return The planned query. */
  public TimeSeriesQuery getPlannedQuery() {
    return planned_query;
  }
  
  /** @return The query time ranges. Will be null until {@link #generatePlan()} is called. */
  public TimeStamp[][] getTimeRanges() {
    return query_time_ranges;
  }
  
  /**
   * Called during initialization to generate the plan for the query.
   */
  protected abstract void generatePlan();
  
  /**
   * Attempts to split the given query on time so that blocks of time can be
   * cached and/or fetched separately. Queries without downsampling will split
   * into raw data chunks of 1 hour each. 
   * <p>
   * Ranges returned are meant to be inclusive of the start and end times (i.e. 
   * if the range is from T1 to T2, the results of a query using that range 
   * should include T1 and T2). Consumers of the data must de-dupe values in 
   * adjacent time ranges.
   * <p>
   * If a slice config is supplied in the query, that will take precedence over
   * all other configs.
   * <p>
   * If a downsampler is supplied, then the splits can be set based on the 
   * downsampling interval. If the interval is less than an hour and on 
   * boundaries that can be satisfied by data within an hour chunk (e.g. 1m, 5m,
   * 30m), 1 hour chunks will be returned. However if the interval is strange
   * (e.g. 45m, 17m) then no chunks will be returned.
   * <p>
   * Similarly if the rollup table can be used (e.g. 1h, 6h, 1d, 30d) then the
   * chunks will be aligned and returned based on 1 day or, if the interval is 
   * greater than a day, multi-day chunks.
   * <p> 
   * When ranges are returned, the start and end times are aligned to boundaries
   * based on the table data (i.e. raw or hourly).
   * <p>
   * The returned arrays are sorted by time ascending, i.e. range[0][] is
   * earlier than range[42][]. Each nested array contains the start Unix
   * epoch timestamp at index zero and the end Unix epoch timestamp at index 1.
   * The nested arrays are guaranteed to be non-null.
   * 
   * @param query A non null query to parse.
   * @return An array of zero or more {@link TimeStamp} arrays in milliseconds.
   * 
   * @throws IllegalArgumentException if the query is null, the query time hasn't
   * been set or the optional metric index is greater than the metric count.
   * 
   * @throws IllegalStateException if the number of intervals overflows. This
   * would only happen if someone asked for hundreds of thousands of years of
   * data. (and if that does happen, wtf?)
   */
  public static TimeStamp[][] getTimeRanges(final TimeSeriesQuery query) {
    return getTimeRanges(query, -1);
  }
  
  /**
   * Attempts to split the given query on time so that blocks of time can be
   * cached and/or fetched separately. Queries without downsampling will split
   * into raw data chunks of 1 hour each.
   * <p>
   * Ranges returned are meant to be inclusive of the start and end times (i.e. 
   * if the range is from T1 to T2, the results of a query using that range 
   * should include T1 and T2). Consumers of the data must de-dupe values in 
   * adjacent time ranges.
   * <p>
   * If a downsampler is supplied, then the splits can be set based on the 
   * downsampling interval. If the interval is less than an hour and on 
   * boundaries that can be satisfied by data within an hour chunk (e.g. 1m, 5m,
   * 30m), 1 hour chunks will be returned. However if the interval is strange
   * (e.g. 45m, 17m) then no chunks will be returned.
   * <p>
   * Similarly if the rollup table can be used (e.g. 1h, 6h, 1d, 30d) then the
   * chunks will be aligned and returned based on 1 day or, if the interval is 
   * greater than a day, multi-day chunks.
   * <p> 
   * When ranges are returned, the start and end times are aligned to boundaries
   * based on the table data (i.e. raw or hourly).
   * <p>
   * The returned arrays are sorted by time ascending, i.e. range[0][] is
   * earlier than range[42][]. Each nested array contains the start Unix
   * epoch timestamp at index zero and the end Unix epoch timestamp at index 1.
   * The nested arrays are guaranteed to be non-null.
   * 
   * @param query A non null query to parse.
   * @param metric_index An optional metric index to use for overriding the
   * {@code Time} downsampler. When not used, supply a negative value.
   * 
   * @return An array of zero or more {@link TimeStamp} arrays in milliseconds.
   * 
   * @throws IllegalArgumentException if the query is null, the query time hasn't
   * been set or the optional metric index is greater than the metric count.
   * 
   * @throws IllegalStateException if the number of intervals overflows. This
   * would only happen if someone asked for hundreds of thousands of years of
   * data. (and if that does happen, wtf?)
   * 
   * TODO - This guy needs to know about the table structure to align properly
   * on rows. This would also account for rollups.
   * TODO - handle Percent slices
   * TODO - handle calendar based DS
   */
  public static TimeStamp[][] getTimeRanges(final TimeSeriesQuery query, 
                                            final int metric_index) {
    if (query == null) {
      throw new IllegalArgumentException("The query hasn't been set.");
    }
    if (query.getTime() == null || 
        (Strings.isNullOrEmpty(query.getTime().getStart()) && 
        Strings.isNullOrEmpty(query.getTime().getEnd()))) {
      throw new IllegalArgumentException("The query time hasn't been set.");
    }
    if (metric_index >= 0 && metric_index >= query.getMetrics().size()) {
      throw new IllegalArgumentException("Metric index out of bounds.");
    }
    
    final SliceConfig slice_config;
    if (query.getTime().sliceConfig() != null ) {
      slice_config = query.getTime().sliceConfig();
    } else {
      slice_config = null;
    }
    
    // TODO - offsets on a per metric basis once we support those
    final long start = query.getTime().startTime().msEpoch() / 1000;
    final long end = query.getTime().endTime().msEpoch() / 1000;
    
    // figure out the interval based on either the row width or downsampling
    long interval = RAW_INTERVAL; // 1 hour by default unless we're overridden by ds or rollups
    
    // check for a downsampler. Metric DS overrides overall
    final Downsampler ds;
    if (metric_index >= 0 && 
        query.getMetrics().get(metric_index).getDownsampler() != null) {
      ds = query.getMetrics().get(metric_index).getDownsampler();
    } else if (query.getTime().getDownsampler() != null) {
      ds = query.getTime().getDownsampler();
    } else {
      ds = null;
    }
    
    final long ds_interval;
    if (ds != null) {
      ds_interval = DateTime.parseDuration(ds.getInterval()) / 1000;
      if (ds_interval < interval) {
        if (interval % ds_interval != 0) {
          interval = 0;
        }
      } else {
        // could potentially use the rollups if we have a proper boundary.
        if (ds_interval < ROLLUP_INTERVAL) {
          if (ROLLUP_INTERVAL % ds_interval != 0) {
            interval = 0;
          } else {
            interval = ROLLUP_INTERVAL;
          }
        } else if (ds_interval % ROLLUP_INTERVAL == 0) {
          interval = ds_interval;
        } else {
          interval = 0;
        }
      }
    } else {
      ds_interval = 0;
    }
    
    // we can't shard properly so return the full range without sharding
    if (interval < 1 || (slice_config != null 
        && slice_config.getSliceType() == SliceType.PERCENT 
        && slice_config.getQuantity() == 100)) {
      final TimeStamp[][] ranges = new TimeStamp[1][];
      ranges[0] = new TimeStamp[] { 
          new MillisecondTimeStamp(start * 1000), 
          new MillisecondTimeStamp(end * 1000) 
      };
      return ranges;
    }
    
    // if we have an absolute interval, use it IF it doesn't interfere with our 
    // downsampling.
    if (slice_config != null && slice_config.getSliceType() == SliceType.DURATION) {
      final long duration = DateTime.parseDuration(slice_config.getStringConfig()) / 1000;
      if (end - start < duration) {
        // return the start and end as the user want's a single slice
        final TimeStamp[][] ranges = new TimeStamp[1][];
        ranges[0] = new TimeStamp[] { 
            new MillisecondTimeStamp(start * 1000), 
            new MillisecondTimeStamp(end * 1000) 
        };
        return ranges;
      }
      
      if (ds != null) {
        if (ds_interval > duration) {
          throw new IllegalArgumentException("Slice duration " + duration 
              + " cannot be less than the downsampling duration " + ds_interval);
        }
        
        if (duration % ds_interval != 0) {
          throw new IllegalArgumentException("Downsampling duration " + ds_interval 
              + " does not sub-divide into the slice duration " + duration);
        }
        
        interval = duration;
      }
    }
    
    // snap start and end to the proper interval
    long snap_start = start - (start % interval);
    long snap_end = end - (end % interval) + interval;
    
    // NOTE - possible rollover if someone asks for around 200,000 years of data.
    int intervals = (int) ((snap_end - snap_start) / interval);
    
    if (slice_config != null && slice_config.getSliceType() == SliceType.PERCENT) {
      // TODO - handle percent.
    }
    
    // bump the interval by one if there is overflow, which there shouldn't be
    // since we're snapping. But in-case we don't snap in the future, leave this
    // here as a safety.
    intervals += ((snap_end - snap_start) - (intervals * interval)) > 0 ? 1 : 0;
    if (intervals < 0) {
      throw new IllegalStateException("The query interval is too narrow for "
          + "sharding: " + intervals);
    }
    
    final TimeStamp[][] ranges;
    if ((snap_end == end || snap_end - interval == end) && intervals > 1) {
      // if the query end aligns nicely with the end of an interval then we
      // don't want to spill over and fetch another set of data when we don't
      // need it. Ranges are inclusive end-to-end.
      ranges = new TimeStamp[intervals - 1][];
    } else {
      ranges = new TimeStamp[intervals][];
    }
    
    int idx = 0;
    while (snap_start < snap_end) {
      ranges[idx++] = new TimeStamp[] { 
          new MillisecondTimeStamp(snap_start * 1000), 
          new MillisecondTimeStamp((snap_start + interval) * 1000) 
      };
      snap_start += interval;
      if (idx >= ranges.length) {
        break;
      }
    }
    return ranges;
  }
}
