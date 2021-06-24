// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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

/**
 * An extension of the low level time series data that contains metric information.
 * 
 * @since 3.0
 */
public interface LowLevelMetricData extends LowLevelTimeSeriesData{
  
  /** The format of the metrics data. */
  public static enum ValueFormat {
    INTEGER, /** Signed 8 byte integer. */
    FLOAT,   /** Single precision float. */
    DOUBLE   /** Double precision float. */
  }

  /** @return The format of the metric name. */
  public StringFormat metricFormat();

  /** @return The starting offset into the {@link #metricBuffer()}. */
  public int metricStart();
  
  /** @return The length of metric data in bytes. */
  public int metricLength();
  
  /** @return The metric buffer to read it's name from. */
  public byte[] metricBuffer();

  /** @return The format of the data. Use to determine which method to call
   * to read data. */
  public ValueFormat valueFormat();
  
  /** @return The integer value if {@link #valueFormat()} was an int. */
  public long longValue();
  
  /** @return The floating point value if {@link #valueFormat()} was float. */
  public float floatValue();
  
  /** @return The double precision floating point value if {@link #valueFormat()} 
   * was double. */
  public double doubleValue();

  /**
   * A hashed metric implementation.
   * 
   * @since 3.0
   */
  public interface HashedLowLevelMetricData extends LowLevelMetricData, 
      HashedLowLevelTimeSeriesData {
    
    /** @return The hash of the current metric. */
    public long metricHash();
  }

  public interface RetriedLowLevelMetricData extends
          RetriedLowLevelTimeSeriesData,
          LowLevelMetricData{

  }

  public interface RetriedHashedLowLevelMetricData extends
          RetriedLowLevelMetricData,
          RetriedHashedLowLevelTimeSeriesData {

  }

  /**
   * A low level interface for rollup values. If multiple aggregates are present
   * in the source then {@link LowLevelTimeSeriesData#advance()} should return
   * the next aggregate.
   */
  public interface LowLevelRollupMetricData extends LowLevelMetricData {

    /** @return The optional rollup interval as a string if it's a time based
     * rollup. Null if only a pre-aggregate.
     */
    public String intervalString();

    /** @return The optional time based aggregator if set. */
    public String intervalAggregatorString();

    /** @return The optional ID of the time based aggregator if pulled from the
     * rollup config. -1 if not set in which case use {@link #intervalAggregatorString()}.
     * @return
     */
    public int intervalAggregator();

    /** @return The non-null aggregation string. */
    public String groupByAggregatorString();

    /** @return The optional numeric ID of the group aggregation from the rollup
     * configuration. -1 if not set in which case use {@link #groupByAggregatorString()}.
     */
    public int groupByAggregator();

  }

  /**
   * Extension for rollup data.
   */
  public interface HashedLowLevelRollupMetricData extends
          LowLevelRollupMetricData,
          HashedLowLevelMetricData {

  }

  /**
   * Extension for retried rollup data.
   */
  public interface RetriedLowLevelRollupMetricData extends RetriedLowLevelMetricData {

  }

  /**
   * Extension for retried rollup data.
   */
  public interface RetriedHashedLowLevelRollupMetricData extends
          RetriedLowLevelMetricData,
          HashedLowLevelRollupMetricData {

  }
}
