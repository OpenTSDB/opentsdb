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
      LowLevelTimeSeriesData {
    
    /** @return The hash of the current metric. */
    public long metricHash();
  }
}
