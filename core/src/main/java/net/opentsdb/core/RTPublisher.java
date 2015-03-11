// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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
package net.opentsdb.core;

import java.util.Map;

import net.opentsdb.meta.Annotation;

import com.stumbleupon.async.Deferred;
import net.opentsdb.uid.TimeseriesId;

/**
 * Real Time publisher plugin interface that is used to emit data from a TSD
 * as data comes in. Initially it supports publishing data points immediately
 * after they are queued for storage. In the future we may support publishing
 * meta data or other types of information as changes are made.
 * <p>
 * <b>Warning:</b> All processing should be performed asynchronously and return
 * a Deferred as quickly as possible.
 * @since 2.0
 */
public abstract class RTPublisher extends Plugin {
  /**
   * Called every time a new data point with an integer value is published.
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds
   * (depending on the TSD's configuration)
   * @param value Value for the data point
   * @param tags Tagk/v pairs
   * @param tsuid Time series UID for the value
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public abstract Deferred<Object> publishDataPoint(final String metric, 
      final long timestamp, final long value, final Map<String, String> tags, 
      final TimeseriesId tsuid);
  
  /**
   * Called every time a new data point with a floating point value is published.
   * @param metric The name of the metric associated with the data point
   * @param timestamp Timestamp as a Unix epoch in seconds or milliseconds
   * (depending on the TSD's configuration)
   * @param value Value for the data point
   * @param tags Tagk/v pairs
   * @param tsuid Time series UID for the value
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public abstract Deferred<Object> publishDataPoint(final String metric, 
      final long timestamp, final double value, final Map<String, String> tags, 
      final TimeseriesId tsuid);
  
  /**
   * Called any time a new annotation is published
   * @param annotation The published annotation
   * @return A deferred without special meaning to wait on if necessary. The 
   * value may be null but a Deferred must be returned.
   */
  public abstract Deferred<Object> publishAnnotation(Annotation annotation);
}
