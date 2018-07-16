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
package net.opentsdb.data;

import java.util.Map;

/**
 * An ID representing a single dataum or value. This is opposed to the
 * {@link TimeSeriesId} which is used at query time and may include 
 * multiple time series wrapped up into a new ID. Hence this ID does not
 * have any aggregated or disjoint tag keys.
 * 
 * @since 3.0
 */
public interface TimeSeriesDatumStringId extends TimeSeriesDatumId {

  /**
   * An optional tenant or group name for the time series.
   * May be null or empty if namespaces are not in use for the platform.
   *  
   * @return A string if set, null if not used.
   */
  public String namespace();
  
  /**
   * The metric component of the time series ID. This is a required value and
   * may not be null or empty.
   *  
   * @return A non-null and non-empty string.
   */
  public String metric();
  
  /**
   * A map of tag name and value pairs included in the sources for this time 
   * series. If the underlying storage system does not support tags or none of
   * the source time series had tags pairs in common, this list may be empty.
   * The map key represents a tag name (tagk) and the map value represents a 
   * tag value (tagv).
   * <p>
   * Invariant: Each tag pair must appear in every source time series.
   * 
   * @return A non-null map of zero or more tag pairs.
   */
  public Map<String, String> tags();
  
}
