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
package net.opentsdb.query;

/**
 * A configuration class for an iterator interpolator.
 * 
 * @since 3.0
 */
public interface QueryInterpolatorConfig {

  /** An optional string matching the name of an interpolator factory
   * registered with the TSD. If this name is null or empty then the
   * default factory is used.
   * @return The ID of a factory to fetch an interpolator from.
   */
  public String id();
  
  /**
   * The type of {@link TimeSeriesDataType} the interpolator works on. 
   * E.g. {@link NumericType}. For now this must be the full class name
   * of a data type.
   * @return A non-null data type name.
   */
  public String type();
}
