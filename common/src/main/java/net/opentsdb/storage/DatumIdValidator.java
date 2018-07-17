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
package net.opentsdb.storage;

import net.opentsdb.data.TimeSeriesDatumId;

/**
 * An interface for validating the string identifier of a datum. E.g.
 * it will look at the metric and tags to determine if the characters 
 * are allowed for storage.
 * 
 * @since 3.0
 */
public interface DatumIdValidator {

  /**
   * Determines if the ID is valid for storage or not. If not, a string
   * is returned with a descriptive error message. If it is validated,
   * a null is returned.
   * @param id A non-null ID to evaluate.
   * @return Null if valid, an error message if not.
   */
  public String validate(final TimeSeriesDatumId id);
  
}
