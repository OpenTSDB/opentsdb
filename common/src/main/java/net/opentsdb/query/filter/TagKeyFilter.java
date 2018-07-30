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
package net.opentsdb.query.filter;

import java.util.Map;

/**
 * An interface to evaluate tag keys.
 * 
 * @since 3.0
 */
public interface TagKeyFilter extends QueryFilter {

  /** @return The non-null and non-empty filter string. */
  public String filter();
  
  /**
   * Whether or not the filter is satisfied with the tag keys.
   * @param tags A non-null (but possibly empty) map of tags.
   * @return True if satisfied, false if not.
   */
  public boolean matches(final Map<String, String> tags);
  
}
