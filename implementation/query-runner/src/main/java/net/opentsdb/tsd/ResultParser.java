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
package net.opentsdb.tsd;

/**
 * An interface for parsing results from the query runner and emitting additional
 * metrics like the number of series and values.
 * 
 * @since 3.0
 */
public interface ResultParser {

  /**
   * Parses the temporary file containing the results.
   * @param path The path to the temp file.
   * @param config The query config.
   * @param tags Tags for the query to report with the metrics.
   */
  public void parse(final String path, final QueryConfig config, final String[] tags);
  
}
