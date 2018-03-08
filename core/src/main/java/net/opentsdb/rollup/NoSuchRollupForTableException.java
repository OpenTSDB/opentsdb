// This file is part of OpenTSDB.
// Copyright (C) 2015 The OpenTSDB Authors.
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
package net.opentsdb.rollup;

import java.util.NoSuchElementException;

/**
 * Exception thrown when a rollup couldn't be found in the table name to rollup
 * interval map
 * @since 2.4
 */
public class NoSuchRollupForTableException extends NoSuchElementException {

  /**
   * Ctor that builds the message based on a string table lookup
   * @param table The table name
   */
  public NoSuchRollupForTableException(final String table) {
    super("No rollups configured for the table: " + table);
  }

  private static final long serialVersionUID = 6620255176637863260L;

}
