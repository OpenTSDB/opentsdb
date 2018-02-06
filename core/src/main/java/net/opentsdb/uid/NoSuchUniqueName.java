// This file is part of OpenTSDB.
// Copyright (C) 2010-2017  The OpenTSDB Authors.
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
package net.opentsdb.uid;

import java.util.NoSuchElementException;

/**
 * Exception used when a name's Unique ID can't be found.
 *
 * @see UniqueId
 * @since 3.0
 */
public final class NoSuchUniqueName extends NoSuchElementException {

  /** The 'kind' of the table.  */
  private final String kind;
  /** The name that couldn't be found.  */
  private final String name;

  /**
   * Constructor.
   *
   * @param kind The kind of unique ID that triggered the exception.
   * @param name The name that couldn't be found.
   */
  public NoSuchUniqueName(final String kind, final String name) {
    super("No such name for '" + kind + "': '" + name + "'");
    this.kind = kind;
    this.name = name;
  }

  /** Returns the kind of unique ID that couldn't be found.  */
  public String kind() {
    return kind;
  }

  /** Returns the name for which the unique ID couldn't be found.  */
  public String name() {
    return name;
  }

  static final long serialVersionUID = 1266815261;

}
