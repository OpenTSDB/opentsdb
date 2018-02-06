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

import java.util.Arrays;
import java.util.NoSuchElementException;

/**
 * Exception used when a Unique ID can't be found.
 *
 * @see UniqueId
 * @since 1.0
 */
public final class NoSuchUniqueId extends NoSuchElementException {

  /** The 'kind' of the table.  */
  private final String kind;
  /** The ID that couldn't be found.  */
  private final byte[] id;

  /**
   * Constructor.
   *
   * @param kind The kind of unique ID that triggered the exception.
   * @param id The ID that couldn't be found.
   */
  public NoSuchUniqueId(final String kind, final byte[] id) {
    super("No such unique ID for '" + kind + "': " + Arrays.toString(id));
    this.kind = kind;
    this.id = id;
  }

  /** Returns the kind of unique ID that couldn't be found.  */
  public String kind() {
    return kind;
  }

  /** Returns the unique ID that couldn't be found.  */
  public byte[] id() {
    return id;
  }

  static final long serialVersionUID = 1266815251;

}
