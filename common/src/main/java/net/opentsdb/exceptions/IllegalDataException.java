// This file is part of OpenTSDB.
// Copyright (C) 2011-2017  The OpenTSDB Authors.
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
package net.opentsdb.exceptions;

/**
 * Some illegal / malformed / corrupted data has been found in the pipeline.
 * @since 1.0
 */
public final class IllegalDataException extends IllegalStateException {

  /**
   * Constructor.
   *
   * @param msg Message describing the problem.
   */
  public IllegalDataException(final String msg) {
    super(msg);
  }

  /**
   * Constructor.
   *
   * @param msg Message describing the problem.
   * @param cause The source exception.
   */
  public IllegalDataException(final String msg, final Throwable cause) {
    super(msg, cause);
  }

  static final long serialVersionUID = 1307719142;

}
