// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.pools;

/**
 * An exception thrown when something went pear shaped with the pool. Should
 * not be thrown or interrupt operation except when initializing a new pool.
 * 
 * @since 3.0
 */
public class ObjectPoolException extends RuntimeException {
  private static final long serialVersionUID = -7872368672672504583L;

  /**
   * Ctor with a useful message.
   * @param message A descriptive message.
   */
  public ObjectPoolException(final String message) {
    super(message);
  }
  
  /**
   * Ctor with a message and cause.
   * @param message A descriptive message.
   * @param t The cause of this exception.
   */
  public ObjectPoolException(final String message, final Throwable t) {
    super(message, t);
  }
  
  /**
   * Ctor with a cause.
   * @param t The cause of this exception.
   */
  public ObjectPoolException(final Throwable t) {
    super(t);
  }
  
}