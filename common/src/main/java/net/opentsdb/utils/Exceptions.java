// This file is part of OpenTSDB.
// Copyright (C) 2015-2017  The OpenTSDB Authors.
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
package net.opentsdb.utils;

import com.stumbleupon.async.DeferredGroupException;

/**
 * A class with utility methods for dealing with Exceptions in OpenTSDB
 * @since 2.2
 */
public class Exceptions {

  /**
   * Iterates through the stack trace, looking for the actual cause of the
   * deferred group exception. These traces can be huge and truncated in the
   * logs so it's really useful to be able to spit out the source.
   * @param e A DeferredGroupException to parse
   * @return The root cause of the exception if found. 
   */
  public static Throwable getCause(final DeferredGroupException e) {
    Throwable ex = e;
    while (ex.getClass().equals(DeferredGroupException.class)) {
      if (ex.getCause() == null) {
        break;
      } else {
        ex = ex.getCause();
      }
    }
    return ex;
  }
}
