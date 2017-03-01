// This file is part of OpenTSDB.
// Copyright (C) 2015  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
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
