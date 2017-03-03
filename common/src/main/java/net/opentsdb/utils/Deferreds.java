// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.util.ArrayList;

import com.stumbleupon.async.Callback;

/**
 * Utility class for handling common tasks related to Deferreds.
 * 
 * @since 3.0
 */
public class Deferreds {

  /** A singleton to group deferreds that are expected to return nulls on completion. */
  public static final NullGroupCB NULL_GROUP_CB = new NullGroupCB();
  
  /** A simple class to group deferreds that are expected to return nulls on completion. */
  static class NullGroupCB implements Callback<Object, ArrayList<Object>> {
    @Override
    public Object call(final ArrayList<Object> ignored) throws Exception {
      return null;
    }
  }
}
