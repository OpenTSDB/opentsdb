// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.util.ArrayList;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

/**
 * Utility class for handling common tasks related to Deferreds.
 * 
 * @since 3.0
 */
public class Deferreds {

  /** A singleton to group deferreds that are expected to return nulls on completion. */
  public static final NullGroupCB NULL_GROUP_CB = new NullGroupCB(null);
  
  /** A simple class to group deferreds that are expected to return nulls on completion. */
  public static class NullGroupCB implements Callback<Object, ArrayList<Object>> {
    private final Deferred<Object> deferred;
    
    /**
     * Default Ctor
     * @param deferred a non-null Deferred to call on completion or null if
     * not used.
     */
    public NullGroupCB(final Deferred<Object> deferred) {
      this.deferred = deferred;
    }
    
    @Override
    public Object call(final ArrayList<Object> ignored) throws Exception {
      if (deferred != null) {
        deferred.callback(null);
      }
      return null;
    }
  }
}
