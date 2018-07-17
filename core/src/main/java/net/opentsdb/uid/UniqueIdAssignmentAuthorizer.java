// This file is part of OpenTSDB.
// Copyright (C) 2016-2018  The OpenTSDB Authors.
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

import com.stumbleupon.async.Deferred;

import net.opentsdb.auth.AuthState;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDBPlugin;
import net.opentsdb.data.TimeSeriesDatumId;

/**
 * A filter that can determine whether or not UIDs should be allowed assignment
 * based on their metric and tags and the user that sent the request. 
 * This is useful for such situations as:
 * <ul><li>Enforcing naming standards</li>
 * <li>Blacklisting certain names or properties</li>
 * <li>Preventing cardinality explosions</li></ul>
 * <b>Note:</b> Implementations must have a parameterless constructor. The 
 * {@link #initialize(TSDB)} method will be called immediately after the plugin is
 * instantiated and before any other methods are called.
 * 
 * @since 3.0
 */
public interface UniqueIdAssignmentAuthorizer extends TSDBPlugin {
  
  /**
   * Determine whether or not the UID should be assigned based on the
   * operating mode, value and user. If allowed, a null should be returned.
   * If not, the response should be a descriptive error string to return
   * to the caller.
   * 
   * @param auth The non-null auth state for roll validation. 
   * @param type The type of UID being assigned.
   * @param value The string value of the UID.
   * @param id The original ID, used in error messages.
   * @return Null if assignment is allowed, a non-null error string if 
   * the assignment should be blocked.
   */
  public Deferred<String> allowUIDAssignment(
      final AuthState auth,
      final UniqueIdType type, 
      final String value, 
      final TimeSeriesDatumId id);
  
  /**
   * Whether or not the filter should process UIDs. 
   * @return True if 
   * {@link #allowUIDAssignment(AuthState, UniqueIdType, String, TimeSeriesDatumId)}
   * should be called, false if not.
   */
  public boolean fillterUIDAssignments();
}
