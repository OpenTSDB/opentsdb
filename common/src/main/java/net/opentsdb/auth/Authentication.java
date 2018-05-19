// This file is part of OpenTSDB.
// Copyright (C) 2017-2018 The OpenTSDB Authors.
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
package net.opentsdb.auth;

/**
 * A plugin interface for performing authentication for OpenTSDB API access.
 * The plugin is embedded within the Netty pipeline and intercepts requests
 * on new channels. Once a channel is authenticated successfully, the plugin is
 * removed from the pipeline so further calls on that channel are not evaluated.
 * <p>
 * An AuthState object is attached to the channel for evaluation later in the
 * pipeline. This state cannot be changed but cane be replaced.
 * <p>
 * The plugin also includes an acessor to an Authorization plugin to allow or 
 * disallow operations per user.  
 * 
 * @since 2.4
 */
public interface Authentication {
  
  /** The key from our config set when Authentication is required. */
  public static final String AUTH_ENABLED_KEY = "tsd.core.authentication.enable";
  
  // TODO - what are common methods we can define?
  
  /**
   * An optional authorization object. If authorization is not enabled, this 
   * call may return null.
   * @return An authorization object or null;
   */
  public Authorization authorization();
  
}