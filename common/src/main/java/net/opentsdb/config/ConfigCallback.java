// This file is part of OpenTSDB.
// Copyright (C) 2018  The OpenTSDB Authors.
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
package net.opentsdb.config;

/**
 * A callback for dynamic configuration entries called any time the 
 * flattened value has been updated upstream.
 *
 * @param <T> The type of data the config entry is encoded as.
 * 
 * @since 3.0
 */
public interface ConfigCallback<T> {

  /**
   * Called from the config implementation only when a value has been
   * updated upstream to a different value.
   * 
   * @param key The key of the setting.
   * @param value The value updated by the config. May be null if the 
   * type is nullable.
   */
  public void update(final String key, final T value);
}
