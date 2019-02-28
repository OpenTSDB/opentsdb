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

import net.opentsdb.core.TSDBPlugin;

/**
 * A factory plugin that returns {@link ObjectPool} instances.
 * 
 * @since 3.0
 */
public interface ObjectPoolFactory extends TSDBPlugin {

  /**
   * Instantiates a new pool with the given config.
   * @param config A non-null config.
   * @return A non-null pool or an exception if the config was missing required
   * information.
   */
  public ObjectPool newPool(final ObjectPoolConfig config);
  
}
