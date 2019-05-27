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

import net.opentsdb.core.BaseTSDBPlugin;

/**
 * Generates blockign queue object pools.
 * 
 * @since 3.0
 */
public class BlockingQueueObjectPoolFactory extends BaseTSDBPlugin 
  implements ObjectPoolFactory {

  @Override
  public ObjectPool newPool(final ObjectPoolConfig config) {
    return new BlockingQueueObjectPool(tsdb, config);
  }

  @Override
  public String type() {
    return "BlockingQueueObjectPoolFactory";
  }

  @Override
  public String version() {
    return "3.0.0";
  }

}
