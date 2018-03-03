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
package net.opentsdb.storage;

import net.opentsdb.core.TSDB;

public interface StorageSchemaFactory {

  /** @return A non-null and non-empty name for the schema unique amongst
   * all schemas, used during instantiation. */
  public String name();
  
  /** @return A non-null and non-emtpy description of the schema. */
  public String description();
  
  /**
   * Instantiate's a schema loading the proper data store.
   * @param tsdb A non-null TSDB.
   * @param id An optional ID.
   * @return A storage schema instance.
   */
  public StorageSchema newInstance(final TSDB tsdb, final String id);
}
