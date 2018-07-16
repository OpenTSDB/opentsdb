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
package net.opentsdb.storage.schemas.tsdb1x;

import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import net.opentsdb.common.Const;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.data.TimeSeriesId;
import net.opentsdb.storage.ReadableTimeSeriesDataStore;
import net.opentsdb.storage.TimeSeriesDataStoreFactory;

/**
 * Simple singleton factory that implements a default and named schemas
 * (for different configurations).
 * 
 * @since 3.0
 */
public class SchemaFactory extends BaseTSDBPlugin 
                           implements TimeSeriesDataStoreFactory {

  /** The default schema. */
  protected volatile Schema default_schema;
  
  /** A map of non-default schemas. */
  protected Map<String, Schema> schemas = Maps.newConcurrentMap();
  
  @Override
  public ReadableTimeSeriesDataStore newInstance(final TSDB tsdb, final String id) {
    // DCLP on the default.
    if (Strings.isNullOrEmpty(id)) {
      if (default_schema == null) {      
        synchronized (this) {
          if (default_schema == null) {
            default_schema = new Schema(tsdb, null);
          }
        }
      }
      
      return default_schema;
    }
    
    Schema schema = schemas.get(id);
    if (schema == null) {
      synchronized (this) {
        schema = schemas.get(id);
        if (schema == null) {
          schema = new Schema(tsdb, id);
          schemas.put(id, schema);
        }
      }
    }
    return schema;
  }

  @Override
  public TypeToken<? extends TimeSeriesId> idType() {
    return Const.TS_BYTE_ID;
  }
  
  @Override
  public String id() {
    return "Tsdb1xSchemaFactory";
  }

  @Override
  public String version() {
    // TODO Implement
    return "3.0.0";
  }

}
