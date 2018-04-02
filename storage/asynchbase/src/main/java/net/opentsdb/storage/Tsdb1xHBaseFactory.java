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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.storage.schemas.tsdb1x.Schema;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStore;
import net.opentsdb.storage.schemas.tsdb1x.Tsdb1xDataStoreFactory;

/**
 * Simple singleton factory that implements a default and named HBase
 * clients (for different configurations).
 * 
 * @since 3.0
 */
public class Tsdb1xHBaseFactory implements Tsdb1xDataStoreFactory {
  
  /** A TSD to pull config data from. */
  private TSDB tsdb;
  
  /** The default clients. */
  protected volatile Tsdb1xHBaseDataStore default_client;
  
  /** A map of non-default clients. */
  protected Map<String, Tsdb1xHBaseDataStore> clients = Maps.newConcurrentMap();
  
  @Override
  public String id() {
    return "Tsdb1xHBaseFactory";
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb) {
    this.tsdb = tsdb;
    return Deferred.fromResult(null);
  }

  @Override
  public Deferred<Object> shutdown() {
    final List<Deferred<Object>> deferreds = Lists.newArrayListWithCapacity(
        clients.size() + (default_client != null ? 1 : 0));
    if (default_client != null) {
      deferreds.add(default_client.shutdown());
    }
    for (final Tsdb1xHBaseDataStore store : clients.values()) {
      deferreds.add(store.shutdown());
    }
    return Deferred.group(deferreds)
        .addCallback(new Callback<Object, ArrayList<Object>>() {
          @Override
          public Object call(final ArrayList<Object> ignored) throws Exception {
            return null;
          }
        });
  }

  @Override
  public String version() {
    return "3.0.0";
  }

  @Override
  public Tsdb1xDataStore newInstance(final TSDB tsdb, 
                                     final String id, 
                                     final Schema schema) {
    // DCLP on the default.
    if (Strings.isNullOrEmpty(id)) {
      if (default_client == null) {      
        synchronized (this) {
          if (default_client == null) {
            default_client = new Tsdb1xHBaseDataStore(this, null, schema);
          }
        }
      }
      
      return default_client;
    }
    
    Tsdb1xHBaseDataStore client = clients.get(id);
    if (client == null) {
      synchronized (this) {
        client = clients.get(id);
        if (client == null) {
          client = new Tsdb1xHBaseDataStore(this, id, schema);
          clients.put(id, client);
        }
      }
    }
    return client;
  }

  /** @return Package private TSDB instance to read the config. */
  TSDB tsdb() {
    return tsdb;
  }
}
