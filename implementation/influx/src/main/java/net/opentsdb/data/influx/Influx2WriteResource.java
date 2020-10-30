// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.data.influx;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.resources.ServletResource;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;

/**
 * Handles an Influx V2 write call as per the documentation at:
 * https://docs.influxdata.com/influxdb/v2.0/api/#operation/PostWrite. Right now
 * it just handles the old style line protocol. 
 * 
 * TODO - handle timestamp precision.
 * TODO - handle the 'identity' content encoding?
 * TODO - handle zap trace span
 * TODO - handle arrow data type?
 * 
 * @since 3.0
 */
@Path("put/influx2/write")
public class Influx2WriteResource extends BaseTSDBPlugin implements ServletResource {
  public static final String KEY_PREFIX = "influx2.parser.";
  public static final String TYPE = Influx2WriteResource.class.getSimpleName();
  
  private String data_store_id;
  private boolean compute_hash;
  private WritableTimeSeriesDataStore data_store;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    registerConfigs(tsdb);
    
    data_store_id = tsdb.getConfig().getString(getConfigKey(
        InfluxWriteResource.DATA_STORE_ID));
    WritableTimeSeriesDataStoreFactory factory = tsdb.getRegistry()
        .getPlugin(WritableTimeSeriesDataStoreFactory.class, data_store_id);
    if (factory == null) {
      return Deferred.fromError(new IllegalStateException(
          "Unable to find a default data store factory for ID: " 
              + (data_store_id == null ? "Default" : data_store_id)));
    }
    data_store = factory.newStoreInstance(tsdb, null);
    if (data_store == null) {
      return Deferred.fromError(new IllegalStateException(
          "Unable to find a default data store for ID: " 
              + (data_store_id == null ? "Default" : data_store_id)));
    }
    
    compute_hash = tsdb.getConfig().getBoolean(getConfigKey(InfluxWriteResource.HASH));
    return Deferred.fromResult(null);
  }
  
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.TEXT_PLAIN)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request) throws Exception {
    
    final StringBuilder namespace_buffer = new StringBuilder();
    boolean found_org = false;
    String[] temp = request.getParameterValues("org");
    if (temp != null && temp.length > 0 && !Strings.isNullOrEmpty(temp[0])) {
      namespace_buffer.append(temp[0])
                      .append(".");
      found_org = true;
    }
    
    if (!found_org) {
      temp = request.getParameterValues("orgID");
      if (temp != null && temp.length > 0 && !Strings.isNullOrEmpty(temp[0])) {
        namespace_buffer.append(temp[0])
                        .append(".");
      }
    }
    
    temp = request.getParameterValues("bucket");
    if (temp != null && temp.length > 0 && !Strings.isNullOrEmpty(temp[0])) {
      namespace_buffer.append(temp[0]);
    }
    
    return InfluxWriteResource.parseStream(servlet_config, 
                                      request, 
                                      namespace_buffer.toString(), 
                                      data_store,
                                      compute_hash);
  }

  @Override
  public String type() {
    return TYPE;
  }
  
  protected void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(InfluxWriteResource.DATA_STORE_ID))) {
      tsdb.getConfig().register(getConfigKey(InfluxWriteResource.DATA_STORE_ID), null, false, 
          "The ID of a data store to write to.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(InfluxWriteResource.HASH))) {
      tsdb.getConfig().register(getConfigKey(InfluxWriteResource.HASH), false, false, 
          "Whether or not to compute the hashes on the Influx payload as we "
          + "parse. Depends no whether or not downstream requires the hashes.");
    }
  }
  
  protected String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ? 
        "" : id + ".")
      + suffix;
  }
  
}