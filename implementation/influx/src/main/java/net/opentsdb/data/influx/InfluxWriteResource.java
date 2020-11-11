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

import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import javax.servlet.ServletConfig;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.pools.ObjectPool;
import net.opentsdb.servlet.resources.ServletResource;
import net.opentsdb.storage.WritableTimeSeriesDataStore;
import net.opentsdb.storage.WritableTimeSeriesDataStoreFactory;

/**
 * Handles a 1.x InfluxDB call with data in the line protocol format.
 * 
 * @since 3.0
 */
@Path("put/influx/write")
public class InfluxWriteResource extends BaseTSDBPlugin implements ServletResource {
  private static Logger LOG = LoggerFactory.getLogger(InfluxWriteResource.class);
  
  public static final String KEY_PREFIX = "influx.parser.";
  public static final String HASH = "hash";
  public static final String DATA_STORE_ID = "store_id";
  public static final String ASYNC_CONSUMER = "asynchronous.enable";
  public static final String TYPE = InfluxWriteResource.class.getSimpleName();
  
  protected static final ThreadLocal<InfluxLineProtocolParser> PARSERS = 
      new ThreadLocal<InfluxLineProtocolParser>() {
    @Override
    protected InfluxLineProtocolParser initialValue() {
      return new InfluxLineProtocolParser();
    }
  };
  
  protected String data_store_id;
  protected boolean compute_hash;
  protected WritableTimeSeriesDataStore data_store;
  protected ObjectPool parser_pool;
  protected boolean async;
  
  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;
    registerConfigs(tsdb);
    
    data_store_id = tsdb.getConfig().getString(getConfigKey(DATA_STORE_ID));
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
    async = tsdb.getConfig().getBoolean(getConfigKey(ASYNC_CONSUMER));
    parser_pool = tsdb.getRegistry().getObjectPool(InfluxLineProtocolParserPool.TYPE);
    compute_hash = tsdb.getConfig().getBoolean(getConfigKey(HASH));
    return Deferred.fromResult(null);
  }
  
  @POST
  @Consumes(MediaType.TEXT_PLAIN)
  @Produces(MediaType.TEXT_PLAIN)
  public Response post(final @Context ServletConfig servlet_config, 
                       final @Context HttpServletRequest request) throws Exception {
    final String namespace;
    final String[] db = request.getParameterValues("db");
    if (db != null && db.length > 0) {
      namespace = db[0];
    } else {
      namespace = null;
    }
    return parseStream(servlet_config, request, namespace);
  }

  @Override
  public String type() {
    return TYPE;
  }
  
  /**
   * Parses the influx line protocol data.
   * @param servlet_config The servlet config.
   * @param request The non-null request we'll read the stream from.
   * @return The response to send to the caller.
   * @throws Exception If something goes pear shaped.
   */
  protected Response parseStream(final @Context ServletConfig servlet_config, 
                                 final @Context HttpServletRequest request, 
                                 final String namespace) 
                                            throws Exception {
    InfluxLineProtocolParser parser = null;
    try {
      InputStream stream = request.getInputStream();
      final String encoding = request.getHeader("Content-Encoding");
      if (!Strings.isNullOrEmpty(encoding)) {
        if (encoding.equalsIgnoreCase("gzip")) {
          stream = new GZIPInputStream(stream);
        }
      }
      
      if (async) {
        if (parser_pool != null) {
          parser = (InfluxLineProtocolParser) parser_pool.claim().object();
        } else {
          parser = new InfluxLineProtocolParser();
        }
        parser.computeHashes(compute_hash);
        parser.fillBufferFromStream(stream);
      } else {
        // sync
        parser = PARSERS.get();
        parser.computeHashes(compute_hash);
        parser.setInputStream(stream);
      }
      
      if (!Strings.isNullOrEmpty(namespace)) {
        parser.setNamespace(namespace);
      }
      
      data_store.write(null, parser, null);
      // TODO - proper async with a call to determine if we should wait to respond
      // with the write status(s) or not.
      return Response.noContent()
          .header("Content-Type", "text/plain")
          .build();
    } catch (Exception e) {
      LOG.error("Failed to parse data", e);
      // TODO - proper influx error format.
      throw new QueryExecutionException("Failed to parse write data.", 400, e);
    } finally {
      if (!async && parser != null) {
        parser.close();
      }
    }
  }
  
  protected void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(getConfigKey(DATA_STORE_ID))) {
      tsdb.getConfig().register(getConfigKey(DATA_STORE_ID), null, false, 
          "The ID of a data store to write to.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(HASH))) {
      tsdb.getConfig().register(getConfigKey(HASH), false, false, 
          "Whether or not to compute the hashes on the Influx payload as we "
          + "parse. Depends no whether or not downstream requires the hashes.");
    }
    if (!tsdb.getConfig().hasProperty(getConfigKey(ASYNC_CONSUMER))) {
      tsdb.getConfig().register(getConfigKey(ASYNC_CONSUMER), false, false, 
          "Whether or not the consumer of these messages is asynchronous meaning "
          + "we need to pull a parser from the pool and pass it on wherein the "
          + "consumer will be responsible for closing it.");
    }
  }
  
  protected String getConfigKey(final String suffix) {
    return KEY_PREFIX + (Strings.isNullOrEmpty(id) || id.equals(TYPE) ? 
        "" : id + ".")
      + suffix;
  }
  
}