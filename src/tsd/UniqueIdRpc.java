// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import net.opentsdb.core.Const;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.TSUIDQuery;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;

/**
 * Handles calls for UID processing including getting UID status, assigning UIDs
 * and other functions.
 * @since 2.0
 */
final class UniqueIdRpc implements HttpRpc {

  @Override
  public void execute(TSDB tsdb, HttpQuery query) throws IOException {
    
    // the uri will be /api/vX/uid/? or /api/uid/?
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : ""; 

    if (endpoint.toLowerCase().equals("assign")) {
      this.handleAssign(tsdb, query);
      return;
    } else if (endpoint.toLowerCase().equals("uidmeta")) {
      this.handleUIDMeta(tsdb, query);
      return;
    } else if (endpoint.toLowerCase().equals("tsmeta")) {
      this.handleTSMeta(tsdb, query);
      return;
    } else {
      throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED, 
          "Other UID endpoints have not been implemented yet");
    }
  }

  /**
   * Assigns UIDs to the given metric, tagk or tagv names if applicable
   * <p>
   * This handler supports GET and POST whereby the GET command can
   * parse query strings with the {@code type} as their parameter and a comma
   * separated list of values to assign UIDs to.
   * <p>
   * Multiple types and names can be provided in one call. Each name will be
   * processed independently and if there's an error (such as an invalid name or
   * it is already assigned) the error will be stored in a separate error map
   * and other UIDs will be processed.
   * @param tsdb The TSDB from the RPC router
   * @param query The query for this request
   */
  private void handleAssign(final TSDB tsdb, final HttpQuery query) {
    // only accept GET And POST
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    final HashMap<String, List<String>> source;
    if (query.method() == HttpMethod.POST) {
      source = query.serializer().parseUidAssignV1();
    } else {
      source = new HashMap<String, List<String>>(3);
      // cut down on some repetitive code, split the query string values by
      // comma and add them to the source hash
      String[] types = {"metric", "tagk", "tagv"};
      for (int i = 0; i < types.length; i++) {
        final String values = query.getQueryStringParam(types[i]);
        if (values != null && !values.isEmpty()) {
          final String[] metrics = values.split(",");
          if (metrics != null && metrics.length > 0) {
            source.put(types[i], Arrays.asList(metrics));
          }
        }
      }
    }
    
    if (source.size() < 1) {
      throw new BadRequestException("Missing values to assign UIDs");
    }
    
    final Map<String, TreeMap<String, String>> response = 
      new HashMap<String, TreeMap<String, String>>();
    
    int error_count = 0;
    for (Map.Entry<String, List<String>> entry : source.entrySet()) {
      final TreeMap<String, String> results = 
        new TreeMap<String, String>();
      final TreeMap<String, String> errors = 
        new TreeMap<String, String>();

      final UniqueIdType type = UniqueIdType.fromString(entry.getKey());
      
      for (String name : entry.getValue()) {
        try {
          final byte[] uid = tsdb.getUniqueIdClient().assignUid(type, name);
          results.put(name, 
              UniqueId.uidToString(uid));
        } catch (IllegalArgumentException e) {
          errors.put(name, e.getMessage());
          error_count++;
        }
      }
      
      response.put(entry.getKey(), results);
      if (errors.size() > 0) {
        response.put(entry.getKey() + "_errors", errors);
      }
    }
    
    if (error_count < 1) {
      query.sendReply(query.serializer().formatUidAssignV1(response));
    } else {
      query.sendReply(HttpResponseStatus.BAD_REQUEST,
          query.serializer().formatUidAssignV1(response));
    }
  }

  /**
   * Handles CRUD calls to individual UIDMeta data entries
   * @param tsdb The TSDB from the RPC router
   * @param query The query for this request
   */
  private void handleUIDMeta(final TSDB tsdb, final HttpQuery query) {

    final HttpMethod method = query.getAPIMethod();
    // GET
    if (method == HttpMethod.GET) {
      
      final String uid = query.getRequiredQueryStringParam("uid");
      final UniqueIdType type = UniqueIdType.fromString(
              query.getRequiredQueryStringParam("type"));
      try {
        final UIDMeta meta = tsdb.getUIDMeta(type, uid)
        .joinUninterruptibly();
        query.sendReply(query.serializer().formatUidMetaV1(meta));
      } catch (NoSuchUniqueId e) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Could not find the requested UID", e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    // POST
    } else if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      
      final UIDMeta meta;
      if (query.hasContent()) {
        meta = query.serializer().parseUidMetaV1();
      } else {
        meta = this.parseUIDMetaQS(query);
      }
      
      /**
       * Storage callback used to determine if the storage call was successful
       * or not. Also returns the updated object from storage.
       */
      class SyncCB implements Callback<Deferred<UIDMeta>, Boolean> {
        
        @Override
        public Deferred<UIDMeta> call(Boolean success) throws Exception {
          if (!success) {
            throw new BadRequestException(
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Failed to save the UIDMeta to storage", 
                "This may be caused by another process modifying storage data");
          }
          
          return tsdb.getUIDMeta(meta.getType(), meta.getUID());
        }
        
      }
      
      try {
        final Deferred<UIDMeta> process_meta = tsdb.syncUIDMetaToStorage(meta,
          method == HttpMethod.PUT).addCallbackDeferring(new SyncCB());
        final UIDMeta updated_meta = process_meta.joinUninterruptibly();
        tsdb.indexUIDMeta(updated_meta);
        query.sendReply(query.serializer().formatUidMetaV1(updated_meta));
      } catch (IllegalStateException e) {
        query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (NoSuchUniqueId e) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Could not find the requested UID", e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    // DELETE    
    } else if (method == HttpMethod.DELETE) {
      
      final UIDMeta meta;
      if (query.hasContent()) {
        meta = query.serializer().parseUidMetaV1();
      } else {
        meta = this.parseUIDMetaQS(query);
      }
      try {
        tsdb.delete(meta).joinUninterruptibly();
        tsdb.deleteUIDMeta(meta);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unable to delete UIDMeta information", e);
      } catch (NoSuchUniqueId e) {
        throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
            "Could not find the requested UID", e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
      query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
      
    } else {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + method.getName() +
          "] is not permitted for this endpoint");
    }
  }
  
  /**
   * Handles CRUD calls to individual TSMeta data entries
   * @param tsdb The TSDB from the RPC router
   * @param query The query for this request
   */
  private void handleTSMeta(final TSDB tsdb, final HttpQuery query) {

    final HttpMethod method = query.getAPIMethod();
    // GET
    if (method == HttpMethod.GET) {
      
      String tsuid = null;
      if (query.hasQueryStringParam("tsuid")) {
        tsuid = query.getQueryStringParam("tsuid");
        try {
          final TSMeta meta = tsdb.getTSMeta(tsuid, true).joinUninterruptibly();
          if (meta != null) {
            query.sendReply(query.serializer().formatTSMetaV1(meta));
          } else {
            throw new BadRequestException(HttpResponseStatus.NOT_FOUND,
                "Could not find Timeseries meta data");
          } 
        } catch (NoSuchUniqueName e) {
          // this would only happen if someone deleted a UID but left the 
          // the timeseries meta data
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to find one of the UIDs", e);
        } catch (BadRequestException e) {
          throw e;
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        String mquery = query.getRequiredQueryStringParam("m");
        // m is of the following forms:
        // metric[{tag=value,...}]
        // where the parts in square brackets `[' .. `]' are optional.
        final HashMap<String, String> tags = new HashMap<String, String>();
        String metric = null;
        try {
          metric = Tags.parseWithMetric(mquery, tags);
        } catch (IllegalArgumentException e) {
          throw new BadRequestException(e);
        }
        final TSUIDQuery tsuid_query = new TSUIDQuery(tsdb);
        try {
          tsuid_query.setQuery(metric, tags);
          final List<TSMeta> tsmetas = tsuid_query.getTSMetas()
          .joinUninterruptibly();
          query.sendReply(query.serializer().formatTSMetaListV1(tsmetas));
        } catch (NoSuchUniqueName e) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to find one of the UIDs", e);
        } catch (BadRequestException e) {
          throw e;
        } catch (RuntimeException e) {
          throw new BadRequestException(e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    // POST / PUT
    } else if (method == HttpMethod.POST || method == HttpMethod.PUT) {

      final TSMeta meta;
      if (query.hasContent()) {
        meta = query.serializer().parseTSMetaV1();
      } else {
        meta = this.parseTSMetaQS(query);
      }
      
      /**
       * Storage callback used to determine if the storage call was successful
       * or not. Also returns the updated object from storage.
       */
      class SyncCB implements Callback<Deferred<TSMeta>, Boolean> {

        @Override
        public Deferred<TSMeta> call(Boolean success) throws Exception {
          if (!success) {
            throw new BadRequestException(
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Failed to save the TSMeta to storage",
                "This may be caused by another process modifying storage data");
          }

          return tsdb.getTSMeta(meta.getTSUID(), true);
        }

      }

      if (Strings.isNullOrEmpty(meta.getTSUID())) {
        // we got a JSON object without TSUID. Try to find a timeseries spec of
        // the form "m": "metric{tagk=tagv,...}"
        final String metric = query.getRequiredQueryStringParam("m");
        final String tsuid = getTSUIDForMetric(metric, tsdb);

        final boolean create = query.getQueryStringParam("create") != null &&
                               query.getQueryStringParam("create").equals("true");
        
        class WriteCounterIfNotPresentCB implements Callback<Boolean, Boolean> {
          @Override
          public Boolean call(Boolean exists) throws Exception {
            if (!exists && create) {
              tsdb.getMetaClient().createTimeseriesCounter(new TSMeta(tsuid));
            }

            return exists;
          }
        }

        try {
          // Check whether we have a TSMeta stored already
          final boolean exists = tsdb.getMetaClient().TSMetaExists(tsuid)
                  .joinUninterruptibly();
          // set TSUID
          meta.setTSUID(tsuid);
          
          if (!exists && create) {
            // Write 0 to counter column if not present
            tsdb.getMetaClient().TSMetaCounterExists(UniqueId.stringToUid(tsuid))
                    .addCallback(new WriteCounterIfNotPresentCB())
                .joinUninterruptibly();
            // set TSUID
            final Deferred<TSMeta> process_meta = tsdb.getMetaClient().create(meta)
                    .addCallbackDeferring(new SyncCB());
            final TSMeta updated_meta = process_meta.joinUninterruptibly();
            tsdb.indexTSMeta(updated_meta);
            tsdb.processTSMetaThroughTrees(updated_meta);
            query.sendReply(query.serializer().formatTSMetaV1(updated_meta));
          } else if (exists) {
            final Deferred<TSMeta> process_meta = tsdb.syncToStorage(meta,
                method == HttpMethod.PUT).addCallbackDeferring(new SyncCB());
            final TSMeta updated_meta = process_meta.joinUninterruptibly();
            tsdb.indexTSMeta(updated_meta);
            query.sendReply(query.serializer().formatTSMetaV1(updated_meta));
          } else {
            throw new BadRequestException(
                "Could not find TSMeta, specify \"create=true\" to create a new one.");
          }
        } catch (IllegalStateException e) {
          query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
        } catch (IllegalArgumentException e) {
          throw new BadRequestException(e);
        } catch (BadRequestException e) {
          throw e;
        } catch (NoSuchUniqueName e) {
          // this would only happen if someone deleted a UID but left the
          // the timeseries meta data
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND,
              "Unable to find one or more UIDs", e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      } else {
        try {
          final Deferred<TSMeta> process_meta = tsdb.syncToStorage(meta,
              method == HttpMethod.PUT).addCallbackDeferring(new SyncCB());
          final TSMeta updated_meta = process_meta.joinUninterruptibly();
          tsdb.indexTSMeta(updated_meta);
          query.sendReply(query.serializer().formatTSMetaV1(updated_meta));
        } catch (IllegalStateException e) {
          query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
        } catch (IllegalArgumentException e) {
          throw new BadRequestException(e);
        } catch (NoSuchUniqueName e) {
          // this would only happen if someone deleted a UID but left the
          // the timeseries meta data
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND,
              "Unable to find one or more UIDs", e);
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }
    // DELETE  
    } else if (method == HttpMethod.DELETE) {
      
      final TSMeta meta;
      if (query.hasContent()) {
        meta = query.serializer().parseTSMetaV1();
      } else {
        meta = this.parseTSMetaQS(query);
      }
      try {
        tsdb.getMetaClient().delete(meta);
        tsdb.deleteTSMeta(meta.getTSUID());
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Unable to delete TSMeta information", e);
      }
      query.sendStatusOnly(HttpResponseStatus.NO_CONTENT);
    } else {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + method.getName() +
          "] is not permitted for this endpoint");
    }
  }
  
  /**
   * Used with verb overrides to parse out values from a query string
   * @param query The query to parse
   * @return An UIDMeta object with configured values
   * @throws BadRequestException if a required value was missing or could not
   * be parsed
   */
  private UIDMeta parseUIDMetaQS(final HttpQuery query) {
    final byte[] uid = UniqueId.stringToUid(query.getRequiredQueryStringParam("uid"));
    final String type = query.getRequiredQueryStringParam("type");
    final UIDMeta meta = new UIDMeta(UniqueIdType.fromString(type), uid);
    final String display_name = query.getQueryStringParam("display_name");
    if (display_name != null) {
      meta.setDisplayName(display_name);
    }
    
    final String description = query.getQueryStringParam("description");
    if (description != null) {
      meta.setDescription(description);
    }
    
    final String notes = query.getQueryStringParam("notes");
    if (notes != null) {
      meta.setNotes(notes);
    }
    
    return meta;
  }
  
  /**
   * Used with verb overrides to parse out values from a query string
   * @param query The query to parse
   * @return An TSMeta object with configured values
   * @throws BadRequestException if a required value was missing or could not
   * be parsed
   */
  private TSMeta parseTSMetaQS(final HttpQuery query) {
    final String tsuid = query.getRequiredQueryStringParam("tsuid");
    final TSMeta meta = new TSMeta(tsuid);
    
    final String display_name = query.getQueryStringParam("display_name");
    if (display_name != null) {
      meta.setDisplayName(display_name);
    }
  
    final String description = query.getQueryStringParam("description");
    if (description != null) {
      meta.setDescription(description);
    }
    
    final String notes = query.getQueryStringParam("notes");
    if (notes != null) {
      meta.setNotes(notes);
    }
    
    final String units = query.getQueryStringParam("units");
    if (units != null) {
      meta.setUnits(units);
    }
    
    final String data_type = query.getQueryStringParam("data_type");
    if (data_type != null) {
      meta.setDataType(data_type);
    }
    
    final String retention = query.getQueryStringParam("retention");
    if (retention != null && !retention.isEmpty()) {
      try {
        meta.setRetention(Integer.parseInt(retention));
      } catch (NumberFormatException nfe) {
        throw new BadRequestException("Unable to parse 'retention' value");
      }
    }
    
    final String max = query.getQueryStringParam("max");
    if (max != null && !max.isEmpty()) {
      try {
        meta.setMax(Float.parseFloat(max));
      } catch (NumberFormatException nfe) {
        throw new BadRequestException("Unable to parse 'max' value");
      }
    }
    
    final String min = query.getQueryStringParam("min");
    if (min != null && !min.isEmpty()) {
      try {
        meta.setMin(Float.parseFloat(min));
      } catch (NumberFormatException nfe) {
        throw new BadRequestException("Unable to parse 'min' value");
      }
    }
    
    return meta;
  }

  /**
   * Parses a query string "m=metric{tagk1=tagv1,...}" type query and returns
   * a tsuid.
   * @param data_query The query we're building
   * @throws BadRequestException if we are unable to parse the query or it is
   * missing components
   */
  private String getTSUIDForMetric(final String query_string, TSDB tsdb) {
    if (query_string == null || query_string.isEmpty()) {
      throw new BadRequestException("The query string was empty");
    }

    // m is of the following forms:
    // metric[{tag=value,...}]
    // where the parts in square brackets `[' .. `]' are optional.
    final HashMap<String, String> tags = new HashMap<String, String>();
    String metric;
    try {
      metric = Tags.parseWithMetric(query_string, tags);
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    }
    final TreeMap<String, String> sortedTags = new TreeMap<String, String>(tags);
    // Byte Buffer to generate TSUID, pre allocated to the size of the TSUID
    final ByteArrayOutputStream buf = new ByteArrayOutputStream(
            Const.METRICS_WIDTH + sortedTags.size() *
                    (Const.TAG_NAME_WIDTH + Const.TAG_VALUE_WIDTH));
    try {
      buf.write(tsdb.getUniqueIdClient().getUID(UniqueIdType.METRIC, metric).joinUninterruptibly());
      for (Entry<String, String> e : sortedTags.entrySet()) {
        buf.write(tsdb.getUniqueIdClient().getUID(UniqueIdType.TAGK, e.getKey()).joinUninterruptibly(), 0, 3);
        buf.write(tsdb.getUniqueIdClient().getUID(UniqueIdType.TAGV, e.getValue()).joinUninterruptibly(), 0, 3);
      }
    } catch (IOException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      Throwables.propagate(e);
    }

    return UniqueId.uidToString(buf.toByteArray());
  }
}
