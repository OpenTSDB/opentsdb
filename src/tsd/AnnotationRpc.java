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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.JSONException;

/**
 * Handles create, update, replace and delete calls for individual annotation
 * objects. Annotations are stored in the data table alongside data points.
 * Queries will return annotations along with the data if requested. This RPC
 * is only used for modifying the individual entries.
 * @since 2.0
 */
final class AnnotationRpc implements HttpRpc {
  private static final Logger LOG = LoggerFactory.getLogger(AnnotationRpc.class);
  
  /**
   * Performs CRUD methods on individual annotation objects.
   * @param tsdb The TSD to which we belong
   * @param query The query to parse and respond to
   */
  public void execute(final TSDB tsdb, HttpQuery query) throws IOException {
    final HttpMethod method = query.getAPIMethod();
    
    final String[] uri = query.explodeAPIPath();
    final String endpoint = uri.length > 1 ? uri[1] : "";
    if (endpoint != null && endpoint.toLowerCase().endsWith("bulk")) {
      executeBulk(tsdb, method, query);
      return;
    }
    
    final Annotation note;
    if (query.hasContent()) {
      note = query.serializer().parseAnnotationV1();
    } else {
      note = parseQS(query);
    }
    
    // GET
    if (method == HttpMethod.GET) {
      try {
        final Annotation stored_annotation = 
          Annotation.getAnnotation(tsdb, note.getTSUID(), note.getStartTime())
            .joinUninterruptibly();
        if (stored_annotation == null) {
          throw new BadRequestException(HttpResponseStatus.NOT_FOUND, 
              "Unable to locate annotation in storage");
        }
        query.sendReply(query.serializer().formatAnnotationV1(stored_annotation));
      } catch (BadRequestException e) {
        throw e;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    // POST
    } else if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      
      /**
       * Storage callback used to determine if the storage call was successful
       * or not. Also returns the updated object from storage.
       */
      class SyncCB implements Callback<Deferred<Annotation>, Boolean> {
        
        @Override
        public Deferred<Annotation> call(Boolean success) throws Exception {
          if (!success) {
            throw new BadRequestException(
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                "Failed to save the Annotation to storage", 
                "This may be caused by another process modifying storage data");
          }
          
          return Annotation.getAnnotation(tsdb, note.getTSUID(), 
              note.getStartTime());
        }
        
      }
      
      try {
        final Deferred<Annotation> process_meta = note.syncToStorage(tsdb, 
            method == HttpMethod.PUT).addCallbackDeferring(new SyncCB());
        final Annotation updated_meta = process_meta.joinUninterruptibly();
        tsdb.indexAnnotation(note);
        query.sendReply(query.serializer().formatAnnotationV1(updated_meta));
      } catch (IllegalStateException e) {
        query.sendStatusOnly(HttpResponseStatus.NOT_MODIFIED);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(e);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    // DELETE    
    } else if (method == HttpMethod.DELETE) {

      try {
        note.delete(tsdb).joinUninterruptibly();
        tsdb.deleteAnnotation(note);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(
            "Unable to delete Annotation information", e);
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
   * Performs CRUD methods on a list of annotation objects to reduce calls to
   * the API.
   * @param tsdb The TSD to which we belong
   * @param method The request method
   * @param query The query to parse and respond to
   */
  void executeBulk(final TSDB tsdb, final HttpMethod method, HttpQuery query) {
    if (method == HttpMethod.POST || method == HttpMethod.PUT) {
      executeBulkUpdate(tsdb, method, query);
    } else if (method == HttpMethod.DELETE) {
      executeBulkDelete(tsdb, query);
    } else {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
  }
  
  /**
   * Performs CRU methods on a list of annotation objects to reduce calls to
   * the API. Only supports body content and adding or updating annotation
   * objects. Deletions are separate.
   * @param tsdb The TSD to which we belong
   * @param method The request method
   * @param query The query to parse and respond to
   */
  void executeBulkUpdate(final TSDB tsdb, final HttpMethod method, HttpQuery query) {
    final List<Annotation> notes;
    try {
      notes = query.serializer().parseAnnotationsV1();
    } catch (IllegalArgumentException e){
      throw new BadRequestException(e);
    } catch (JSONException e){
      throw new BadRequestException(e);
    }
    final List<Deferred<Annotation>> callbacks = 
        new ArrayList<Deferred<Annotation>>(notes.size());
    
    /**
     * Storage callback used to determine if the storage call was successful
     * or not. Also returns the updated object from storage.
     */
    class SyncCB implements Callback<Deferred<Annotation>, Boolean> {
      final private Annotation note;
      
      public SyncCB(final Annotation note) {
        this.note = note;
      }
      
      @Override
      public Deferred<Annotation> call(Boolean success) throws Exception {
        if (!success) {
          throw new BadRequestException(
              HttpResponseStatus.INTERNAL_SERVER_ERROR,
              "Failed to save an Annotation to storage", 
              "This may be caused by another process modifying storage data: "
              + note);
        }
        
        return Annotation.getAnnotation(tsdb, note.getTSUID(), 
            note.getStartTime());
      }
    }
    
    /**
     * Simple callback that will index the updated annotation
     */
    class IndexCB implements Callback<Deferred<Annotation>, Annotation> {
      @Override
      public Deferred<Annotation> call(final Annotation note) throws Exception {
        tsdb.indexAnnotation(note);
        return Deferred.fromResult(note);
      }
    }
    
    for (Annotation note : notes) {
      try {
        Deferred<Annotation> deferred = 
            note.syncToStorage(tsdb, method == HttpMethod.PUT)
            .addCallbackDeferring(new SyncCB(note));
        Deferred<Annotation> indexer = 
            deferred.addCallbackDeferring(new IndexCB());
        callbacks.add(indexer);
      } catch (IllegalStateException e) {
        LOG.info("No changes for annotation: " + note);
      } catch (IllegalArgumentException e) {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
            e.getMessage(), "Annotation error: " + note, e);
      }
    }
    
    try {
      // wait untill all of the syncs have completed, then rebuild the list
      // of annotations using the data synced from storage.
      Deferred.group(callbacks).joinUninterruptibly();
      notes.clear();
      for (Deferred<Annotation> note : callbacks) {
        notes.add(note.joinUninterruptibly());
      }
      query.sendReply(query.serializer().formatAnnotationsV1(notes));
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * Handles bulk deletions of a range of annotations (local or global) using
   * query string or body data
   * @param tsdb The TSD to which we belong
   * @param query The query to parse and respond to
   */
  void executeBulkDelete(final TSDB tsdb, HttpQuery query) {
    try {
      final AnnotationBulkDelete delete_request;
      if (query.hasContent()) {
        delete_request = query.serializer().parseAnnotationBulkDeleteV1();
      } else {
        delete_request = parseBulkDeleteQS(query);
      }
      
      // validate the start time on the string. Users could request a timestamp of
      // 0 to delete all annotations, BUT we don't want them doing that accidentally
      if (delete_request.start_time == null || delete_request.start_time.isEmpty()) {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
            "Missing the start time value");
      }
      if (!delete_request.global && 
          (delete_request.tsuids == null || delete_request.tsuids.isEmpty())) {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST, 
            "Missing the TSUIDs or global annotations flag");
      }
      
      final int pre_allocate = delete_request.tsuids != null ? 
          delete_request.tsuids.size() + 1 : 1;
      List<Deferred<Integer>> deletes = new ArrayList<Deferred<Integer>>(pre_allocate);
      if (delete_request.global) {
        deletes.add(Annotation.deleteRange(tsdb, null, 
            delete_request.getStartTime(), delete_request.getEndTime()));
      }
      if (delete_request.tsuids != null) {
        for (String tsuid : delete_request.tsuids) {
          deletes.add(Annotation.deleteRange(tsdb, UniqueId.stringToUid(tsuid), 
              delete_request.getStartTime(), delete_request.getEndTime()));
        }
      }
      
      Deferred.group(deletes).joinUninterruptibly();
      delete_request.total_deleted = 0; // just in case the caller set it
      for (Deferred<Integer> count : deletes) {
        delete_request.total_deleted += count.joinUninterruptibly();
      }
      query.sendReply(query.serializer()
          .formatAnnotationBulkDeleteV1(delete_request));
    } catch (BadRequestException e) {
      throw e;
    } catch (IllegalArgumentException e) {
      throw new BadRequestException(e);
    } catch (RuntimeException e) {
      throw new BadRequestException(e);
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }
  }
  
  /**
   * Parses a query string for annotation information. Note that {@code custom}
   * key/values are not supported via query string. Users must issue a POST or
   * PUT with content data.
   * @param query The query to parse
   * @return An annotation object if parsing was successful
   * @throws IllegalArgumentException - if the request was malformed 
   */
  private Annotation parseQS(final HttpQuery query) {
    final Annotation note = new Annotation();
    
    final String tsuid = query.getQueryStringParam("tsuid");
    if (tsuid != null) {
      note.setTSUID(tsuid);
    }
    
    final String start = query.getQueryStringParam("start_time");
    final Long start_time = DateTime.parseDateTimeString(start, "");
    if (start_time < 1) {
      throw new BadRequestException("Missing start time");
    }
    // TODO - fix for ms support in the future
    note.setStartTime(start_time / 1000);
    
    final String end = query.getQueryStringParam("end_time");
    final Long end_time = DateTime.parseDateTimeString(end, "");
    // TODO - fix for ms support in the future
    note.setEndTime(end_time / 1000);
    
    final String description = query.getQueryStringParam("description");
    if (description != null) {
      note.setDescription(description);
    }
    
    final String notes = query.getQueryStringParam("notes");
    if (notes != null) {
      note.setNotes(notes);
    }
    
    return note;
  }

  /**
   * Parses a query string for a bulk delet request
   * @param query The query to parse
   * @return A bulk delete query
   */
  private AnnotationBulkDelete parseBulkDeleteQS(final HttpQuery query) {
    final AnnotationBulkDelete settings = new AnnotationBulkDelete();
    settings.start_time = query.getRequiredQueryStringParam("start_time");
    settings.end_time = query.getQueryStringParam("end_time");
    
    if (query.hasQueryStringParam("tsuids")) {
      String[] tsuids = query.getQueryStringParam("tsuids").split(",");
      settings.tsuids = new ArrayList<String>(tsuids.length);
      for (String tsuid : tsuids) {
        settings.tsuids.add(tsuid.trim());
      }
    }
    
    if (query.hasQueryStringParam("global")) {
      settings.global = true;
    }
    return settings;
  }
  
  /**
   * Represents a bulk annotation delete query. Either one or more TSUIDs must
   * be supplied or the global flag can be set to determine what annotations
   * are purged. Both may be set in one request. Annotations for the time 
   * between and including the start and end times will be removed based on
   * the annotation's recorded start time.
   */
  public static class AnnotationBulkDelete {
    /** The start time, may be relative, absolute or unixy */
    private String start_time;
    /** An option end time. If not set, current time is used */
    private String end_time;
    /** Optional list of TSUIDs */
    private List<String> tsuids;
    /** Optional flag to determine whether global notes for the range should be
     *  purged */
    private boolean global;
    /** Total number of items deleted (for later response to the user) */
    private long total_deleted;
    
    /**
     * Default ctor for Jackson
     */
    public AnnotationBulkDelete() {
      
    }
    
    /** @return The start timestamp in milliseconds */
    public long getStartTime() {
      return DateTime.parseDateTimeString(start_time, null);
    }
    
    /** @return The ending timestamp in milliseconds. If it wasn't set, the
     * current time is returned */
    public long getEndTime() {
      if (end_time == null || end_time.isEmpty()) {
        return System.currentTimeMillis();
      }
      return DateTime.parseDateTimeString(end_time, null);
    }

    /** @return List of TSUIDs to delete annotations for (may be NULL) */
    public List<String> getTsuids() {
      return tsuids;
    }

    /** @return Whether or not global annotations for the span should be purged */
    public boolean getGlobal() {
      return global;
    }

    /** @return The total number of annotations matched and deleted */
    public long getTotalDeleted() {
      return total_deleted;
    }

    /** @param start_time Start time for the range. May be relative, absolute 
     * or unixy in seconds or milliseconds */
    public void setStartTime(String start_time) {
      this.start_time = start_time;
    }

    /** @param end_time Optional end time to set for the range. Similar to start */
    public void setEndTime(String end_time) {
      this.end_time = end_time;
    }

    /** @param tsuids A list of TSUIDs to scan for annotations */
    public void setTsuids(List<String> tsuids) {
      this.tsuids = tsuids;
    }

    /** @param global Whether or not to delete global annotations for the range */
    public void setGlobal(boolean global) {
      this.global = global;
    }

    /** @param total_deleted Total number of annotations deleted */
    public void setTotalDeleted(long total_deleted) {
      this.total_deleted = total_deleted;
    }

  }
}
