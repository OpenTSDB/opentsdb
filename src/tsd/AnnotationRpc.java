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
   * Performs RU methods on a list of annotation objects to reduce calls to
   * the API. Only supports body content and adding or updating annotation
   * objects. Deletions are separate.
   * @param tsdb The TSD to which we belong
   * @param method The request method
   * @param query The query to parse and respond to
   */
  void executeBulk(final TSDB tsdb, final HttpMethod method, HttpQuery query) {
    // only accept POST And PUT
    if (query.method() != HttpMethod.POST && query.method() != HttpMethod.PUT) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
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
        callbacks.add(deferred.addCallbackDeferring(new IndexCB()));
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
}
