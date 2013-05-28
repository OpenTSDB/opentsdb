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
package net.opentsdb.search;

import net.opentsdb.core.TSDB;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;

import com.stumbleupon.async.Deferred;

/**
 * Search plugins allow data from OpenTSDB to be published to a search indexer.
 * Many great products already exist for searching so it doesn't make sense to
 * re-implement an engine within OpenTSDB. Likewise, going directly to the 
 * storage system for searching isn't efficient. 
 * <p>
 * <b>Note:</b> Implementations must have a parameterless constructor. The 
 * {@link #initialize()} method will be called immediately after the plugin is
 * instantiated and before any other methods are called.
 * <p>
 * <b>Note:</b> Since canonical information is stored in the underlying OpenTSDB 
 * database, the same document may be re-indexed more than once. This may happen
 * if someone runs a full re-indexing thread to make sure the search engine is
 * up to date, particularly after a TSD crash where some data may not have been
 * sent. Be sure to account for that when indexing. Each object has a way to 
 * uniquely identify it, see the method notes below.
 * <p>
 * <b>Warning:</b> All indexing methods should be performed asynchronously. You 
 * may want to create a queue in the implementation to store data until you can 
 * ship it off to the service. Every indexing method should return as quickly as 
 * possible.
 * @since 2.0
 */
public abstract class SearchPlugin {

  /**
   * Called by TSDB to initialize the plugin
   * Implementations are responsible for setting up any IO they need as well
   * as starting any required background threads.
   * <b>Note:</b> Implementations should throw exceptions if they can't start
   * up properly. The TSD will then shutdown so the operator can fix the 
   * problem. Please use IllegalArgumentException for configuration issues.
   * @param tsdb The parent TSDB object
   * @throws IllegalArgumentException if required configuration parameters are 
   * missing
   * @throws Exception if something else goes wrong
   */
  public abstract void initialize(final TSDB tsdb);
  
  /**
   * Called to gracefully shutdown the plugin. Implementations should close 
   * any IO they have open
   * <b>Note:</b> Please do not throw exceptions directly, store them in the 
   * Deferred callback chain.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> shutdown();
  
  /**
   * Should return the version of this plugin in the format:
   * MAJOR.MINOR.MAINT, e.g. 2.0.1. The MAJOR version should match the major
   * version of OpenTSDB the plugin is meant to work with.
   * @return A version string used to log the loaded version
   */
  public abstract String version();
  
  /**
   * Indexes a timeseries metadata object in the search engine
   * <b>Note:</b> Unique Document ID = TSUID 
   * <b>Note:</b> Please do not throw exceptions directly, store them in the 
   * Deferred callback chain.
   * @param meta The TSMeta to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexTSMeta(final TSMeta meta);
  
  /**
   * Called when we need to remove a timeseries meta object from the engine
   * <b>Note:</b> Unique Document ID = TSUID 
   * <b>Note:</b> Please do not throw exceptions directly, store them in the 
   * Deferred callback chain.
   * @param tsuid The hex encoded TSUID to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteTSMeta(final String tsuid);
  
  /**
   * Indexes a UID metadata object for a metric, tagk or tagv
   * <b>Note:</b> Unique Document ID = UID and the Type "TYPEUID"
   * @param meta The UIDMeta to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexUIDMeta(final UIDMeta meta);

  /**
   * Called when we need to remove a UID meta object from the engine
   * <b>Note:</b> Unique Document ID = UID and the Type "TYPEUID"
   * <b>Note:</b> Please do not throw exceptions directly, store them in the 
   * Deferred callback chain.
   * @param meta The UIDMeta to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteUIDMeta(final UIDMeta meta);

  /**
   * Indexes an annotation object
   * <b>Note:</b> Unique Document ID = TSUID and Start Time
   * @param note The annotation to index
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> indexAnnotation(final Annotation note);

  /**
   * Called to remove an annotation object from the index
   * <b>Note:</b> Unique Document ID = TSUID and Start Time
   * @param note The annotation to remove
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public abstract Deferred<Object> deleteAnnotation(final Annotation note);

  /**
   * Executes a very basic search query, returning the results in the SearchQuery
   * object passed in.
   * @param query The query to execute against the search engine
   * @return The query results
   */
  public abstract Deferred<SearchQuery> executeQuery(final SearchQuery query); 
}
