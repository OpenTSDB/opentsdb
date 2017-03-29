// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
package net.opentsdb.query.execution;

import java.util.concurrent.atomic.AtomicBoolean;

import com.stumbleupon.async.Deferred;

import net.opentsdb.execution.QueryExecutor;
import net.opentsdb.query.pojo.Query;

/**
 * A state container for asynchronous queries. All {@link QueryExecutor}s should
 * return an instance of this execution when responding to a query. The query
 * client can then wait on the {@link #deferred()} for results or if it needs
 * to shutdown, it can cancel the request.
 *
 * @param <T> The type of data that's returned by the query.
 * 
 * @since 3.0
 */
public abstract class QueryExecution<T> {
  /** The query that is associated with this request. */
  protected final Query query;
  
  /** The deferred that will be called with a result (good data or an exception) */
  protected final Deferred<T> deferred;
  
  /** A thread safe boolean to use when calling or canceling. */
  protected final AtomicBoolean completed;
  
  /**
   * Default ctor.
   * @param query A non-null query.
   * @throws IllegalArgumentException if the query was null.
   */
  public QueryExecution(final Query query) {
    if (query == null) {
      throw new IllegalArgumentException("Query cannot be null.");
    }
    this.query = query;
    deferred = new Deferred<T>();
    completed = new AtomicBoolean();
  }
  
  /** @return The deferred that will be called with a result. */
  public Deferred<T> deferred() {
    return deferred;
  }
  
  /**
   * Passes the result to the deferred and triggers it's callback chain.
   * @param result A non-null result of type T or an exception.
   * @throws IllegalStateException if the deferred was already called.
   */
  public void callback(final Object result) {
    if (completed.compareAndSet(false, true)) {
      deferred.callback(result);
    } else {
      throw new IllegalStateException("Callback was already executed: " + this);
    }
  }

  /** @return The query associated with this execution. */
  public Query query() {
    return query;
  }
  
  /** @return Whether or not the query has completed and the deferred has a 
   * result. */
  public boolean completed() {
    return completed.get();
  }
  
  /**
   * Cancels the query if it's outstanding. 
   * <b>WARNING:</b> Implementations must make sure that the {@link #deferred()}
   * is called somehow when a cancellation occurs.
   * <b>Note:</b> Race conditions are possible if the implementation calls into
   * {@link #callback(Object)}. Check the {@link #completed} state.
   */
  public abstract void cancel();
}
