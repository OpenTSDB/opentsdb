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
package net.opentsdb.query;

/**
 * An interface implemented by an upstream consumer or the final pipeline sink
 * to receive results from a query. This follows the observer pattern with the
 * following behavior for a chunked stream:
 * <ol>
 * <li>The sink or upstream will generally call {@code fetchNext()} on the 
 * pipeline.</li>
 * <li>The pipeline calls {@link #onNext(QueryResult)} with a result.</li>
 * <li>The sink calls {@code fetchNext()} again on the pipeline.</li>
 * <li>The pipeline either calls {@link #onNext(QueryResult)} with the next
 * result or {@link #onComplete()} to signal the end of the query.</li>
 * <li>The sink releases the query resources.</li>
 * </ol>
 * <p>
 * If no data is available for a given query, {@link #onComplete()} may be 
 * called before {@link #onNext(QueryResult)}. 
 * <p>
 * If at any time, an exception occurs, {@link #onError(Throwable)} will be
 * executed by the pipeline. No further calls to any method of the listener 
 * will be made and the pipeline should be closed.
 * 
 * @since 3.0
 */
public interface QuerySink {
  
  /**
   * Called by the pipeline to signify the end of data. Note that in asynchronous
   * operation this method may be called before the {@link #onNext(QueryResult)}
   * function has completed operation. 
   * <b>Note:</b> Pipelines must guarantee that the final {@link #onNext(QueryResult)}
   * call must complete before calling {@link #onComplete()} when operating in
   * any mode. 
   */
  public void onComplete();
  
  /**
   * Called by the pipeline when a new query result is available. The result is
   * always non-null but may have an empty result set. 
   * @param next A non-null result for a query.
   */
  public void onNext(final QueryResult next);
  
  /**
   * Called by the pipeline when an exception occurred at any point in processing.
   * When called, the pipeline is considered dead and no further calls will occur.
   * @param t The exception that was thrown.
   */
  public void onError(final Throwable t);
}
