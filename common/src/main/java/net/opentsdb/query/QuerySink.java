// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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
