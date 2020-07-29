// This file is part of OpenTSDB.
// Copyright (C) 2020 The OpenTSDB Authors.
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
package net.opentsdb.servlet.resources;

import java.io.IOException;

import javax.servlet.AsyncContext;
import javax.servlet.AsyncEvent;
import javax.servlet.AsyncListener;
import javax.servlet.http.HttpServletResponse;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.exceptions.QueryExecutionException;
import net.opentsdb.query.QueryContext;
import net.opentsdb.query.QueryMode;
import net.opentsdb.servlet.exceptions.GenericExceptionMapper;
import net.opentsdb.stats.Span;
import net.opentsdb.stats.Trace;

/**
 * Static helper to handle an async query. 
 * 
 * @since 3.0
 */
public class AsyncRunner {
  private static final Logger LOG = LoggerFactory.getLogger(AsyncRunner.class);
  
  private AsyncRunner() { }
  
  /**
   * Creates a listener and runs the query.
   * @param trace The trace state.
   * @param query_span Optional query span
   * @param setup_span Optional setup span.
   * @param async The non-null async context.
   * @param ctx The non-null query context.
   * @param resource The name of the resource that brought us here (for logging)
   */
  static void asyncRun(final Trace trace, 
                       final Span query_span, 
                       final Span setup_span,
                       final AsyncContext async, 
                       final QueryContext ctx,
                       final Class<?> resource) {
    class AsyncTimeout implements AsyncListener {

      @Override
      public void onComplete(final AsyncEvent event) throws IOException {
        // no-op
      }

      @Override
      public void onTimeout(final AsyncEvent event) throws IOException {
        LOG.error("[" + resource + "] The query has timed out for " + ctx);
        GenericExceptionMapper.serialize(
            new QueryExecutionException("The query has exceeded "
            + "the timeout limit.", 504), event.getAsyncContext().getResponse());
        ctx.close();
        event.getAsyncContext().complete();
      }

      @Override
      public void onError(final AsyncEvent event) throws IOException {
        LOG.error("[" + resource + "] WTF? An error for the AsyncTimeout?: " 
            + event.getThrowable());
        GenericExceptionMapper.serialize(
            new QueryExecutionException("Unhandled exception.", 500, 
                event.getThrowable()), event.getAsyncContext().getResponse());
        ctx.close();
      }

      @Override
      public void onStartAsync(final AsyncEvent event) throws IOException {
        // no-op
      }

    }

    async.addListener(new AsyncTimeout());
    async.start(new Runnable() {
      public void run() {
        Span execute_span = null;
        try {
          ctx.initialize(query_span).join();
          if (setup_span != null) {
            setup_span.setSuccessTags()
                      .finish();
          }
          
          if (ctx.query().getMode() == QueryMode.VALIDATE) {
            async.getResponse().setContentType("application/json");
            // TODO - here it would be better to write the query plan.
            async.getResponse().getWriter().write("{\"status\":\"OK\"}");
            ((HttpServletResponse) async.getResponse()).setStatus(200);
            async.complete();
            if (query_span != null) {
              query_span.setSuccessTags().finish();
            }
            return;
          }
          
          if (query_span != null) {
            execute_span = trace.newSpanWithThread("startExecution")
                .withTag("startThread", Thread.currentThread().getName())
                .asChildOf(query_span)
                .start();
          }
          ctx.fetchNext(query_span);
        } catch (Throwable t) {
          LOG.error("[" + resource + "] Unexpected exception adding callbacks "
              + "to deferred.", t);
          if (execute_span != null) {
            execute_span.setErrorTags(t)
                        .finish();
          }
          GenericExceptionMapper.serialize(t, async.getResponse());
          ctx.close();
          async.complete();
          if (query_span != null) {
            query_span.setErrorTags(t)
                       .finish();
          }
          throw new QueryExecutionException("Unexpected expection", 500, t);
        }
        
        if (execute_span != null) {
          execute_span.setSuccessTags()
                      .finish();
        }
      }
    });
  }
}
