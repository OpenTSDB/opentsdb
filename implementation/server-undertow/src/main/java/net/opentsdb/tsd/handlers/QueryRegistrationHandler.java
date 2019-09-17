// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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
package net.opentsdb.tsd.handlers;

import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;

public class QueryRegistrationHandler implements HttpHandler {
  
  private TSDB tsdb;
  private final HttpHandler next;
  
  public QueryRegistrationHandler(final TSDB tsdb, final HttpHandler next) {
    this.tsdb = tsdb;
    this.next = next;
  }

  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    if (!exchange.isComplete()) {
      // TODO - a white list.
      if (exchange.getRequestURI().contains("/api/query")) {
        // TODO - make sure this is actually unique.
        final int hash = exchange.hashCode();
        exchange.getRequestHeaders().add(new HttpString(
                OpenTSDBApplication.INTERNAL_HASH_HEADER),
            hash);

        exchange.addExchangeCompleteListener(new ExchangeCompletionListener() {
          @Override
          public void exchangeEvent(final HttpServerExchange exchange,
                                    final NextListener nextListener) {
            try {
              tsdb.completeRunningQuery(hash);
            } finally {
              nextListener.proceed();
            }
          }
        });
      }
    }
    next.handleRequest(exchange);
  }
}
