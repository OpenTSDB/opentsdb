package net.opentsdb.tsd.handlers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HttpString;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.threadpools.TSDTask;

public class QueryRegistrationHandler implements HttpHandler {
  
  private static final Logger LOG = LoggerFactory.getLogger(QueryRegistrationHandler.class);

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
