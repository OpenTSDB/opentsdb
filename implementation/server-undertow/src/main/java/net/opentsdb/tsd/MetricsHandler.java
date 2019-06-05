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
package net.opentsdb.tsd;

import java.time.temporal.ChronoUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.undertow.server.ExchangeCompletionListener;
import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.stats.StatsCollector.StatsTimer;

/**
 * A handler to capture some metrics from Undertow shamelessly cribbed from the
 * included io.undertow.server.handlers.MetricsHandler.
 * 
 * @since 3.0
 */
public class MetricsHandler implements HttpHandler {
  private static Logger LOG = LoggerFactory.getLogger(MetricsHandler.class);
  
  private final StatsCollector stats_collector;
  private final HttpHandler next;
  
  public MetricsHandler(final StatsCollector stats_collector, 
                        final HttpHandler next) {
    this.stats_collector = stats_collector;
    this.next = next;
  }
  
  @Override
  public void handleRequest(HttpServerExchange exchange) throws Exception {
    if (!exchange.isComplete()) {
      final StatsTimer timer = stats_collector.startTimer(
          "undertow.request.latency", ChronoUnit.MILLIS);
      stats_collector.incrementCounter("undertow.requests");
      
      // from io.undertow.server.handlers.MetricsHandler
      exchange.addExchangeCompleteListener(new ExchangeCompletionListener() {
        @Override
        public void exchangeEvent(final HttpServerExchange exchange, 
                                  final NextListener nextListener) {
          try {
            timer.stop();
            stats_collector.incrementCounter("undertow.response.statuscode", 
                "code", Integer.toString(exchange.getStatusCode()));
          } catch (Throwable t) {
            // we don't want to ruin the call if something goes pear shaped in
            // the stats collector code.
            LOG.error("Failed to record stats for Undertow", t);
          }
          nextListener.proceed();
        }
      });
    }
    next.handleRequest(exchange);
  }

}
