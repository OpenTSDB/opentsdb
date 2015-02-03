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

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.JsonReporter;
import net.opentsdb.stats.SerializerReporter;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.stumbleupon.async.Deferred;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Handles fetching statistics from all over the code, collating them in a
 * string buffer or list, and emitting them to the caller. Stats are collected
 * lazily, i.e. only when this method is called.
 * This class supports the 1.x style HTTP call as well as the 2.x style API
 * calls.
 * @since 2.0
 */
public final class StatsRpc implements TelnetRpc, HttpRpc {
  private final MetricRegistry metricRegistry;
  private final ConsoleReporter.Builder textReporterBuilder;

  public StatsRpc(final MetricRegistry metricRegistry) {
    this.metricRegistry = checkNotNull(metricRegistry);

    textReporterBuilder = ConsoleReporter.forRegistry(metricRegistry)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .convertRatesTo(TimeUnit.SECONDS);
  }

  /**
   * Telnet RPC responder that returns the stats in ASCII style
   * @param tsdb The TSDB to use for fetching stats
   * @param chan The netty channel to respond on
   * @param cmd call parameters
   */
  public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
      final String[] cmd) {
    final ByteArrayOutputStream output = new ByteArrayOutputStream();
    final ConsoleReporter consoleReporter = textReporterBuilder
            .outputTo(new PrintStream(output))
            .build();

    consoleReporter.report();

    chan.write(output.toString());

    return Deferred.fromResult(null);
  }

  /**
   * HTTP resposne handler
   * @param tsdb The TSDB to which we belong
   * @param query The query to parse and respond to
   */
  public void execute(final TSDB tsdb, final HttpQuery query) {
    // only accept GET/POST
    if (query.method() != HttpMethod.GET && query.method() != HttpMethod.POST) {
      throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED, 
          "Method not allowed", "The HTTP method [" + query.method().getName() +
          "] is not permitted for this endpoint");
    }
    
    // if we don't have an API request we need to respond with the 1.x version
    if (query.apiVersion() < 1) {
      if (query.hasQueryStringParam("json")) {
        query.sendReply(new JsonReporter(metricRegistry).report());
      } else {
        final ByteArrayOutputStream output = new ByteArrayOutputStream();
        final ConsoleReporter consoleReporter = textReporterBuilder
                .outputTo(new PrintStream(output))
                .build();

        consoleReporter.report();

        query.sendReply(output.toString());
      }
      return;
    }

    final SerializerReporter serializerReporter = new SerializerReporter(metricRegistry);
    query.sendReply(query.serializer().formatStatsV1(serializerReporter.report()));
  }
}
