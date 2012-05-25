// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpRequest;

import net.opentsdb.BuildData;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * Stateless handler for RPCs (telnet-style or HTTP).
 */
final class RpcHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(RpcHandler.class);

  private static final AtomicLong telnet_rpcs_received = new AtomicLong();
  private static final AtomicLong http_rpcs_received = new AtomicLong();
  private static final AtomicLong exceptions_caught = new AtomicLong();

  /** Commands we can serve on the simple, telnet-style RPC interface. */
  private final HashMap<String, TelnetRpc> telnet_commands;
  /** RPC executed when there's an unknown telnet-style command. */
  private final TelnetRpc unknown_cmd = new Unknown();
  /** Commands we serve on the HTTP interface. */
  private final HashMap<String, HttpRpc> http_commands;

  /** The TSDB to use. */
  private final TSDB tsdb;

  /**
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public RpcHandler(final TSDB tsdb) {
    this.tsdb = tsdb;

    telnet_commands = new HashMap<String, TelnetRpc>(7);
    http_commands = new HashMap<String, HttpRpc>(11);
    {
      final DieDieDie diediedie = new DieDieDie();
      telnet_commands.put("diediedie", diediedie);
      http_commands.put("diediedie", diediedie);
    }
    {
      final StaticFileRpc staticfile = new StaticFileRpc();
      http_commands.put("favicon.ico", staticfile);
      http_commands.put("s", staticfile);
    }
    {
      final Stats stats = new Stats();
      telnet_commands.put("stats", stats);
      http_commands.put("stats", stats);
    }
    {
      final Version version = new Version();
      telnet_commands.put("version", version);
      http_commands.put("version", version);
    }
    {
      final DropCaches dropcaches = new DropCaches();
      telnet_commands.put("dropcaches", dropcaches);
      http_commands.put("dropcaches", dropcaches);
    }

    telnet_commands.put("exit", new Exit());
    telnet_commands.put("help", new Help());
    telnet_commands.put("put", new PutDataPointRpc());

    http_commands.put("", new HomePage());
    http_commands.put("aggregators", new ListAggregators());
    http_commands.put("logs", new LogsRpc());
    http_commands.put("q", new GraphHandler());
    http_commands.put("suggest", new Suggest());
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final MessageEvent msgevent) {
    try {
      final Object message = msgevent.getMessage();
      if (message instanceof String[]) {
        handleTelnetRpc(msgevent.getChannel(), (String[]) message);
      } else if (message instanceof HttpRequest) {
        handleHttpQuery(msgevent.getChannel(), (HttpRequest) message);
      } else {
        logError(msgevent.getChannel(), "Unexpected message type "
                 + message.getClass() + ": " + message);
        exceptions_caught.incrementAndGet();
      }
    } catch (Exception e) {
      Object pretty_message = msgevent.getMessage();
      if (pretty_message instanceof String[]) {
        pretty_message = Arrays.toString((String[]) pretty_message);
      }
      logError(msgevent.getChannel(), "Unexpected exception caught"
               + " while serving " + pretty_message, e);
      exceptions_caught.incrementAndGet();
    }
  }

  /**
   * Finds the right handler for a telnet-style RPC and executes it.
   * @param chan The channel on which the RPC was received.
   * @param command The split telnet-style command.
   */
  private void handleTelnetRpc(final Channel chan, final String[] command) {
    TelnetRpc rpc = telnet_commands.get(command[0]);
    if (rpc == null) {
      rpc = unknown_cmd;
    }
    telnet_rpcs_received.incrementAndGet();
    rpc.execute(tsdb, chan, command);
  }

  /**
   * Finds the right handler for an HTTP query and executes it.
   * @param chan The channel on which the query was received.
   * @param req The parsed HTTP request.
   */
  private void handleHttpQuery(final Channel chan, final HttpRequest req) {
    http_rpcs_received.incrementAndGet();
    final HttpQuery query = new HttpQuery(req, chan);
    if (req.isChunked()) {
      logError(query, "Received an unsupported chunked request: "
               + query.request());
      query.badRequest("Chunked request not supported.");
      return;
    }
    try {
      final HttpRpc rpc = http_commands.get(getEndPoint(query));
      if (rpc != null) {
        rpc.execute(tsdb, query);
      } else {
        query.notFound();
      }
    } catch (BadRequestException ex) {
      query.badRequest(ex.getMessage());
    } catch (Exception ex) {
      query.internalError(ex);
      exceptions_caught.incrementAndGet();
    }
  }

  /**
   * Returns the "first path segment" in the URI.
   *
   * Examples:
   * <pre>
   *   URI request | Value returned
   *   ------------+---------------
   *   /           | ""
   *   /foo        | "foo"
   *   /foo/bar    | "foo"
   *   /foo?quux   | "foo"
   * </pre>
   * @param query The HTTP query.
   */
  private String getEndPoint(final HttpQuery query) {
    final String uri = query.request().getUri();
    if (uri.length() < 1) {
      throw new BadRequestException("Empty query");
    }
    if (uri.charAt(0) != '/') {
      throw new BadRequestException("Query doesn't start with a slash: <code>"
                                    // TODO(tsuna): HTML escape to avoid XSS.
                                    + uri + "</code>");
    }
    final int questionmark = uri.indexOf('?', 1);
    final int slash = uri.indexOf('/', 1);
    int pos;  // Will be set to where the first path segment ends.
    if (questionmark > 0) {
      if (slash > 0) {
        pos = (questionmark < slash
               ? questionmark       // Request: /foo?bar/quux
               : slash);            // Request: /foo/bar?quux
      } else {
        pos = questionmark;         // Request: /foo?bar
      }
    } else {
      pos = (slash > 0
             ? slash                // Request: /foo/bar
             : uri.length());       // Request: /foo
    }
    return uri.substring(1, pos);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("rpc.received", telnet_rpcs_received, "type=telnet");
    collector.record("rpc.received", http_rpcs_received, "type=http");
    collector.record("rpc.exceptions", exceptions_caught);
    HttpQuery.collectStats(collector);
    GraphHandler.collectStats(collector);
    PutDataPointRpc.collectStats(collector);
  }

  // ---------------------------- //
  // Individual command handlers. //
  // ---------------------------- //

  /** The "diediedie" command and "/diediedie" endpoint. */
  private final class DieDieDie implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      logWarn(chan, "shutdown requested");
      chan.write("Cleaning up and exiting now.\n");
      return doShutdown(tsdb, chan);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) {
      logWarn(query, "shutdown requested");
      query.sendReply(HttpQuery.makePage("TSD Exiting", "You killed me",
                                         "Cleaning up and exiting now."));
      doShutdown(tsdb, query.channel());
    }

    private Deferred<Object> doShutdown(final TSDB tsdb, final Channel chan) {
      ((GraphHandler) http_commands.get("q")).shutdown();
      ConnectionManager.closeAllConnections();
      // Netty gets stuck in an infinite loop if we shut it down from within a
      // NIO thread.  So do this from a newly created thread.
      final class ShutdownNetty extends Thread {
        ShutdownNetty() {
          super("ShutdownNetty");
        }
        public void run() {
          chan.getFactory().releaseExternalResources();
        }
      }
      new ShutdownNetty().start();  // Stop accepting new connections.

      // Log any error that might occur during shutdown.
      final class ShutdownTSDB implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          LOG.error("Unexpected exception while shutting down", arg);
          return arg;
        }
        public String toString() {
          return "shutdown callback";
        }
      }
      return tsdb.shutdown().addErrback(new ShutdownTSDB());
    }
  }

  /** The "exit" command. */
  private static final class Exit implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      chan.disconnect();
      return Deferred.fromResult(null);
    }
  }

  /** The "help" command. */
  private final class Help implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      final StringBuilder buf = new StringBuilder();
      buf.append("available commands: ");
      // TODO(tsuna): Maybe sort them?
      for (final String command : telnet_commands.keySet()) {
        buf.append(command).append(' ');
      }
      buf.append('\n');
      chan.write(buf.toString());
      return Deferred.fromResult(null);
    }
  }

  /** The home page ("GET /"). */
  private static final class HomePage implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query) {
      final StringBuilder buf = new StringBuilder(2048);
      buf.append("<div id=queryuimain></div>"
                 + "<noscript>You must have JavaScript enabled.</noscript>"
                 + "<iframe src=javascript:'' id=__gwt_historyFrame tabIndex=-1"
                 + " style=position:absolute;width:0;height:0;border:0>"
                 + "</iframe>");
      query.sendReply(HttpQuery.makePage(
        "<script type=text/javascript language=javascript"
        + " src=/s/queryui.nocache.js></script>",
        "TSD", "Time Series Database", buf.toString()));
    }
  }

  /** The "/aggregators" endpoint. */
  private static final class ListAggregators implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query) {
      query.sendJsonArray(Aggregators.set());
    }
  }

  /** The "stats" command and the "/stats" endpoint. */
  private static final class Stats implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      final StringBuilder buf = new StringBuilder(1024);
      final StatsCollector collector = new StatsCollector("tsd") {
        @Override
        public final void emit(final String line) {
          buf.append(line);
        }
      };
      doCollectStats(tsdb, collector);
      chan.write(buf.toString());
      return Deferred.fromResult(null);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) {
      final boolean json = query.hasQueryStringParam("json");
      final StringBuilder buf = json ? null : new StringBuilder(2048);
      final ArrayList<String> stats = json ? new ArrayList<String>(64) : null;
      final StatsCollector collector = new StatsCollector("tsd") {
        @Override
        public final void emit(final String line) {
          if (json) {
            stats.add(line.substring(0, line.length() - 1));  // strip the '\n'
          } else {
            buf.append(line);
          }
        }
      };
      doCollectStats(tsdb, collector);
      if (json) {
        query.sendJsonArray(stats);
      } else {
        query.sendReply(buf);
      }
    }

    private void doCollectStats(final TSDB tsdb,
                                final StatsCollector collector) {
      collector.addHostTag();
      ConnectionManager.collectStats(collector);
      RpcHandler.collectStats(collector);
      tsdb.collectStats(collector);
    }
  }

  /** The "/suggest" endpoint. */
  private static final class Suggest implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query) {
      final String type = query.getRequiredQueryStringParam("type");
      final String q = query.getQueryStringParam("q");
      if (q == null) {
        throw BadRequestException.missingParameter("q");
      }
      List<String> suggestions;
      if ("metrics".equals(type)) {
        suggestions = tsdb.suggestMetrics(q);
      } else if ("tagk".equals(type)) {
        suggestions = tsdb.suggestTagNames(q);
      } else if ("tagv".equals(type)) {
        suggestions = tsdb.suggestTagValues(q);
      } else {
        throw new BadRequestException("Invalid 'type' parameter:" + type);
      }
      query.sendJsonArray(suggestions);
    }
  }

  /** For unknown commands. */
  private static final class Unknown implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      logWarn(chan, "unknown command : " + Arrays.toString(cmd));
      chan.write("unknown command: " + cmd[0] + ".  Try `help'.\n");
      return Deferred.fromResult(null);
    }
  }

  /** The "version" command. */
  private static final class Version implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      if (chan.isConnected()) {
        chan.write(BuildData.revisionString() + '\n'
                   + BuildData.buildString() + '\n');
      }
      return Deferred.fromResult(null);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) {
      final boolean json = query.request().getUri().endsWith("json");
      StringBuilder buf;
      if (json) {
        buf = new StringBuilder(157 + BuildData.repo_status.toString().length()
                                + BuildData.user.length() + BuildData.host.length()
                                + BuildData.repo.length());
        buf.append("{\"short_revision\":\"").append(BuildData.short_revision)
          .append("\",\"full_revision\":\"").append(BuildData.full_revision)
          .append("\",\"timestamp\":").append(BuildData.timestamp)
          .append(",\"repo_status\":\"").append(BuildData.repo_status)
          .append("\",\"user\":\"").append(BuildData.user)
          .append("\",\"host\":\"").append(BuildData.host)
          .append("\",\"repo\":\"").append(BuildData.repo)
          .append("\"}");
      } else {
        final String revision = BuildData.revisionString();
        final String build = BuildData.buildString();
        buf = new StringBuilder(2 // For the \n's
                                + revision.length() + build.length());
        buf.append(revision).append('\n').append(build).append('\n');
      }
      query.sendReply(buf);
    }
  }

  /**
   * Returns the directory path stored in the given system property.
   * @param prop The name of the system property.
   * @return The directory path.
   * @throws IllegalStateException if the system property is not set
   * or has an invalid value.
   */
  static String getDirectoryFromSystemProp(final String prop) {
    final String dir = System.getProperty(prop);
    String err = null;
    if (dir == null) {
      err = "' is not set.";
    } else if (dir.isEmpty()) {
      err = "' is empty.";
    } else if (dir.charAt(dir.length() - 1) != '/') {  // Screw Windows.
      err = "' is not terminated with `/'.";
    }
    if (err != null) {
      throw new IllegalStateException("System property `" + prop + err);
    }
    return dir;
  }

  /** The "dropcaches" command. */
  private static final class DropCaches implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      dropCaches(tsdb, chan);
      chan.write("Caches dropped.\n");
      return Deferred.fromResult(null);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) {
      dropCaches(tsdb, query.channel());
      query.sendReply("Caches dropped.\n");
    }

    /** Drops in memory caches.  */
    private void dropCaches(final TSDB tsdb, final Channel chan) {
      LOG.warn(chan + " Dropping all in-memory caches.");
      tsdb.dropCaches();
    }
  }


  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  //private static void logInfo(final HttpQuery query, final String msg) {
  //  LOG.info(query.channel().toString() + ' ' + msg);
  //}

  private static void logWarn(final HttpQuery query, final String msg) {
    LOG.warn(query.channel().toString() + ' ' + msg);
  }

  //private void logWarn(final HttpQuery query, final String msg,
  //                     final Exception e) {
  //  LOG.warn(query.channel().toString() + ' ' + msg, e);
  //}

  private void logError(final HttpQuery query, final String msg) {
    LOG.error(query.channel().toString() + ' ' + msg);
  }

  //private static void logError(final HttpQuery query, final String msg,
  //                             final Exception e) {
  //  LOG.error(query.channel().toString() + ' ' + msg, e);
  //}

  //private void logInfo(final Channel chan, final String msg) {
  //  LOG.info(chan.toString() + ' ' + msg);
  //}

  private static void logWarn(final Channel chan, final String msg) {
    LOG.warn(chan.toString() + ' ' + msg);
  }

  //private void logWarn(final Channel chan, final String msg, final Exception e) {
  //  LOG.warn(chan.toString() + ' ' + msg, e);
  //}

  private void logError(final Channel chan, final String msg) {
    LOG.error(chan.toString() + ' ' + msg);
  }

  private void logError(final Channel chan, final String msg, final Exception e) {
    LOG.error(chan.toString() + ' ' + msg, e);
  }

}
