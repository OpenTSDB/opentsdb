// This file is part of OpenTSDB.
// Copyright (C) 2010  StumbleUpon, Inc.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.handler.codec.http.HttpRequest;

import net.opentsdb.BuildData;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;

/**
 * Stateless handler for HTTP requests.
 */
final class HttpHandler extends SimpleChannelUpstreamHandler {

  private static final Logger LOG =
    LoggerFactory.getLogger(HttpHandler.class);

  private static final AtomicInteger queries_received
    = new AtomicInteger();
  private static final AtomicInteger exceptions_caught
    = new AtomicInteger();

  /** The TSDB to use. */
  private final TSDB tsdb;

  /** Command we can serve on the simple, text-based RPC interface. */
  private final HashMap<String, Command> commands;

  /**
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public HttpHandler(final TSDB tsdb) {
    this.tsdb = tsdb;

    final StaticFile staticfile = new StaticFile();
    commands = new HashMap<String, Command>(10);
    commands.put("", new HomePage());
    commands.put("aggregators", new ListAggregators());
    commands.put("diediedie", new DieDieDie());
    commands.put("favicon.ico", staticfile);
    commands.put("q", new GraphHandler(tsdb));
    commands.put("s", staticfile);
    commands.put("stats", new Stats());
    commands.put("version", new Version());
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final MessageEvent e) {
    final HttpQuery query = new HttpQuery((HttpRequest) e.getMessage(),
                                          e.getChannel());
    try {
      if (query.request().isChunked()) {
        logError(query, "Received an unsupported chunked request: "
                 + query.request());
        throw new BadRequestException("Chunked request not supported.");
      }
      final Command cmd = commands.get(getEndPoint(query));
      if (cmd != null) {
        cmd.process(query);
        logInfo(query, "HTTP " + query.request().getUri() + " done in "
                + query.processingTimeMillis() + "ms");
      } else {
        query.notFound();
      }
    } catch (BadRequestException ex) {
      query.badRequest(ex.getMessage());
    } catch (Exception ex) {
      query.internalError(ex);
      exceptions_caught.incrementAndGet();
    }
    queries_received.incrementAndGet();
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.queries", queries_received);
    collector.record("http.exceptions", exceptions_caught);
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

  // ---------------------------- //
  // Individual command handlers. //
  // ---------------------------- //

  /**
   * Interface implemented by each command.
   * All implementations are stateless.
   */
  interface Command {
    /** Processes the given HTTP query. */
    void process(HttpQuery query) throws IOException;
  }

  /** The "/diediedie" endpoint. */
  private final class DieDieDie implements Command {
    public void process(final HttpQuery query) {
      logWarn(query, "shutdown requested");
      query.sendReply(query.makePage("TSD Exiting", "You killed me",
                                     "Cleaning up and exiting now."));
      ConnectionManager.closeAllConnections();
      // Attempt to commit any data point still only in RAM.
      // TODO(tsuna): See TODO in TextRpc#DieDieDie
      tsdb.flush();
      // Netty gets stuck in an infinite loop if we shut it down from within a
      // NIO thread.  So do this from a newly created thread.
      final class ShutdownNetty extends Thread {
        ShutdownNetty() {
          super("ShutdownNetty");
        }
        public void run() {
          query.channel().getFactory().releaseExternalResources();
        }
      }
      new ShutdownNetty().start();
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

  /** The "/s" endpoint. */
  private final class StaticFile implements Command {
    /**
     * The path to the directory where to find static files
     * (for the {@code /s} URLs).
     */
    private final String staticroot;

    /**
     * Constructor.
     */
    public StaticFile() {
      staticroot = getDirectoryFromSystemProp("tsd.http.staticroot");
    }

    public void process(final HttpQuery query) throws IOException {
      final String uri = query.request().getUri();
      if ("/favicon.ico".equals(uri)) {
        query.sendFile(staticroot + "/favicon.ico");
        return;
      }
      if (uri.length() < 3) {  // Must be at least 3 because of the "/s/".
        throw new BadRequestException("URI too short <code>" + uri + "</code>");
      }
      // Cheap security check to avoid directory traversal attacks.
      // TODO(tsuna): This is certainly not sufficient.
      if (uri.indexOf("..", 3) > 0) {
        throw new BadRequestException("Malformed URI <code>" + uri + "</code>");
      }
      final int questionmark = uri.indexOf('?', 3);
      final int pathend = questionmark > 0 ? questionmark : uri.length();
      query.sendFile(staticroot + uri.substring(3, pathend));
    }
  }

  /** The home page ("GET /"). */
  private final class HomePage implements Command {
    public void process(final HttpQuery query) {
      final StringBuilder buf = new StringBuilder(2048);
      buf.append("<div id=queryuimain></div>"
                 + "<noscript>You must have JavaScript enabled.</noscript>"
                 + "<iframe src=javascript:'' id=__gwt_historyFrame tabIndex=-1"
                 + " style=position:absolute;width:0;height:0;border:0>"
                 + "</iframe>");
      query.sendReply(query.makePage(
        "<script type=text/javascript language=javascript"
        + " src=/s/queryui.nocache.js></script>",
        "TSD", "Time Series DataBase", buf.toString()));
    }
  }

  /** The "/aggregators" endpoint. */
  private final class ListAggregators implements Command {
    public void process(final HttpQuery query) {
      query.sendJsonArray(Aggregators.set());
    }
  }

  /** The "/stats" endpoint. */
  private final class Stats implements Command {
    public void process(final HttpQuery query) {
      final StringBuilder buf = new StringBuilder(2048);
      final StatsCollector collector = new StatsCollector("tsd") {
        @Override
        public final void emit(final String line) {
          buf.append(line);
        }
      };

      // TODO(tsuna): Code smell, duplicated from TextRpc#Stats, plz fix.
      collector.addHostTag();
      ConnectionManager.collectStats(collector);
      TextRpc.collectStats(collector);
      HttpHandler.collectStats(collector);
      tsdb.collectStats(collector);
      query.sendReply(buf);
    }
  }

  /** The "/version" endpoint. */
  private final class Version implements Command {
    public void process(final HttpQuery query) {
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

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  private void logInfo(final HttpQuery query, final String msg) {
    LOG.info(query.channel().toString() + ' ' + msg);
  }

  private void logWarn(final HttpQuery query, final String msg) {
    LOG.warn(query.channel().toString() + ' ' + msg);
  }

  private void logWarn(final HttpQuery query, final String msg,
                       final Exception e) {
    LOG.warn(query.channel().toString() + ' ' + msg, e);
  }

  private void logError(final HttpQuery query, final String msg) {
    LOG.error(query.channel().toString() + ' ' + msg);
  }

  private void logError(final HttpQuery query, final String msg,
                        final Exception e) {
    LOG.error(query.channel().toString() + ' ' + msg, e);
  }

}
