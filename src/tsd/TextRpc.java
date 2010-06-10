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

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import net.opentsdb.BuildData;
import net.opentsdb.HBaseException;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.Tags;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.stats.StatsCollector;

/**
 * Stateless handler for simple, text-only RPCs.
 */
final class TextRpc extends SimpleChannelUpstreamHandler {

  private static final Logger LOG = LoggerFactory.getLogger(TextRpc.class);

  private static final AtomicInteger rpcs_received
    = new AtomicInteger();
  private static final AtomicInteger exceptions_caught
    = new AtomicInteger();

  /** Command we can serve on the simple, text-based RPC interface. */
  private final HashMap<String, Command> commands;

  private final Command unknown_cmd = new Unknown();

  /** The TSDB to use. */
  private final TSDB tsdb;

  /**
   * Dirty rows for time series that are being written to.
   *
   * The key in the map is a string that uniquely identifies a time series.
   * Right now we use the time series name concatenated with the stringified
   * version of the map that stores all the tags, e.g. "foo{bar=a,quux=42}".
   *
   * TODO(tsuna): Need a shutdown hook and a fix for HBASE-2669 to ensure that
   * all dirty rows are flushed to HBase when we get a SIGTERM.  Right now a
   * SIGTERM causes data loss! XXX
   */
  private ConcurrentHashMap<String, WritableDataPoints> dirty_rows
    = new ConcurrentHashMap<String, WritableDataPoints>();

  /**
   * Returns the dirty row for the given time series or creates a new one.
   * @param metric The metric of the time series.
   * @param tags The tags of the time series.
   * @return the dirty row in which data points for the given time series
   * should be appended.
   */
  private WritableDataPoints getDirtyRow(final String metric,
                                         final HashMap<String, String> tags) {
    final String key = metric + tags;
    WritableDataPoints row = dirty_rows.get(key);
    if (row == null) {  // Try to create a new row.
      // TODO(tsuna): Properly evict old rows to save memory.
      if (dirty_rows.size() >= 20000) {
        dirty_rows.clear();  // free some RAM.
      }
      final WritableDataPoints new_row = tsdb.newDataPoints();
      new_row.setSeries(metric, tags);
      row = dirty_rows.putIfAbsent(key, new_row);
      if (row == null) {  // We've just inserted a new row.
        return new_row;   // So use that.
      }
    }
    return row;
  }

  /**
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public TextRpc(final TSDB tsdb) {
    this.tsdb = tsdb;

    commands = new HashMap<String, Command>(5);
    commands.put("diediedie", new DieDieDie());
    commands.put("help", new Help());
    commands.put("put", new Put());
    commands.put("stats", new Stats());
    commands.put("version", new Version());
  }

  @Override
  public void messageReceived(final ChannelHandlerContext ctx,
                              final MessageEvent e) {
    final String[] command = (String[]) e.getMessage();
    Command cmd = commands.get(command[0]);
    if (cmd == null) {
      cmd = unknown_cmd;
    }
    try {
      cmd.process(ctx, e);
    } catch (Exception ex) {
      logError(e.getChannel(), "Unexpected exception caught"
                + " while serving " + cmd, ex);
      exceptions_caught.incrementAndGet();
    }
    rpcs_received.incrementAndGet();
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("textrpc.rpcs", rpcs_received);
    collector.record("textrpc.exceptions", exceptions_caught);
  }

  /**
   * Interface implemented by each command.
   * All implementations are stateless.
   */
  private interface Command {

    /** Processes the given message. */
    void process(ChannelHandlerContext ctx, MessageEvent e);

  }

  /** The "diediedie" command. */
  private final class DieDieDie implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      final Channel chan = e.getChannel();
      logWarn(chan, "shutdown requested");
      chan.write("Cleaning up and exiting now.\n");
      ConnectionManager.closeAllConnections();
      // Attempt to commit any data point still only in RAM.
      // TODO(tsuna): Need a way of ensuring we don't spend more than X
      // seconds doing this.  If we're asked to die, we should do so
      // promptly.  Right now I believe we can spend an indefinite and
      // unbounded amount of time in the HBase client library.
      tsdb.flush();
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
      new ShutdownNetty().start();
    }
  }

  /** The "help" command. */
  private final class Help implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      final StringBuilder buf = new StringBuilder();
      buf.append("available commands: ");
      // TODO(tsuna): Maybe sort them?
      for (final String cmd : commands.keySet()) {
        buf.append(cmd).append(' ');
      }
      buf.append('\n');
      e.getChannel().write(buf.toString());
    }
  }

  /** The "put" command. */
  private final class Put implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      try {
        importDataPoint((String[]) e.getMessage());
      } catch (NumberFormatException x) {
        e.getChannel().write("put: invalid value: " + x.getMessage() + '\n');
      } catch (IllegalArgumentException x) {
        e.getChannel().write("put: illegal argument: " + x.getMessage() + '\n');
      } catch (HBaseException x) {
        e.getChannel().write("put: HBase error: " + x.getMessage() + '\n');
      } catch (NoSuchUniqueName x) {
        e.getChannel().write("put: unknown metric: " + x.getMessage() + '\n');
      }
    }

    /**
     * Imports a single data point.
     * @param words The words describing the data point to import, in
     * the following format: {@code [metric, timestamp, value, ..tags..]}
     * @throws NumberFormatException if the timestamp or value is invalid.
     * @throws IllegalArgumentException if any other argument is invalid.
     * @throws HBaseException if there's a problem when writing to HBase.
     * @throws NoSuchUniqueName if the metric isn't registered.
     */
    private void importDataPoint(final String[] words) {
      words[0] = null; // Ditch the "put".
      if (words.length < 5) {  // Need at least: metric timestamp value tag
        //               ^ 5 and not 4 because words[0] is "put".
        throw new IllegalArgumentException("not enough arguments"
            + " (need least 4, got " + (words.length - 1) + ')');
      }
      final String metric = words[1];
      if (metric.length() <= 0) {
        throw new IllegalArgumentException("empty metric name");
      }
      final long timestamp = Long.parseLong(words[2]);
      if (timestamp <= 0) {
        throw new IllegalArgumentException("invalid timestamp: " + timestamp);
      }
      final String value = words[3];
      if (value.length() <= 0) {
        throw new IllegalArgumentException("empty value");
      }
      final HashMap<String, String> tags = new HashMap<String, String>();
      for (int i = 4; i < words.length; i++) {
        if (!words[i].isEmpty()) {
          Tags.parse(tags, words[i]);
        }
      }
      final WritableDataPoints dp = getDirtyRow(metric, tags);
      if (value.indexOf('.') < 0) {  // integer value
        dp.addPoint(timestamp, Long.parseLong(value));
      } else {  // floating point value
        dp.addPoint(timestamp, Float.parseFloat(value));
      }
    }
  }

  /** The "stats" command. */
  private final class Stats implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      final Channel chan = e.getChannel();
      final StringBuilder buf = new StringBuilder(1024);
      final StatsCollector collector = new StatsCollector("tsd") {
        @Override
        public final void emit(final String line) {
          buf.append(line);
        }
      };

      collector.addHostTag();
      ConnectionManager.collectStats(collector);
      TextRpc.collectStats(collector);
      HttpHandler.collectStats(collector);
      tsdb.collectStats(collector);
      chan.write(buf.toString());
    }
  }

  /** For unknown commands. */
  private final class Unknown implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      final String[] command = (String[]) e.getMessage();
      final Channel chan = e.getChannel();
      logWarn(chan, "unknown command : " + Arrays.toString(command));
      chan.write("unknown command: " + command[0] + ".  Try `help'.\n");
    }
  }

  /** The "version" command. */
  private final class Version implements Command {
    public void process(final ChannelHandlerContext ctx,
                        final MessageEvent e) {
      final Channel chan = e.getChannel();
      if (chan.isConnected()) {
        chan.write(BuildData.revisionString() + '\n'
                   + BuildData.buildString() + '\n');
      }
    }
  }

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  private void logInfo(final Channel chan, final String msg) {
    LOG.info(chan.toString() + ' ' + msg);
  }

  private void logWarn(final Channel chan, final String msg) {
    LOG.warn(chan.toString() + ' ' + msg);
  }

  private void logWarn(final Channel chan, final String msg, final Exception e) {
    LOG.warn(chan.toString() + ' ' + msg, e);
  }

  private void logError(final Channel chan, final String msg) {
    LOG.error(chan.toString() + ' ' + msg);
  }

  private void logError(final Channel chan, final String msg, final Exception e) {
    LOG.error(chan.toString() + ' ' + msg, e);
  }

}
