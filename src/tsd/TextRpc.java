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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;

import net.opentsdb.BuildData;
import net.opentsdb.core.TSDB;
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
   * Constructor.
   * @param tsdb The TSDB to use.
   */
  public TextRpc(final TSDB tsdb) {
    this.tsdb = tsdb;

    commands = new HashMap<String, Command>(5);
    commands.put("diediedie", new DieDieDie());
    commands.put("help", new Help());
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
