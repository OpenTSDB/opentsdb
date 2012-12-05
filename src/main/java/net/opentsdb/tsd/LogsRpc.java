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

import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.IThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;
import ch.qos.logback.core.read.CyclicBufferAppender;

import net.opentsdb.core.TSDB;

/** The "/logs" endpoint. */
final class LogsRpc implements HttpRpc {

  public void execute(final TSDB tsdb, final HttpQuery query) {
    LogIterator logmsgs = new LogIterator();
    if (query.hasQueryStringParam("json")) {
      query.sendJsonArray(logmsgs);
    } else if (query.hasQueryStringParam("level")) {
      final Level level = Level.toLevel(query.getQueryStringParam("level"),
                                        null);
      if (level == null) {
        throw new BadRequestException("Invalid level: "
                                      + query.getQueryStringParam("level"));
      }
      final Logger root =
        (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      String logger_name = query.getQueryStringParam("logger");
      if (logger_name == null) {
        logger_name = Logger.ROOT_LOGGER_NAME;
      } else if (root.getLoggerContext().exists(logger_name) == null) {
        throw new BadRequestException("Invalid logger: " + logger_name);
      }
      final Logger logger = (Logger) LoggerFactory.getLogger(logger_name);
      int nloggers = 0;
      if (logger == root) {  // Update all the loggers.
        for (final Logger l : logger.getLoggerContext().getLoggerList()) {
          l.setLevel(level);
          nloggers++;
        }
      } else {
        logger.setLevel(level);
        nloggers++;
      }
      query.sendReply("Set the log level to " + level + " on " + nloggers
                      + " logger" + (nloggers > 1 ? "s" : "") + ".\n");
    } else {
      final StringBuilder buf = new StringBuilder(512);
      for (final String logmsg : logmsgs) {
        buf.append(logmsg).append('\n');
      }
      logmsgs = null;
      query.sendReply(buf);
    }
  }

  /** Helper class to iterate over logback's recent log messages. */
  private static final class LogIterator implements Iterator<String>,
                                                    Iterable<String> {

    private final CyclicBufferAppender<ILoggingEvent> logbuf;
    private final StringBuilder buf = new StringBuilder(64);
    private int nevents;

    public LogIterator() {
      final Logger root =
        (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
      logbuf = (CyclicBufferAppender<ILoggingEvent>) root.getAppender("CYCLIC");
    }

    public Iterator<String> iterator() {
      nevents = logbuf.getLength();
      return this;
    }

    public boolean hasNext() {
      return nevents > 0;
    }

    public String next() {
      if (hasNext()) {
        nevents--;
        final ILoggingEvent event = (ILoggingEvent) logbuf.get(nevents);
        final String msg = event.getFormattedMessage();
        buf.setLength(0);
        buf.append(event.getTimeStamp() / 1000)
          .append('\t').append(event.getLevel().toString())
          .append('\t').append(event.getThreadName())
          .append('\t').append(event.getLoggerName())
          .append('\t').append(msg);
        final IThrowableProxy thrown = event.getThrowableProxy();
        if (thrown != null) {
          buf.append('\t').append(ThrowableProxyUtil.asString(thrown));
        }
        return buf.toString();
      }
      throw new NoSuchElementException("no more elements");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
