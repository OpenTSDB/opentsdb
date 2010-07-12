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

import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.CyclicBufferAppender;

import net.opentsdb.core.TSDB;

/** The "/logs" endpoint. */
final class LogsRpc implements HttpRpc {

  public void execute(final TSDB tsdb, final HttpQuery query) {
    LogIterator logmsgs = new LogIterator();
    if (query.hasQueryStringParam("json")) {
      query.sendJsonArray(logmsgs);
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
          .append('\t').append(msg.replace("\"", "\\\""));
        return buf.toString();
      }
      throw new NoSuchElementException("no more elements");
    }

    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
