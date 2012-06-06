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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.stumbleupon.async.Deferred;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.handler.codec.http.QueryStringDecoder;
import org.jboss.netty.util.CharsetUtil;

import net.opentsdb.core.Const;
import net.opentsdb.graph.Plot;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;

/**
 * Binds together an HTTP request and the channel on which it was received.
 *
 * It makes it easier to provide a few utility methods to respond to the
 * requests.
 */
final class HttpQuery {

  private static final Logger LOG = LoggerFactory.getLogger(HttpQuery.class);

  private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  /**
   * Keep track of the latency of HTTP requests.
   */
  private static final Histogram httplatency =
    new Histogram(16000, (short) 2, 100);

  /** When the query was started (useful for timing). */
  private final long start_time = System.nanoTime();

  /** The request in this HTTP query. */
  private final HttpRequest request;

  /** The channel on which the request was received. */
  private final Channel chan;

  /** Parsed query string (lazily built on first access). */
  private Map<String, List<String>> querystring;

  /** Deferred result of this query, to allow asynchronous processing.  */
  private final Deferred<Object> deferred = new Deferred<Object>();

  /**
   * Constructor.
   * @param request The request in this HTTP query.
   * @param chan The channel on which the request was received.
   */
  public HttpQuery(final HttpRequest request, final Channel chan) {
    this.request = request;
    this.chan = chan;
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.latency", httplatency, "type=all");
  }

  /**
   * Returns the underlying Netty {@link HttpRequest} of this query.
   */
  public HttpRequest request() {
    return request;
  }

  /**
   * Returns the underlying Netty {@link Channel} of this query.
   */
  public Channel channel() {
    return chan;
  }

  /**
   * Return the {@link Deferred} associated with this query.
   */
  public Deferred<Object> getDeferred() {
    return deferred;
  }

  /** Returns how many ms have elapsed since this query was created. */
  public int processingTimeMillis() {
    return (int) ((System.nanoTime() - start_time) / 1000000);
  }

  /**
   * Returns the query string parameters passed in the URI.
   */
  public Map<String, List<String>> getQueryString() {
    if (querystring == null) {
      try {
        querystring = new QueryStringDecoder(request.getUri()).getParameters();
      } catch (IllegalArgumentException e) {
        throw new BadRequestException("Bad query string: " + e.getMessage());
      }
    }
    return querystring;
  }

  /**
   * Returns the value of the given query string parameter.
   * <p>
   * If this parameter occurs multiple times in the URL, only the last value
   * is returned and others are silently ignored.
   * @param paramname Name of the query string parameter to get.
   * @return The value of the parameter or {@code null} if this parameter
   * wasn't passed in the URI.
   */
  public String getQueryStringParam(final String paramname) {
    final List<String> params = getQueryString().get(paramname);
    return params == null ? null : params.get(params.size() - 1);
  }

  /**
   * Returns the non-empty value of the given required query string parameter.
   * <p>
   * If this parameter occurs multiple times in the URL, only the last value
   * is returned and others are silently ignored.
   * @param paramname Name of the query string parameter to get.
   * @return The value of the parameter.
   * @throws BadRequestException if this query string parameter wasn't passed
   * or if its last occurrence had an empty value ({@code &amp;a=}).
   */
  public String getRequiredQueryStringParam(final String paramname)
    throws BadRequestException {
    final String value = getQueryStringParam(paramname);
    if (value == null || value.isEmpty()) {
      throw BadRequestException.missingParameter(paramname);
    }
    return value;
  }

  /**
   * Returns whether or not the given query string parameter was passed.
   * @param paramname Name of the query string parameter to get.
   * @return {@code true} if the parameter
   */
  public boolean hasQueryStringParam(final String paramname) {
    return getQueryString().get(paramname) != null;
  }

  /**
   * Returns all the values of the given query string parameter.
   * <p>
   * In case this parameter occurs multiple times in the URL, this method is
   * useful to get all the values.
   * @param paramname Name of the query string parameter to get.
   * @return The values of the parameter or {@code null} if this parameter
   * wasn't passed in the URI.
   */
  public List<String> getQueryStringParams(final String paramname) {
    return getQueryString().get(paramname);
  }

  /**
   * Sends a 500 error page to the client.
   * @param cause The unexpected exception that caused this error.
   */
  public void internalError(final Exception cause) {
    ThrowableProxy tp = new ThrowableProxy(cause);
    tp.calculatePackagingData();
    final String pretty_exc = ThrowableProxyUtil.asString(tp);
    tp = null;
    if (hasQueryStringParam("json")) {
      // 32 = 10 + some extra space as exceptions always have \t's to escape.
      final StringBuilder buf = new StringBuilder(32 + pretty_exc.length());
      buf.append("{\"err\":\"");
      HttpQuery.escapeJson(pretty_exc, buf);
      buf.append("\"}");
      sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR, buf);
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.INTERNAL_SERVER_ERROR, pretty_exc, 30);
    } else {
      sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR,
                makePage("Internal Server Error", "Houston, we have a problem",
                         "<blockquote>"
                         + "<h1>Internal Server Error</h1>"
                         + "Oops, sorry but your request failed due to a"
                         + " server error.<br/><br/>"
                         + "Please try again in 30 seconds.<pre>"
                         + pretty_exc
                         + "</pre></blockquote>"));
    }
    logError("Internal Server Error on " + request.getUri(), cause);
  }

  /**
   * Sends a 400 error page to the client.
   * @param explain The string describing why the request is bad.
   */
  public void badRequest(final String explain) {
    if (hasQueryStringParam("json")) {
      final StringBuilder buf = new StringBuilder(10 + explain.length());
      buf.append("{\"err\":\"");
      HttpQuery.escapeJson(explain, buf);
      buf.append("\"}");
      sendReply(HttpResponseStatus.BAD_REQUEST, buf);
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.BAD_REQUEST, explain, 3600);
    } else {
      sendReply(HttpResponseStatus.BAD_REQUEST,
                makePage("Bad Request", "Looks like it's your fault this time",
                         "<blockquote>"
                         + "<h1>Bad Request</h1>"
                         + "Sorry but your request was rejected as being"
                         + " invalid.<br/><br/>"
                         + "The reason provided was:<blockquote>"
                         + explain
                         + "</blockquote></blockquote>"));
    }
    logWarn("Bad Request on " + request.getUri() + ": " + explain);
  }

  /** Sends a 404 error page to the client. */
  public void notFound() {
    logWarn("Not Found: " + request.getUri());
    if (hasQueryStringParam("json")) {
      sendReply(HttpResponseStatus.NOT_FOUND,
                new StringBuilder("{\"err\":\"Page Not Found\"}"));
    } else if (hasQueryStringParam("png")) {
      sendAsPNG(HttpResponseStatus.NOT_FOUND, "Page Not Found", 3600);
    } else {
      sendReply(HttpResponseStatus.NOT_FOUND, PAGE_NOT_FOUND);
    }
  }

  /** Redirects the client's browser to the given location.  */
  public void redirect(final String location) {
    // TODO(tsuna): We currently redirect with some HTML because `sendReply'
    // doesn't easily allow us to pass a `Location' header, which is lame.
    sendReply(HttpResponseStatus.OK,
              makePage("<meta http-equiv=\"refresh\" content=\"0; url="
                       + location + "\">",
                       "Redirecting...", "Redirecting...", "Loading..."));
  }

  /** An empty JSON array ready to be sent. */
  private static final byte[] EMPTY_JSON_ARRAY = new byte[] { '[', ']' };

  /**
   * Sends the given sequence of strings as a JSON array.
   * @param strings A possibly empty sequence of strings.
   */
  public void sendJsonArray(final Iterable<String> strings) {
    int nstrings = 0;
    int sz = 0;  // Pre-compute the buffer size to avoid re-allocations.
    for (final String string : strings) {
      sz += string.length();
      nstrings++;
    }
    if (nstrings == 0) {
      sendReply(EMPTY_JSON_ARRAY);
      return;
    }
    final StringBuilder buf = new StringBuilder(sz // All the strings
                                                + nstrings * 3  // "",
                                                + 1);  // Leading `['
    toJsonArray(strings, buf);
    sendReply(buf);
  }

  /**
   * Escapes a string appropriately to be a valid in JSON.
   * Valid JSON strings are defined in RFC 4627, Section 2.5.
   * @param s The string to escape, which is assumed to be in .
   * @param buf The buffer into which to write the escaped string.
   */
  static void escapeJson(final String s, final StringBuilder buf) {
    final int length = s.length();
    int extra = 0;
    // First count how many extra chars we'll need, if any.
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':
        case '\\':
        case '\b':
        case '\f':
        case '\n':
        case '\r':
        case '\t':
          extra++;
          continue;
      }
      if (c < 0x001F) {
        extra += 4;
      }
    }
    if (extra == 0) {
      buf.append(s);  // Nothing to escape.
      return;
    }
    buf.ensureCapacity(buf.length() + length + extra);
    for (int i = 0; i < length; i++) {
      final char c = s.charAt(i);
      switch (c) {
        case '"':  buf.append('\\').append('"');  continue;
        case '\\': buf.append('\\').append('\\'); continue;
        case '\b': buf.append('\\').append('b');  continue;
        case '\f': buf.append('\\').append('f');  continue;
        case '\n': buf.append('\\').append('n');  continue;
        case '\r': buf.append('\\').append('r');  continue;
        case '\t': buf.append('\\').append('t');  continue;
      }
      if (c < 0x001F) {
        buf.append('\\').append('u').append('0').append('0')
          .append((char) Const.HEX[(c >>> 4) & 0x0F])
          .append((char) Const.HEX[c & 0x0F]);
      } else {
        buf.append(c);
      }
    }
  }

  /**
   * Transforms a non-empty sequence of strings into a JSON array.
   * The behavior of this method is undefined if the input sequence is empty.
   * @param strings The strings to transform into a JSON array.
   * @param buf The buffer where to write the JSON array.
   */
  public static void toJsonArray(final Iterable<String> strings,
                                 final StringBuilder buf) {
    buf.append('[');
    for (final String string : strings) {
      buf.append('"');
      escapeJson(string, buf);
      buf.append("\",");
    }
    buf.setCharAt(buf.length() - 1, ']');
  }

  /**
   * Sends data in an HTTP "200 OK" reply to the client.
   * @param data Raw byte array to send as-is after the HTTP headers.
   */
  public void sendReply(final byte[] data) {
    sendBuffer(HttpResponseStatus.OK, ChannelBuffers.wrappedBuffer(data));
  }

  /**
   * Sends an HTTP reply to the client.
   * <p>
   * This is equivalent of
   * <code>{@link sendReply(HttpResponseStatus, StringBuilder)
   * sendReply}({@link HttpResponseStatus#OK
   * HttpResponseStatus.OK}, buf)</code>
   * @param buf The content of the reply to send.
   */
  public void sendReply(final StringBuilder buf) {
    sendReply(HttpResponseStatus.OK, buf);
  }

  /**
   * Sends an HTTP reply to the client.
   * <p>
   * This is equivalent of
   * <code>{@link sendReply(HttpResponseStatus, StringBuilder)
   * sendReply}({@link HttpResponseStatus#OK
   * HttpResponseStatus.OK}, buf)</code>
   * @param buf The content of the reply to send.
   */
  public void sendReply(final String buf) {
    sendBuffer(HttpResponseStatus.OK,
               ChannelBuffers.copiedBuffer(buf, CharsetUtil.UTF_8));
  }

  /**
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  public void sendReply(final HttpResponseStatus status,
                        final StringBuilder buf) {
    sendBuffer(status, ChannelBuffers.copiedBuffer(buf.toString(),
                                                   CharsetUtil.UTF_8));
  }

  /**
   * Sends the given message as a PNG image.
   * <strong>This method will block</strong> while image is being generated.
   * It's only recommended for cases where we want to report an error back to
   * the user and the user's browser expects a PNG image.  Don't abuse it.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param msg The message to send as an image.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendAsPNG(final HttpResponseStatus status,
                        final String msg,
                        final int max_age) {
    try {
      final long now = System.currentTimeMillis() / 1000;
      Plot plot = new Plot(now - 1, now);
      HashMap<String, String> params = new HashMap<String, String>(1);
      StringBuilder buf = new StringBuilder(1 + msg.length() + 18);

      buf.append('"');
      escapeJson(msg, buf);
      buf.append("\" at graph 0.02,0.97");
      params.put("label", buf.toString());
      buf = null;
      plot.setParams(params);
      params = null;
      final String basepath =
        RpcHandler.getDirectoryFromSystemProp("tsd.http.cachedir")
        + Integer.toHexString(msg.hashCode());
      GraphHandler.runGnuplot(this, basepath, plot);
      plot = null;
      sendFile(status, basepath + ".png", max_age);
    } catch (Exception e) {
      getQueryString().remove("png");  // Avoid recursion.
      internalError(new RuntimeException("Failed to generate a PNG with the"
                                         + " following message: " + msg, e));
    }
  }

  /**
   * Send a file (with zero-copy) to the client with a 200 OK status.
   * This method doesn't provide any security guarantee.  The caller is
   * responsible for the argument they pass in.
   * @param path The path to the file to send to the client.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendFile(final String path,
                       final int max_age) throws IOException {
    sendFile(HttpResponseStatus.OK, path, max_age);
  }

  /**
   * Send a file (with zero-copy) to the client.
   * This method doesn't provide any security guarantee.  The caller is
   * responsible for the argument they pass in.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param path The path to the file to send to the client.
   * @param max_age The expiration time of this entity, in seconds.  This is
   * not a timestamp, it's how old the resource is allowed to be in the client
   * cache.  See RFC 2616 section 14.9 for more information.  Use 0 to disable
   * caching.
   */
  public void sendFile(final HttpResponseStatus status,
                       final String path,
                       final int max_age) throws IOException {
    if (max_age < 0) {
      throw new IllegalArgumentException("Negative max_age=" + max_age
                                         + " for path=" + path);
    }
    if (!chan.isConnected()) {
      done();
      return;
    }
    RandomAccessFile file;
    try {
      file = new RandomAccessFile(path, "r");
    } catch (FileNotFoundException e) {
      logWarn("File not found: " + e.getMessage());
      if (querystring != null) {
        querystring.remove("png");  // Avoid potential recursion.
      }
      notFound();
      return;
    }
    final long length = file.length();
    {
      final DefaultHttpResponse response =
        new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
      final String mimetype = guessMimeTypeFromUri(path);
      response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
                         mimetype == null ? "text/plain" : mimetype);
      final long mtime = new File(path).lastModified();
      if (mtime > 0) {
        response.setHeader(HttpHeaders.Names.AGE,
                           (System.currentTimeMillis() - mtime) / 1000);
      } else {
        logWarn("Found a file with mtime=" + mtime + ": " + path);
      }
      response.setHeader(HttpHeaders.Names.CACHE_CONTROL,
                         max_age == 0 ? "no-cache" : "max-age=" + max_age);
      HttpHeaders.setContentLength(response, length);
      chan.write(response);
    }
    final DefaultFileRegion region = new DefaultFileRegion(file.getChannel(),
                                                           0, length);
    final ChannelFuture future = chan.write(region);
    future.addListener(new ChannelFutureListener() {
      public void operationComplete(final ChannelFuture future) {
        region.releaseExternalResources();
        done();
      }
    });
    if (!HttpHeaders.isKeepAlive(request)) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Method to call after writing the HTTP response to the wire.
   */
  private void done() {
    final int processing_time = processingTimeMillis();
    httplatency.add(processing_time);
    logInfo("HTTP " + request.getUri() + " done in " + processing_time + "ms");
    deferred.callback(null);
  }

  /**
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  private void sendBuffer(final HttpResponseStatus status,
                          final ChannelBuffer buf) {
    if (!chan.isConnected()) {
      done();
      return;
    }
    final DefaultHttpResponse response =
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE, guessMimeType(buf));
    // TODO(tsuna): Server, X-Backend, etc. headers.
    response.setContent(buf);
    final boolean keepalive = HttpHeaders.isKeepAlive(request);
    if (keepalive) {
      HttpHeaders.setContentLength(response, buf.readableBytes());
    }
    final ChannelFuture future = chan.write(response);
    if (!keepalive) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
    done();
  }

  /**
   * Returns the result of an attempt to guess the MIME type of the response.
   * @param buf The content of the reply to send.
   */
  private String guessMimeType(final ChannelBuffer buf) {
    final String mimetype = guessMimeTypeFromUri(request.getUri());
    return mimetype == null ? guessMimeTypeFromContents(buf) : mimetype;
  }

  /**
   * Attempts to guess the MIME type by looking at the URI requested.
   * @param uri The URI from which to infer the MIME type.
   */
  private static String guessMimeTypeFromUri(final String uri) {
    final int questionmark = uri.indexOf('?', 1);  // 1 => skip the initial /
    final int end = (questionmark > 0 ? questionmark : uri.length()) - 1;
    if (end < 5) {  // Need at least: "/a.js"
      return null;
    }
    final char a = uri.charAt(end - 3);
    final char b = uri.charAt(end - 2);
    final char c = uri.charAt(end - 1);
    switch (uri.charAt(end)) {
      case 'g':
        return a == '.' && b == 'p' && c == 'n' ? "image/png" : null;
      case 'l':
        return a == 'h' && b == 't' && c == 'm' ? HTML_CONTENT_TYPE : null;
      case 's':
        if (a == '.' && b == 'c' && c == 's') {
          return "text/css";
        } else if (b == '.' && c == 'j') {
          return "text/javascript";
        } else {
          break;
        }
      case 'f':
        return a == '.' && b == 'g' && c == 'i' ? "image/gif" : null;
      case 'o':
        return a == '.' && b == 'i' && c == 'c' ? "image/x-icon" : null;
    }
    return null;
  }

  /**
   * Simple "content sniffing".
   * May not be a great idea, but will do until this class has a better API.
   * @param buf The content of the reply to send.
   * @return The MIME type guessed from {@code buf}.
   */
  private String guessMimeTypeFromContents(final ChannelBuffer buf) {
    if (!buf.readable()) {
      logWarn("Sending an empty result?! buf=" + buf);
      return "text/plain";
    }
    final int firstbyte = buf.getUnsignedByte(buf.readerIndex());
    switch (firstbyte) {
      case '<':  // <html or <!DOCTYPE
        return HTML_CONTENT_TYPE;
      case '{':  // JSON object
      case '[':  // JSON array
        return "application/json";  // RFC 4627 section 6 mandates this.
      case 0x89:  // magic number in PNG files.
        return "image/png";
    }
    return "text/plain";  // Default.
  }

  /**
   * Easy way to generate a small, simple HTML page.
   * <p>
   * Equivalent to {@code makePage(null, title, subtitle, body)}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
  public static StringBuilder makePage(final String title,
                                       final String subtitle,
                                       final String body) {
    return makePage(null, title, subtitle, body);
  }

  /**
   * Easy way to generate a small, simple HTML page.
   * @param htmlheader Text to insert in the {@code head} tag.
   * Ignored if {@code null}.
   * @param title What should be in the {@code title} tag of the page.
   * @param subtitle Small sentence to use next to the TSD logo.
   * @param body The body of the page (excluding the {@code body} tag).
   * @return A full HTML page.
   */
   public static StringBuilder makePage(final String htmlheader,
                                        final String title,
                                        final String subtitle,
                                        final String body) {
    final StringBuilder buf = new StringBuilder(
      BOILERPLATE_LENGTH + (htmlheader == null ? 0 : htmlheader.length())
      + title.length() + subtitle.length() + body.length());
    buf.append(PAGE_HEADER_START)
      .append(title)
      .append(PAGE_HEADER_MID);
    if (htmlheader != null) {
      buf.append(htmlheader);
    }
    buf.append(PAGE_HEADER_END_BODY_START)
      .append(subtitle)
      .append(PAGE_BODY_MID)
      .append(body)
      .append(PAGE_FOOTER);
    return buf;
  }

  public String toString() {
    return "HttpQuery"
      + "(start_time=" + start_time
      + ", request=" + request
      + ", chan=" + chan
      + ", querystring=" + querystring
      + ')';
  }

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

  private void logInfo(final String msg) {
    LOG.info(chan.toString() + ' ' + msg);
  }

  private void logWarn(final String msg) {
    LOG.warn(chan.toString() + ' ' + msg);
  }

  private void logError(final String msg, final Exception e) {
    LOG.error(chan.toString() + ' ' + msg, e);
  }

  // -------------------------------------------- //
  // Boilerplate (shamelessly stolen from Google) //
  // -------------------------------------------- //

  private static final String PAGE_HEADER_START =
    "<!DOCTYPE HTML PUBLIC \"-//W3C//DTD HTML 4.01 Transitional//EN\">"
    + "<html><head>"
    + "<meta http-equiv=content-type content=\"text/html;charset=utf-8\">"
    + "<title>";

  private static final String PAGE_HEADER_MID =
    "</title>\n"
    + "<style><!--\n"
    + "body{font-family:arial,sans-serif;margin-left:2em}"
    + "A.l:link{color:#6f6f6f}"
    + "A.u:link{color:green}"
    + ".subg{background-color:#e2f4f7}"
    + ".fwf{font-family:monospace;white-space:pre-wrap}"
    + "//--></style>";

  private static final String PAGE_HEADER_END_BODY_START =
    "</head>\n"
    + "<body text=#000000 bgcolor=#ffffff>"
    + "<table border=0 cellpadding=2 cellspacing=0 width=100%>"
    + "<tr><td rowspan=3 width=1% nowrap><b>"
    + "<font color=#c71a32 size=10>T</font>"
    + "<font color=#00a189 size=10>S</font>"
    + "<font color=#1a65b7 size=10>D</font>"
    + "&nbsp;&nbsp;</b><td>&nbsp;</td></tr>"
    + "<tr><td class=subg><font color=#507e9b><b>";

  private static final String PAGE_BODY_MID =
    "</b></td></tr>"
    + "<tr><td>&nbsp;</td></tr></table>";

  private static final String PAGE_FOOTER =
    "<table width=100% cellpadding=0 cellspacing=0>"
    + "<tr><td class=subg><img alt=\"\" width=1 height=6></td></tr>"
    + "</table></body></html>";

  private static final int BOILERPLATE_LENGTH =
    PAGE_HEADER_START.length()
    + PAGE_HEADER_MID.length()
    + PAGE_HEADER_END_BODY_START.length()
    + PAGE_BODY_MID.length()
    + PAGE_FOOTER.length();

  /** Precomputed 404 page. */
  private static final StringBuilder PAGE_NOT_FOUND =
    makePage("Page Not Found", "Error 404",
             "<blockquote>"
             + "<h1>Page Not Found</h1>"
             + "The requested URL was not found on this server."
             + "</blockquote>");

}
