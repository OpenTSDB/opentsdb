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

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

/**
 * Binds together an HTTP request and the channel on which it was received.
 *
 * It makes it easier to provide a few utility methods to respond to the
 * requests.
 */
final class HttpQuery {

  private static final Logger LOG = LoggerFactory.getLogger(HttpQuery.class);

  private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  /** When the query was started (useful for timing). */
  private final long start_time = System.nanoTime();

  /** The request in this HTTP query. */
  private final HttpRequest request;

  /** The channel on which the request was received. */
  private final Channel chan;

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

  /** Returns how many ms have elapsed since this query was created. */
  public int processingTimeMillis() {
    return (int) ((System.nanoTime() - start_time) / 1000000);
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
    {
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
    {
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
    sendReply(HttpResponseStatus.NOT_FOUND, PAGE_NOT_FOUND);
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
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  public void sendReply(final HttpResponseStatus status,
                        final StringBuilder buf) {
    sendBuffer(status, ChannelBuffers.copiedBuffer(buf.toString(),
                                                   CharsetUtil.UTF_8));
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
  private void sendBuffer(final HttpResponseStatus status,
                          final ChannelBuffer buf) {
    final DefaultHttpResponse response =
      new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
    response.setHeader(HttpHeaders.Names.CONTENT_TYPE,
                       (status == HttpResponseStatus.OK
                        ? guessMimeType(buf)
                        : HTML_CONTENT_TYPE));  // Error pages are in HTML.
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
      + ')';
  }

  // ---------------- //
  // Logging helpers. //
  // ---------------- //

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
