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
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.graph.Plot;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.PluginLoader;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFuture;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.DefaultFileRegion;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.spi.ThrowableProxy;
import ch.qos.logback.classic.spi.ThrowableProxyUtil;

import com.stumbleupon.async.Deferred;

/**
 * Binds together an HTTP request and the channel on which it was received.
 *
 * It makes it easier to provide a few utility methods to respond to the
 * requests.
 */
final class HttpQuery extends AbstractHttpQuery {

  private static final Logger LOG = LoggerFactory.getLogger(HttpQuery.class);

  private static final String HTML_CONTENT_TYPE = "text/html; charset=UTF-8";

  /** The maximum implemented API version, set when the user doesn't */
  private static final int MAX_API_VERSION = 1;

  /**
   * Keep track of the latency of HTTP requests.
   */
  private static final Histogram httplatency =
    new Histogram(16000, (short) 2, 100);

  /** Maps Content-Type to a serializer */
  private static HashMap<String, Constructor<? extends HttpSerializer>>
    serializer_map_content_type = null;

  /** Maps query string names to a serializer */
  private static HashMap<String, Constructor<? extends HttpSerializer>>
    serializer_map_query_string = null;

  /** Caches serializer implementation information for user access */
  private static ArrayList<HashMap<String, Object>> serializer_status = null;

  /** API version parsed from the incoming request */
  private int api_version = 0;

  /** The serializer to use for parsing input and responding */
  private HttpSerializer serializer = null;

  /** Whether or not to show stack traces in the output */
  private final boolean show_stack_trace;

  /**
   * Constructor.
   * @param request The request in this HTTP query.
   * @param chan The channel on which the request was received.
   */
  public HttpQuery(final TSDB tsdb, final HttpRequest request, final Channel chan) {
    super(tsdb, request, chan);
    this.show_stack_trace =
      tsdb.getConfig().getBoolean("tsd.http.show_stack_trace");
    this.serializer = new HttpJsonSerializer(this);
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public static void collectStats(final StatsCollector collector) {
    collector.record("http.latency", httplatency, "type=all");
  }

  /**
   * Returns the version for an API request. If the request was for a deprecated
   * API call (such as /q, /suggest, /logs) this value will be 0. If the request
   * was for a new API call, the version will be 1 or higher. If the user does
   * not supply a version, the MAX_API_VERSION value will be used.
   * @since 2.0
   */
  public int apiVersion() {
    return this.api_version;
  }

  /** @return Whether or not to show stack traces in errors @since 2.0 */
  public boolean showStackTrace() {
    return this.show_stack_trace;
  }

  /**
   * Return the {@link Deferred} associated with this query.
   */
  public Deferred<Object> getDeferred() {
    return deferred;
  }

  /** @return The selected seralizer. Will return null if {@link #setSerializer}
   * hasn't been called yet @since 2.0  */
  public HttpSerializer serializer() {
    return this.serializer;
  }

  /**
   * Helper that strips the api and optional version from the URI array since
   * api calls only care about what comes after.
   * E.g. if the URI is "/api/v1/uid/assign" this method will return the
   * {"uid", "assign"}
   * @return An array with 1 or more components, note the first item may be
   * an empty string if given just "/api" or "/api/v1"
   * @throws BadRequestException if the URI is empty or does not start with a
   * slash
   * @throws NullPointerException if the URI is null
   * @throws IllegalArgumentException if the uri does not start with "/api"
   * @since 2.0
   */
  public String[] explodeAPIPath() {
    final String[] split = this.explodePath();
    int index = 1;
    if (split.length < 1 || !split[0].toLowerCase().equals("api")) {
      throw new IllegalArgumentException("The URI does not start with \"/api\"");
    }
    if (split.length < 2) {
      // given "/api"
      final String[] root = { "" };
      return root;
    }
    if (split[1].toLowerCase().startsWith("v") && split[1].length() > 1 &&
        Character.isDigit(split[1].charAt(1))) {
      index = 2;
    }

    if (split.length - index == 0) {
      // given "/api/v#"
      final String[] root = { "" };
      return root;
    }

    final String[] path = new String[split.length - index];
    int path_idx = 0;
    for (int i = index; i < split.length; i++) {
      path[path_idx] = split[i];
      path_idx++;
    }
    return path;
  }

  /**
   * This method splits the query path component and returns a string suitable
   * for routing by {@link RpcHandler}. The resulting route is always lower case
   * and will consist of either an empty string, a deprecated API call or an
   * API route. API routes will set the {@link #apiVersion} to either a user
   * provided value or the MAX_API_VERSION.
   * <p>
   * Some URIs and their routes include:<ul>
   * <li>"/" - "" - the home directory</li>
   * <li>"/q?start=1h-ago&m=..." - "q" - a deprecated API call</li>
   * <li>"/api/v4/query" - "api/query" - a versioned API call</li>
   * <li>"/api/query" - "api/query" - a default versioned API call</li>
   * </ul>
   * @return the base route
   * @throws BadRequestException if the version requested is greater than the
   * max or the version # can't be parsed
   * @since 2.0
   */
  @Override
  public String getQueryBaseRoute() {
    final String[] split = explodePath();
    if (split.length < 1) {
      return "";
    }
    if (!split[0].toLowerCase().equals("api")) {
      return split[0].toLowerCase();
    }
    // set the default api_version so the API call is handled by a serializer if
    // an exception is thrown
    this.api_version = MAX_API_VERSION;
    if (split.length < 2) {
      return "api";
    }
    if (split[1].toLowerCase().startsWith("v") && split[1].length() > 1 &&
        Character.isDigit(split[1].charAt(1))) {
      try {
        final int version = Integer.parseInt(split[1].substring(1));
        if (version > MAX_API_VERSION) {
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
              "Requested API version is greater than the max implemented",
              "API version [" + version + "] is greater than the max [" +
              MAX_API_VERSION + "]");
        }
        this.api_version = version;
      } catch (NumberFormatException nfe) {
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
            "Invalid API version format supplied",
            "API version [" + split[1].substring(1) +
            "] cannot be parsed to an integer");
      }
    } else {
      return "api/" + split[1].toLowerCase();
    }
    if (split.length < 3){
      return "api";
    }
    return "api/" + split[2].toLowerCase();
  }

  /**
   * Determines the requested HttpMethod via VERB and QS override.
   * If the request is a {@code GET} and the user provides a valid override
   * method in the {@code method=&lt;method&gt;} query string parameter, then
   * the override is returned. If the user supplies an invalid override, an
   * exception is thrown. If the verb was not a GET, then the original value
   * is returned.
   * @return An HttpMethod
   * @throws BadRequestException if the user provided a {@code method} qs
   * without a value or the override contained an invalid value
   * @since 2.0
   */
  public HttpMethod getAPIMethod() {
    if (this.method() != HttpMethod.GET) {
      return this.method();
    } else {
      if (this.hasQueryStringParam("method_override")) {
        final String qs_method = this.getQueryStringParam("method_override");
        if (qs_method == null || qs_method.isEmpty()) {
          throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
              "Missing method override value");
        }
        if (qs_method.toLowerCase().equals("get")) {
          // you can't fix dumb
          return HttpMethod.GET;
        } else if (qs_method.toLowerCase().equals("post")){
          return HttpMethod.POST;
        } else if (qs_method.toLowerCase().equals("put")){
          return HttpMethod.PUT;
        } else if (qs_method.toLowerCase().equals("delete")){
          return HttpMethod.DELETE;
        } else {
          throw new BadRequestException(HttpResponseStatus.METHOD_NOT_ALLOWED,
            "Unknown or unsupported method override value");
        }
      }

      // no override, so just return the method
      return this.method();
    }
  }

  /**
   * Sets the local serializer based on a query string parameter or content type.
   * <p>
   * If the caller supplies a "serializer=" parameter, the proper serializer is
   * loaded if found. If the serializer doesn't exist, an exception will be
   * thrown and the user gets an error
   * <p>
   * If no query string parameter is supplied, the Content-Type header for the
   * request is parsed and if a matching serializer is found, it's used.
   * Otherwise we default to the HttpJsonSerializer.
   * @throws InvocationTargetException if the serializer cannot be instantiated
   * @throws IllegalArgumentException if the serializer cannot be instantiated
   * @throws InstantiationException if the serializer cannot be instantiated
   * @throws IllegalAccessException if a security manager is blocking access
   * @throws BadRequestException if a serializer requested via query string does
   * not exist
   */
  public void setSerializer() throws InvocationTargetException,
    IllegalArgumentException, InstantiationException, IllegalAccessException {
    if (this.hasQueryStringParam("serializer")) {
      final String qs = this.getQueryStringParam("serializer");
      Constructor<? extends HttpSerializer> ctor =
        serializer_map_query_string.get(qs);
      if (ctor == null) {
        this.serializer = new HttpJsonSerializer(this);
        throw new BadRequestException(HttpResponseStatus.BAD_REQUEST,
            "Requested serializer was not found",
            "Could not find a serializer with the name: " + qs);
      }

      this.serializer = ctor.newInstance(this);
      return;
    }

    // attempt to parse the Content-Type string. We only want the first part,
    // not the character set. And if the CT is missing, we'll use the default
    // serializer
    String content_type = request().headers().get("Content-Type");
    if (content_type == null || content_type.isEmpty()) {
      return;
    }
    if (content_type.indexOf(";") > -1) {
      content_type = content_type.substring(0, content_type.indexOf(";"));
    }
    Constructor<? extends HttpSerializer> ctor =
      serializer_map_content_type.get(content_type);
    if (ctor == null) {
      return;
    }

    this.serializer = ctor.newInstance(this);
  }

  /**
   * Sends a 500 error page to the client.
   * Handles responses from deprecated API calls as well as newer, versioned
   * API calls
   * @param cause The unexpected exception that caused this error.
   */
  @Override
  public void internalError(final Exception cause) {
    logError("Internal Server Error on " + request().getUri(), cause);

    if (this.api_version > 0) {
      // always default to the latest version of the error formatter since we
      // need to return something
      switch (this.api_version) {
        case 1:
        default:
          sendReply(HttpResponseStatus.INTERNAL_SERVER_ERROR,
              serializer.formatErrorV1(cause));
      }
      return;
    }

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
  }

  /**
   * Sends a 400 error page to the client.
   * Handles responses from deprecated API calls
   * @param explain The string describing why the request is bad.
   */
  public void badRequest(final String explain) {
    badRequest(new BadRequestException(explain));
  }

  /**
   * Sends an error message to the client. Handles responses from 
   * deprecated API calls.
   * @param exception The exception that was thrown
   */
  @Override
  public void badRequest(final BadRequestException exception) {
    logWarn("Bad Request on " + request().getUri() + ": " + exception.getMessage());
    if (this.api_version > 0) {
      // always default to the latest version of the error formatter since we
      // need to return something
      switch (this.api_version) {
        case 1:
        default:
          sendReply(exception.getStatus(), serializer.formatErrorV1(exception));
      }
      return;
    }
    if (hasQueryStringParam("json")) {
      final StringBuilder buf = new StringBuilder(10 +
          exception.getDetails().length());
      buf.append("{\"err\":\"");
      HttpQuery.escapeJson(exception.getMessage(), buf);
      buf.append("\"}");
      sendReply(HttpResponseStatus.BAD_REQUEST, buf);
    } else {
      sendReply(HttpResponseStatus.BAD_REQUEST,
                makePage("Bad Request", "Looks like it's your fault this time",
                         "<blockquote>"
                         + "<h1>Bad Request</h1>"
                         + "Sorry but your request was rejected as being"
                         + " invalid.<br/><br/>"
                         + "The reason provided was:<blockquote>"
                         + exception.getMessage()
                         + "</blockquote></blockquote>"));
    }
  }

  /**
   * Sends a 404 error page to the client.
   * Handles responses from deprecated API calls
   */
  @Override
  public void notFound() {
    logWarn("Not Found: " + request().getUri());
    if (this.api_version > 0) {
      // always default to the latest version of the error formatter since we
      // need to return something
      switch (this.api_version) {
        case 1:
        default:
          sendReply(HttpResponseStatus.NOT_FOUND, serializer.formatNotFoundV1());
      }
      return;
    }
    if (hasQueryStringParam("json")) {
      sendReply(HttpResponseStatus.NOT_FOUND,
                new StringBuilder("{\"err\":\"Page Not Found\"}"));
    } else {
      sendReply(HttpResponseStatus.NOT_FOUND, PAGE_NOT_FOUND);
    }
  }

  /** Redirects the client's browser to the given location.  */
  public void redirect(final String location) {
    // set the header AND a meta refresh just in case
    response().headers().set("Location", location);
    sendReply(HttpResponseStatus.OK,
      new StringBuilder(
          "<html></head><meta http-equiv=\"refresh\" content=\"0; url="
           + location + "\"></head></html>")
         .toString().getBytes(this.getCharset())
    );
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
   * Sends data in an HTTP "200 OK" reply to the client.
   * @param data Raw byte array to send as-is after the HTTP headers.
   */
  public void sendReply(final byte[] data) {
    sendBuffer(HttpResponseStatus.OK, ChannelBuffers.wrappedBuffer(data));
  }

  /**
   * Sends data to the client with the given HTTP status code.
   * @param status HTTP status code to return
   * @param data Raw byte array to send as-is after the HTTP headers.
   * @since 2.0
   */
  public void sendReply(final HttpResponseStatus status, final byte[] data) {
    sendBuffer(status, ChannelBuffers.wrappedBuffer(data));
  }

  /**
   * Sends an HTTP reply to the client.
   * <p>
   * This is equivalent of
   * <code>{@link #sendReply(HttpResponseStatus, StringBuilder)
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
   * <code>{@link #sendReply(HttpResponseStatus, StringBuilder)
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
   * Sends the ChannelBuffer with a 200 status
   * @param buf The buffer to send
   * @since 2.0
   */
  public void sendReply(final ChannelBuffer buf) {
    sendBuffer(HttpResponseStatus.OK, buf);
  }

  /**
   * Sends the ChannelBuffer with the given status
   * @param status HttpResponseStatus to reply with
   * @param buf The buffer to send
   * @since 2.0
   */
  public void sendReply(final HttpResponseStatus status,
      final ChannelBuffer buf) {
    sendBuffer(status, buf);
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
  @SuppressWarnings("resource") // Clears warning about RandomAccessFile not
      // being closed. It is closed in operationComplete().
  public void sendFile(final HttpResponseStatus status,
                       final String path,
                       final int max_age) throws IOException {
    if (max_age < 0) {
      throw new IllegalArgumentException("Negative max_age=" + max_age
                                         + " for path=" + path);
    }
    if (!channel().isConnected()) {
      done();
      return;
    }
    RandomAccessFile file;
    try {
      file = new RandomAccessFile(path, "r");
    } catch (FileNotFoundException e) {
      logWarn("File not found: " + e.getMessage());
      if (getQueryString() != null && !getQueryString().isEmpty()) {
        getQueryString().remove("png");  // Avoid potential recursion.
      }
      this.sendReply(HttpResponseStatus.NOT_FOUND, serializer.formatNotFoundV1());
      return;
    }
    final long length = file.length();
    {
      final String mimetype = guessMimeTypeFromUri(path);
      response().headers().set(HttpHeaders.Names.CONTENT_TYPE,
                         mimetype == null ? "text/plain" : mimetype);
      final long mtime = new File(path).lastModified();
      if (mtime > 0) {
        response().headers().set(HttpHeaders.Names.AGE,
                           (System.currentTimeMillis() - mtime) / 1000);
      } else {
        logWarn("Found a file with mtime=" + mtime + ": " + path);
      }
      response().headers().set(HttpHeaders.Names.CACHE_CONTROL,
                         "max-age=" + max_age);
      HttpHeaders.setContentLength(response(), length);
      channel().write(response());
    }
    final DefaultFileRegion region = new DefaultFileRegion(file.getChannel(),
                                                           0, length);
    final ChannelFuture future = channel().write(region);
    future.addListener(new ChannelFutureListener() {
      public void operationComplete(final ChannelFuture future) {
        region.releaseExternalResources();
        done();
      }
    });
    if (!HttpHeaders.isKeepAlive(request())) {
      future.addListener(ChannelFutureListener.CLOSE);
    }
  }

  /**
   * Method to call after writing the HTTP response to the wire.
   */
  @Override
  public void done() {
    final int processing_time = processingTimeMillis();
    httplatency.add(processing_time);
    logInfo("HTTP " + request().getUri() + " done in " + processing_time + "ms");
    deferred.callback(null);
  }

  /**
   * Sends an HTTP reply to the client.
   * @param status The status of the request (e.g. 200 OK or 404 Not Found).
   * @param buf The content of the reply to send.
   */
  private void sendBuffer(final HttpResponseStatus status,
                          final ChannelBuffer buf) {
    final String contentType = (api_version < 1 ? guessMimeType(buf) :
      serializer.responseContentType());
    sendBuffer(status, buf, contentType);
  }

  /**
   * Returns the result of an attempt to guess the MIME type of the response.
   * @param buf The content of the reply to send.
   */
  private String guessMimeType(final ChannelBuffer buf) {
    final String mimetype = guessMimeTypeFromUri(request().getUri());
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
   * Loads the serializer maps with present, implemented serializers. If no
   * plugins are loaded, only the default implementations will be available.
   * This method also builds the status map that users can access via the API
   * to see what has been implemented.
   * <p>
   * <b>WARNING:</b> The TSDB should have called on of the JAR load or search
   * methods from PluginLoader before calling this method. This will only scan
   * the class path for plugins that implement the HttpSerializer class
   * @param tsdb The TSDB to pass on to plugins
   * @throws NoSuchMethodException if a class could not be instantiated
   * @throws SecurityException if a security manager is present and causes
   * trouble
   * @throws ClassNotFoundException if the base class couldn't be found, for
   * some really odd reason
   * @throws IllegalStateException if a mapping collision occurs
   * @since 2.0
   */
  public static void initializeSerializerMaps(final TSDB tsdb)
    throws SecurityException, NoSuchMethodException, ClassNotFoundException {
    List<HttpSerializer> serializers =
      PluginLoader.loadPlugins(HttpSerializer.class);

    // add the default serializers compiled with OpenTSDB
    if (serializers == null) {
      serializers = new ArrayList<HttpSerializer>(1);
    }
    final HttpSerializer default_serializer = new HttpJsonSerializer();
    serializers.add(default_serializer);

    serializer_map_content_type =
      new HashMap<String, Constructor<? extends HttpSerializer>>();
    serializer_map_query_string =
      new HashMap<String, Constructor<? extends HttpSerializer>>();
    serializer_status = new ArrayList<HashMap<String, Object>>();

    for (HttpSerializer serializer : serializers) {
      final Constructor<? extends HttpSerializer> ctor =
        serializer.getClass().getDeclaredConstructor(HttpQuery.class);

      // check for collisions before adding serializers to the maps
      Constructor<? extends HttpSerializer> map_ctor =
        serializer_map_content_type.get(serializer.requestContentType());
      if (map_ctor != null) {
        final String err = "Serializer content type collision between \"" +
        serializer.getClass().getCanonicalName() + "\" and \"" +
        map_ctor.getClass().getCanonicalName() + "\"";
        LOG.error(err);
        throw new IllegalStateException(err);
      }
      serializer_map_content_type.put(serializer.requestContentType(), ctor);

      map_ctor = serializer_map_query_string.get(serializer.shortName());
      if (map_ctor != null) {
        final String err = "Serializer name collision between \"" +
        serializer.getClass().getCanonicalName() + "\" and \"" +
        map_ctor.getClass().getCanonicalName() + "\"";
        LOG.error(err);
        throw new IllegalStateException(err);
      }
      serializer_map_query_string.put(serializer.shortName(), ctor);

      // initialize the plugins
      serializer.initialize(tsdb);

      // write the status for any serializers OTHER than the default
      if (serializer.shortName().equals("json")) {
        continue;
      }
      HashMap<String, Object> status = new HashMap<String, Object>();
      status.put("version", serializer.version());
      status.put("class", serializer.getClass().getCanonicalName());
      status.put("serializer", serializer.shortName());
      status.put("request_content_type", serializer.requestContentType());
      status.put("response_content_type", serializer.responseContentType());

      HashSet<String> parsers = new HashSet<String>();
      HashSet<String> formats = new HashSet<String>();
      Method[] methods = serializer.getClass().getDeclaredMethods();
      for (Method m : methods) {
        if (Modifier.isPublic(m.getModifiers())) {
          if (m.getName().startsWith("parse")) {
            parsers.add(m.getName().substring(5));
          } else if (m.getName().startsWith("format")) {
            formats.add(m.getName().substring(6));
          }
        }
      }
      status.put("parsers", parsers);
      status.put("formatters", formats);
      serializer_status.add(status);
    }

    // add the base class to the status map so users can see everything that
    // is implemented
    HashMap<String, Object> status = new HashMap<String, Object>();
    // todo - set the OpenTSDB version
    //status.put("version", BuildData.version);
    final Class<?> base_serializer =
      Class.forName("net.opentsdb.tsd.HttpSerializer");
    status.put("class", default_serializer.getClass().getCanonicalName());
    status.put("serializer", default_serializer.shortName());
    status.put("request_content_type", default_serializer.requestContentType());
    status.put("response_content_type", default_serializer.responseContentType());

    ArrayList<String> parsers = new ArrayList<String>();
    ArrayList<String> formats = new ArrayList<String>();
    Method[] methods = base_serializer.getDeclaredMethods();
    for (Method m : methods) {
      if (Modifier.isPublic(m.getModifiers())) {
        if (m.getName().startsWith("parse")) {
          parsers.add(m.getName().substring(5));
        }
        if (m.getName().startsWith("format")) {
          formats.add(m.getName().substring(6));
        }
      }
    }
    status.put("parsers", parsers);
    status.put("formatters", formats);
    serializer_status.add(status);
  }

  /**
   * Returns the serializer status map.
   * <b>Note:</b> Do not modify this map, it is for read only purposes only
   * @return the serializer status list and maps
   * @since 2.0
   */
  public static ArrayList<HashMap<String, Object>> getSerializerStatus() {
    return serializer_status;
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
  
  @Override
  protected Logger logger() {
    return LOG;
  }
  
  // -------------------------------------------- //
  // Boilerplate (shamelessly stolen from Google) //
  // -------------------------------------------- //

  private static final String PAGE_HEADER_START =
    "<!DOCTYPE html>"
    + "<html><head>"
    + "<meta http-equiv=content-type content=\"text/html;charset=utf-8\">"
    + "<title>";

  private static final String PAGE_HEADER_MID =
    "</title>\n"
    + "<style><!--\n"
    + "body{font-family:arial,sans-serif;margin-left:2em}"
    + "A.l:link{color:#6f6f6f}"
    + "A.u:link{color:green}"
    + ".fwf{font-family:monospace;white-space:pre-wrap}"
    + "//--></style>";

  private static final String PAGE_HEADER_END_BODY_START =
    "</head>\n"
    + "<body text=#000000 bgcolor=#ffffff>"
    + "<table border=0 cellpadding=2 cellspacing=0 width=100%>"
    + "<tr><td rowspan=3 width=1% nowrap>"
    + "<img src=s/opentsdb_header.jpg>"
    + "<td>&nbsp;</td></tr>"
    + "<tr><td><font color=#507e9b><b>";

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
