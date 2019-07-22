// This file is part of OpenTSDB.
// Copyright (C)2019  The OpenTSDB Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package net.opentsdb.tsd;

import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.HeaderValues;
import io.undertow.util.Headers;
import io.undertow.util.HttpString;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.core.TSDB;

/**
 * A CORs handler inspired by https://github.com/Download/undertow-cors-filter 
 * that can return a 403 if the Origin isn't present or doesn't satisfy the
 * parameters.
 * 
 * @since 3.0
 */
public class CORSHandler implements HttpHandler, ConfigurationCallback<Object> {
  private static final Logger LOG = LoggerFactory.getLogger(CORSHandler.class);
  
  public static final String CORS_ORIGIN_KEY = "tsd.http.request.cors.pattern";
  public static final String CORS_HEADERS_KEY = "tsd.http.request.cors.headers";
  
  private static final HttpString ALLOW_ORIGIN = HttpString.tryFromString("Access-Control-Allow-Origin");
  private static final HttpString ALLOW_HEADERS = HttpString.tryFromString("Access-Control-Allow-Headers");
  private static final HttpString EXPOSE_HEADERS = HttpString.tryFromString("Access-Control-Expose-Headers");
  private static final HttpString ALLOW_METHODS = HttpString.tryFromString("Access-Control-Allow-Methods");
  private static final HttpString ALLOW_CREDENTIALS = HttpString.tryFromString("Access-Control-Allow-Credentials");
  private static final HttpString MAX_AGE = HttpString.tryFromString("Access-Control-Max-Age");
  
  private static final String DEFAULT_METHODS = "DELETE,GET,HEAD,OPTIONS,PATCH,POST,PUT";
  private static final String DEFAULT_ALLOW_HEADERS = "Authorization,Content-Type,Link,X-Total-Count,Range";
  private static final String DEFAULT_EXPOSE_HEADERS = "Accept-Ranges,Content-Length,Content-Range,ETag,Link,Server,X-Total-Count";
  private static final int DEFAULT_AGE = 86400 * 7;
  
  private static final byte[] FORBIDDEN = ("{\"error\":{\"code\":403,"
      + "\"message\":\"Forbidden.\"}}").getBytes();

  private final HttpHandler next;
  private volatile Pattern request_pattern;
  private volatile Pattern origin_pattern;
  private volatile String allow_headers;
  private volatile boolean require_origin;
  
  protected CORSHandler(final TSDB tsdb, final HttpHandler next) {
    if (next == null) {
      throw new IllegalArgumentException("Next cannot be null.");
    }
    this.next = next;
    
    // TODO allow for a requet
    request_pattern = Pattern.compile(".*");
    origin_pattern = Pattern.compile(tsdb.getConfig().getString(CORS_ORIGIN_KEY));
    allow_headers = tsdb.getConfig().getString(CORS_HEADERS_KEY);
    LOG.info("Starting CORS handler with pattern: " + origin_pattern);
  }
  
  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    if (exchange.isInIoThread()) {
      // don't do anything here. Trigger a dispatch.
      exchange.dispatch(this);
      return;
    }
    
    if (request_pattern.matcher(exchange.getRequestURL()).matches()) {
      final HeaderValues headers = exchange.getRequestHeaders().get("Origin");
      final String origin = headers == null ? null : headers.peekFirst();
      if (Strings.isNullOrEmpty(origin)) {
        if (require_origin) {
          exchange.startBlocking();
          exchange.setStatusCode(403);
          exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
          exchange.getOutputStream().write(FORBIDDEN);
          exchange.setResponseContentLength(FORBIDDEN.length);
          exchange.endExchange();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Blocking request from " 
                + exchange.getConnection().getPeerAddress() 
                + " did not have an Origin header and we require one.");
          }
          return;
        }
      } else {
        if (!origin_pattern.matcher(origin).matches()) {
          exchange.startBlocking();
          exchange.setStatusCode(403);
          exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "application/json");
          exchange.getOutputStream().write(FORBIDDEN);
          exchange.setResponseContentLength(FORBIDDEN.length);
          exchange.endExchange();
          if (LOG.isTraceEnabled()) {
            LOG.trace("Blocking request from " 
                + exchange.getConnection().getPeerAddress() 
                + " as the origin did not matcher our pattern: " + origin);
          }
          return;
        }
        
        // matched!
        if (!exchange.getResponseHeaders().contains(ALLOW_HEADERS)) {
          exchange.getResponseHeaders().add(ALLOW_HEADERS, allow_headers);
        }
        if (!exchange.getResponseHeaders().contains(ALLOW_CREDENTIALS)) {
          exchange.getResponseHeaders().add(ALLOW_CREDENTIALS, "true");
        }
        if (!exchange.getResponseHeaders().contains(ALLOW_ORIGIN)) {
          exchange.getResponseHeaders().add(ALLOW_ORIGIN, origin);
        }
        if (!exchange.getResponseHeaders().contains(ALLOW_METHODS)) {
          exchange.getResponseHeaders().add(ALLOW_METHODS, DEFAULT_METHODS);
        }
        if (!exchange.getResponseHeaders().contains(EXPOSE_HEADERS)) {
          exchange.getResponseHeaders().add(EXPOSE_HEADERS, DEFAULT_EXPOSE_HEADERS);
        }
        if (!exchange.getResponseHeaders().contains(MAX_AGE)) {
          exchange.getResponseHeaders().add(MAX_AGE, DEFAULT_AGE);
        }
        
      }
    }
    
    next.handleRequest(exchange);
  }
  
  public static void registerConfigs(final TSDB tsdb) {
    tsdb.getConfig().register(CORS_ORIGIN_KEY, null, false, "A regular expression "
        + "of domain names to allow access to OpenTSDB when the Origin "
        + "header is specified by the client. If empty, CORS requests "
        + "are passed through without validation. The list may not "
        + "contain the public wildcard * and specific domains at the "
        + "same time.");
    tsdb.getConfig().register(CORS_HEADERS_KEY, DEFAULT_ALLOW_HEADERS, 
        false, 
        "A comma separated list of headers sent to clients when "
            + "executing a CORs request. The literal value of this option "
            + "will be passed to clients.");
  }

  @Override
  public void update(final String key, final Object value) {
    try {
      if (key.equals(CORS_ORIGIN_KEY)) {
        origin_pattern = Pattern.compile((String) value);
        LOG.info("Updated CORS pattern to: " + origin_pattern);
      } else if (key.equals(CORS_HEADERS_KEY)) {
        allow_headers = (String) value;
        LOG.info("Updated CORS allowed headers to: " + allow_headers);
      }
    } catch (Throwable t) {
      LOG.error("Failed to parse setting: " + key, t);
    }
    
  }
}
