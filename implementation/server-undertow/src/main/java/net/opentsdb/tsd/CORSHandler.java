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

/**
 * A CORs handler inspired by https://github.com/Download/undertow-cors-filter 
 * that can return a 403 if the Origin isn't present or doesn't satisfy the
 * parameters.
 * 
 * @since 3.0
 */
public class CORSHandler implements HttpHandler {
  private static final Logger LOG = LoggerFactory.getLogger(CORSHandler.class);
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
  private final Pattern request_pattern;
  private final Pattern origin_pattern;
  private final Builder builder;
  
  protected CORSHandler(final Builder builder) {
    if (builder.next == null) {
      throw new IllegalArgumentException("Next cannot be null.");
    }
    if (Strings.isNullOrEmpty(builder.request_pattern)) {
      request_pattern = Pattern.compile(".*");
    } else {
      request_pattern = Pattern.compile(builder.request_pattern);
    }
    if (Strings.isNullOrEmpty(builder.origin_pattern)) {
      origin_pattern = Pattern.compile(".*");
    } else {
      origin_pattern = Pattern.compile(builder.origin_pattern);
    }
    this.builder = builder;
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
        if (builder.require_origin) {
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
          exchange.getResponseHeaders().add(ALLOW_HEADERS, DEFAULT_ALLOW_HEADERS);
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
    
    builder.next.handleRequest(exchange);
  }

  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    protected HttpHandler next;
    protected String request_pattern;
    protected String origin_pattern;
    protected boolean require_origin;
    
    public Builder setNext(final HttpHandler next) {
      this.next = next;
      return this;
    }
    
    public Builder setRequestPattern(final String pattern) {
      this.request_pattern = pattern;
      return this;
    }
    
    public Builder setOriginPattern(final String pattern) {
      this.origin_pattern = pattern;
      return this;
    }
    
    public Builder setRequireOrigin(final boolean require_origin) {
      this.require_origin = require_origin;
      return this;
    }
    
    public CORSHandler build() {
      return new CORSHandler(this);
    }
  }
}
