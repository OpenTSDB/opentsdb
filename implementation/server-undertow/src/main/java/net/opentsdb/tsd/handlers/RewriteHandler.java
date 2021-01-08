// This file is part of OpenTSDB.
// Copyright (C) 2020  The OpenTSDB Authors.
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
package net.opentsdb.tsd.handlers;

import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.regex.Pattern;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.servlet.UndertowServletLogger;

/**
 * Super simple re-write handler inspired by io.undertow.servlet.compat.rewrite.RewriteHandler 
 * (that only works as a part of a servlet context, and even then, didn't quite
 * work in 2.0.21). The rules are a map of patterns to replacement strings and 
 * only apply to the exchange's relative path. Make sure to anchor the patterns
 * if you want some odd side-effects.
 * 
 * @since 3.0
 */
public class RewriteHandler implements HttpHandler {

  private final Pattern[] patterns;
  private final String[] replacements;
  private final HttpHandler next;
  
  /**
   * Default ctor.
   * @param rules A non-null (could be empty) set of rules.
   * @param next The next handler to call.
   */
  public RewriteHandler(final Map<String, String> rules, final HttpHandler next) {
    Map<String, String> sorted = new TreeMap<String, String>(rules);
    patterns = new Pattern[sorted.size()];
    replacements = new String[sorted.size()];
    
    if (rules != null && !rules.isEmpty()) {
      int i = 0;
      for (final Entry<String, String> entry : sorted.entrySet()) {
        patterns[i] = Pattern.compile(entry.getKey());
        replacements[i++] = entry.getValue();
      }
    }
    this.next = next;
  }
  
  @Override
  public void handleRequest(final HttpServerExchange exchange) throws Exception {
    if (patterns.length < 1) {
      next.handleRequest(exchange);
      return;
    }

    String url = exchange.getRelativePath();
    for (int i = 0; i < patterns.length; i++) {
      if (patterns[i].matcher(url).find()) {
        // matched!
        url = replacements[i];
        exchange.setRelativePath(url);
        System.out.println("*MMMMMMATCHED");
        if (UndertowServletLogger.REQUEST_LOGGER.isDebugEnabled()) {
          UndertowServletLogger.REQUEST_LOGGER.debug("Rewrote relative url " 
              + exchange.getRelativePath() + " as " + url
              + " with rule pattern " + patterns[i]);
      }
        break;
      }
    }
    
    next.handleRequest(exchange);
  }

}
