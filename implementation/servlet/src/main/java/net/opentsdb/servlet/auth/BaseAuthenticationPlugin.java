// This file is part of OpenTSDB.
// Copyright (C) 2021  The OpenTSDB Authors.
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
package net.opentsdb.servlet.auth;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AuthState;
import net.opentsdb.configuration.ConfigurationCallback;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.BaseTSDBPlugin;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.stats.StatsCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.security.Principal;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

/**
 * Base implementation for an authentication filter plugin that contains
 * lists for bypassing auth based on:
 * - literal URI (not URL)
 * - pattern on the URI (not URL)
 * - HTTP method
 *
 * @since 3.0
 */
public abstract class BaseAuthenticationPlugin extends BaseTSDBPlugin
        implements AuthFilter {

  private static final Logger LOG = LoggerFactory.getLogger(
          BaseAuthenticationPlugin.class);

  public static final String BYPASS_LITERALS_KEY = "auth.bypass.literals";
  public static final String BYPASS_REGEX_KEY = "auth.bypass.regex";
  public static final String BYPASS_METHODS_KEY = "auth.bypass.methods";

  public static final String BYPASS_URI_METRIC = "auth.bypass.uri";
  public static final String BYPASS_METHOD_METRIC = "auth.bypass.method";

  public static final TypeReference<List<String>> LIST_REF =
          new TypeReference<List<String>>() { };

  protected volatile List<String> bypass_methods;
  protected volatile List<String> bypass_literals;
  protected volatile List<Pattern> bypass_regex;

  protected String[] tags;
  protected StatsCollector stats;

  public BaseAuthenticationPlugin() {
    bypass_methods = Lists.newArrayList();
    bypass_literals = Lists.newArrayList();
    bypass_regex = Lists.newArrayList();
  }

  /** Used to denote that authentication is bypassed for the user */
  public static final AuthState BYPASS_STATE = new AuthState() {

    @Override
    public String getUser() {
      return "AuthentiationBypass";
    }

    @Override
    public Principal getPrincipal() {
      return null;
    }

    @Override
    public AuthStatus getStatus() {
      return AuthStatus.SUCCESS;
    }

    @Override
    public String getMessage() {
      return null;
    }

    @Override
    public Throwable getException() {
      return null;
    }

    @Override
    public String getTokenType() {
      return null;
    }

    @Override
    public byte[] getToken() {
      return null;
    }

    @Override
    public boolean hasRole(final String role) {
      return false;
    }

    @Override
    public boolean hasPermission(final String action) {
      return false;
    }

  };

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    this.tsdb = tsdb;
    this.id = id;

    stats = tsdb.getStatsCollector();
    tags = new String[] { "plugin", id };
    return Deferred.fromResult(null);
  }

  @Override
  public void doFilter(final ServletRequest servletRequest,
                       final ServletResponse servletResponse,
                       final FilterChain chain) throws IOException, ServletException {
    // if we already have a valid auth state, skip
    final Object extant = servletRequest.getAttribute(AUTH_STATE_KEY);
    if (extant != null &&
            extant instanceof AuthState &&
            ((AuthState) extant).getStatus() == AuthState.AuthStatus.SUCCESS) {
      chain.doFilter(servletRequest, servletResponse);
      return;
    }

    if (!bypass_methods.isEmpty()) {
      final String method = ((HttpServletRequest) servletRequest).getMethod();
      for (int i = 0; i < bypass_methods.size(); i++) {
        if (method.equals(bypass_methods.get(i))) {
          stats.incrementCounter(BYPASS_METHOD_METRIC, tags);
          servletRequest.setAttribute(AUTH_STATE_KEY, BYPASS_STATE);
          chain.doFilter(servletRequest, servletResponse);
          return;
        }
      }
    }

    // check the literal bypass list first.
    final String uri = ((HttpServletRequest) servletRequest).getRequestURI();
    if (!bypass_literals.isEmpty()) {
      for (int i = 0; i < bypass_literals.size(); i++) {
        if (uri.equals(bypass_literals.get(i))) {
          stats.incrementCounter(BYPASS_URI_METRIC, tags);
          servletRequest.setAttribute(AUTH_STATE_KEY, BYPASS_STATE);
          chain.doFilter(servletRequest, servletResponse);
          return;
        }
      }
    }

    if (!bypass_regex.isEmpty()) {
      for (int i = 0; i < bypass_regex.size(); i++) {
        if (bypass_regex.get(i).matcher(uri).find()) {
          stats.incrementCounter(BYPASS_URI_METRIC, tags);
          servletRequest.setAttribute(AUTH_STATE_KEY, BYPASS_STATE);
          chain.doFilter(servletRequest, servletResponse);
          return;
        }
      }
    }

    // let this filter check the request.
    runFilter(servletRequest, servletResponse, chain);
  }

  @Override
  public void init(final FilterConfig filterConfig) throws ServletException {
    // defaults to a no-op
  }

  @Override
  public void destroy() {
    // defaults to a no-op
  }

  /**
   * Dynamic reload callback that will swap out the references to the bypass
   * lists when updated.
   */
  private class SettingsCallback implements ConfigurationCallback<Object> {

    @Override
    public void update(final String key, final Object value) {
      if (key.equals(BYPASS_LITERALS_KEY)) {
        if (value == null || ((List<String>) value).isEmpty()) {
          bypass_literals = Collections.emptyList();
        } else {
          bypass_literals = (List<String>) value;
        }
      } else if (key.equals(BYPASS_REGEX_KEY)) {
        if (value == null || ((List<String>) value).isEmpty()) {
          bypass_regex = Collections.emptyList();
        } else {
          final List<String> patterns = (List<String>) value;
          final List<Pattern> compiled =
                  Lists.newArrayListWithExpectedSize(patterns.size());
          try {
            for (final String pattern : patterns) {
              final Pattern temp = Pattern.compile(pattern);
              compiled.add(temp);
            }
            bypass_regex = compiled;
          } catch (PatternSyntaxException e) {
            LOG.error("Failed to parse allow list pattern. Flushing all " +
                    "allow list patterns.", e);
            bypass_regex = Collections.emptyList();
          }
        }
      } else if (key.equals(BYPASS_METHODS_KEY)) {
        if (value == null || ((List<String>) value).isEmpty()) {
          bypass_methods = Collections.emptyList();
        } else {
          bypass_methods = (List<String>) value;
        }
      }
    }

  }

  protected void registerConfigs(final TSDB tsdb) {
    if (!tsdb.getConfig().hasProperty(BYPASS_LITERALS_KEY)) {
      tsdb.getConfig().register(
              ConfigurationEntrySchema.newBuilder()
                      .setKey(BYPASS_LITERALS_KEY)
                      .setType(LIST_REF)
                      .setDescription("An optional list of literal paths to bypass "
                              + "authentication for, e.g. '/status.html'")
                      .isNullable()
                      .setSource(getClass().getName())
                      .build());
    }
    if (!tsdb.getConfig().hasProperty(BYPASS_REGEX_KEY)) {
      tsdb.getConfig().register(
              ConfigurationEntrySchema.newBuilder()
                      .setKey(BYPASS_REGEX_KEY)
                      .setType(LIST_REF)
                      .setDescription("An optional list of regular expressions to bypass "
                              + "authentication for, e.g. '/api/.*'")
                      .isNullable()
                      .setSource(getClass().getName())
                      .build());
    }
    if (!tsdb.getConfig().hasProperty(BYPASS_METHODS_KEY)) {
      tsdb.getConfig().register(
              ConfigurationEntrySchema.newBuilder()
                      .setKey(BYPASS_METHODS_KEY)
                      .setType(LIST_REF)
                      .setDescription("An optional list of HTTP methods to " +
                              "bypass auth for, e.g. 'HEAD'")
                      .isNullable()
                      .setSource(getClass().getName())
                      .build());
    }
    final SettingsCallback callback = new SettingsCallback();
    tsdb.getConfig().bind(BYPASS_LITERALS_KEY, callback);
    tsdb.getConfig().bind(BYPASS_REGEX_KEY, callback);
  }

  /**
   * The plugin implementation that will be called if we haven't hit on a valid
   * authentication method yet.
   *
   * @param servletRequest The non-null request.
   * @param servletResponse The non-null response.
   * @param chain The non-null filter chain to continue on to.
   * @throws IOException If something goes pear shaped.
   * @throws ServletException If something else goes pear shaped.
   */
  protected abstract void runFilter(final ServletRequest servletRequest,
                                    final ServletResponse servletResponse,
                                    final FilterChain chain) throws IOException, ServletException;
}
