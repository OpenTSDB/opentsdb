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
package net.opentsdb.servlet.filter;

import com.google.common.collect.Lists;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.AuthState.AuthStatus;
import net.opentsdb.auth.Authorization;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.configuration.ConfigurationEntrySchema.Builder;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.auth.BaseAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.security.Principal;
import java.util.List;

/**
 * A filter that takes a list of two or more {@link AuthFilter}s and examines
 * each in order until it finds a valid auth state or they all fail. Filters
 * are processed in order. If none of the filters are valid then the last filter
 * in the list is called to respond to the client.
 *
 * @since 3.0
 */
public class MultiAuthFilter extends BaseAuthenticationPlugin {
  protected static final Logger LOG = LoggerFactory.getLogger(MultiAuthFilter.class);

  public static final String TYPE = "MultiAuthFilter";
  public static final String FILTERS_KEY = "multiauth.filter.filters";

  private List<AuthFilter> filters;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    return super.initialize(tsdb, id).addCallback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        registerConfigs(tsdb);

        final List<String> filterIds = tsdb.getConfig().getTyped(FILTERS_KEY, List.class);
        if (filterIds == null || filterIds.isEmpty()) {
          return Deferred.fromError(new IllegalArgumentException(
                  "Filter IDs list must have one or more entries."));
        }
        filters = Lists.newArrayListWithExpectedSize(filterIds.size());
        for (final String plugin : filterIds) {
          final AuthFilter authFilter = tsdb.getRegistry().getPlugin(AuthFilter.class, plugin);
          if (authFilter == null) {
            LOG.error("No auth filter plugin found for {}", plugin);
            return Deferred.fromError(new IllegalArgumentException(
                    "No auth filter plugin found for " + plugin));
          }

          // recursion is a no-no
          if (authFilter instanceof MultiAuthFilter) {
            return Deferred.fromError(new IllegalArgumentException(
                    "Recursive or nested multi-auth filters are not allowed."));
          }

          filters.add(authFilter);
          LOG.info("Loaded plugin {} for multi-auth.", plugin);
        }
        return null;
      }
    });
  }

  @Override
  protected void runFilter(final ServletRequest servletRequest,
                           final ServletResponse servletResponse,
                           final FilterChain chain) throws IOException, ServletException {
    for (int i = 0; i < filters.size(); i++) {
      final AuthFilter filter = filters.get(i);
      final AuthState state = filter.authenticate(servletRequest);
      if (state == null) {
        continue;
      }

      if (state.getStatus() != AuthStatus.SUCCESS) {
        continue;
      }

      // yay!
      servletRequest.setAttribute(AUTH_STATE_KEY, state);
      HttpServletRequestWrapper wrapper =
              new HttpServletRequestWrapper((HttpServletRequest) servletRequest) {
        @Override
        public Principal getUserPrincipal() {
          return state.getPrincipal();
        }
      };

      chain.doFilter(wrapper, servletResponse);
    }

    // nothing so re-do the last one.
    filters.get(filters.size() - 1).doFilter(servletRequest, servletResponse, chain);
  }

  @Override
  public Authorization authorization() {
    return null;
  }

  @Override
  public String type() {
    return TYPE;
  }

  @Override
  protected void registerConfigs(final TSDB tsdb) {
    super.registerConfigs(tsdb);

    if (!tsdb.getConfig().hasProperty(FILTERS_KEY)) {
      tsdb.getConfig().register(ConfigurationEntrySchema.newBuilder()
              .setKey(FILTERS_KEY)
              .setDefaultValue(Lists.newArrayList())
              .setDescription("A list of auth filter plugin IDs to be " +
                      "processed in order.")
              .setType(List.class)
              .build());
    }
  }

  @Override
  public AuthState authenticate(ServletRequest servletRequest) {
    throw new IllegalStateException(
            "Nested or recursive multi-auth filters are not allowed.");
  }
}
