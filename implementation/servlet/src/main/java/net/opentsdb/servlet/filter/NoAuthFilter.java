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

import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authorization;

import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.io.IOException;
import java.security.Principal;

/**
 * Simple filter that sets a "NoAuth" principal.
 *
 * @since 3.0
 */
public class NoAuthFilter implements AuthFilter {
  private final Principal principal;
  private final AuthState authState;

  public NoAuthFilter() {
    principal = new Principal() {
      @Override
      public String getName() {
        return "user.noauth";
      }
    };

    authState = new AuthState() {
      @Override
      public String getUser() {
        return principal.getName();
      }

      @Override
      public Principal getPrincipal() {
        return principal;
      }

      @Override
      public AuthStatus getStatus() {
        return AuthStatus.UNAUTHORIZED;
      }

      @Override
      public String getMessage() {
        return "NoAuth filter is loaded.";
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
      public boolean hasRole(String role) {
        return false;
      }

      @Override
      public boolean hasPermission(String action) {
        return false;
      }
    };
  }

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {

  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    HttpServletRequestWrapper wrapper = new HttpServletRequestWrapper((HttpServletRequest) request) {
      @Override
      public Principal getUserPrincipal() {
        return principal;
      }
    };
    request.setAttribute(AUTH_STATE_KEY, authState);
    chain.doFilter(wrapper, response);
  }

  @Override
  public void destroy() {

  }

  @Override
  public Authorization authorization() {
    return null;
  }

  @Override
  public AuthState authenticate(ServletRequest servletRequest) {
    return authState;
  }
}
