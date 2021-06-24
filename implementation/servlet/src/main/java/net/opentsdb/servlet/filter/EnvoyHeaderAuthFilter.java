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

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import net.opentsdb.auth.AuthState;
import net.opentsdb.auth.Authorization;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.auth.BaseAuthenticationPlugin;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.Principal;


/**
 * A filter that processes MTLS certificates from Athenz to authenticate a
 * caller.
 *
 * @since 3.0
 */
public class EnvoyHeaderAuthFilter extends BaseAuthenticationPlugin {
  protected static final Logger LOG = LoggerFactory.getLogger(
          EnvoyHeaderAuthFilter.class);
  public static final String ENVOY_HEADER_KEY = "envoy_header_key";
  public static final String TYPE = "EnvoyHeaderAuthFilter";
  public static final String TOKEN_TYPE = "X509";
  public Principal principal;



  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    return super.initialize(tsdb, id).addCallback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        registerConfigs(tsdb);
        LOG.info("Successfully loaded Envoy Header authentication filter.");
        return null;
      }
    });
  }

  @Override
  protected void registerConfigs(final TSDB tsdb) {
    super.registerConfigs(tsdb);
    Configuration config = tsdb.getConfig();
    if (!config.hasProperty(ENVOY_HEADER_KEY)) {
      config.register(ENVOY_HEADER_KEY, "x-forwarded-client-cert", true,
              "The header name of envoy certificates.");
    }

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
  protected void runFilter(final ServletRequest servletRequest,
                           final ServletResponse servletResponse,
                           final FilterChain chain) throws IOException, ServletException {

    final AuthState state = authenticate(servletRequest);

    if (state != null) {
      servletRequest.setAttribute(AUTH_STATE_KEY, state);
      HttpServletRequestWrapper wrapper =
              new HttpServletRequestWrapper((HttpServletRequest) servletRequest) {
                @Override
                public java.security.Principal getUserPrincipal() {
                  return state.getPrincipal();
                }
              };
      chain.doFilter(wrapper, servletResponse);
    }
    else {
      sendResponse((HttpServletResponse) servletResponse, 403,
              "Missing or invalid header ");
    }
  }

  @Override
  public AuthState authenticate(final ServletRequest servletRequest) {

    String header = ((HttpServletRequest) servletRequest).getHeader(tsdb.getConfig().getString(ENVOY_HEADER_KEY));
    if (header != null) {
      if (header.contains("Subject")){
        String headerSplit=header.substring(header.indexOf("Subject"),header.length());
        if (headerSplit.contains("CN")){
          String metaInfo=headerSplit.substring(headerSplit.indexOf("CN"),headerSplit.indexOf(","));
          String commonName=metaInfo.split("=")[1];

          principal = new Principal() {
            @Override
            public String getName() {
              return commonName;
            }
          };

          return new HeaderAuthState(principal);
        }
        else {
          LOG.debug("Missing the Common name in the header");
          return null;
        }
      }
      else{
        LOG.debug("Missing the Subject name in the header");
        return null;
      }

    }
    LOG.debug("Missing header");
    return null;
  }


  /**
   * Helper to return a status and/or content to the caller.
   *
   * @param response The non-null response to fill int.
   * @param status The HTTP status code.
   * @param content The non-null content to send.
   */
  protected void sendResponse(final HttpServletResponse response,
                              final int status,
                              final String content) {
    response.setStatus(status);
    response.setContentLength(content.getBytes().length);
    try {
      response.getWriter().print(content);
      response.getWriter().flush();
    } catch (IOException e) {
      LOG.error("Failed to write to http stream", e);
    }
  }

  /**
   * TSD Auth state for the user.
   */
  class HeaderAuthState implements AuthState {

    protected final Principal principal;

    HeaderAuthState(final Principal principal) {
      this.principal = principal;
    }

    @Override
    public String getUser() {
      return principal.getName();
    }

    @Override
    public java.security.Principal getPrincipal() {
      return () -> principal.getName();
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
      return TOKEN_TYPE;
    }

    @Override
    public byte[] getToken() {
      // not exposing the user's token right now.
      return null;
    }

    @Override
    public boolean hasRole(String role) {
      throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean hasPermission(String action) {
      throw new UnsupportedOperationException("TODO");
    }
  }
}
