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
package net.opentsdb.auth;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.yahoo.athenz.auth.Principal;
import com.yahoo.athenz.auth.impl.CertificateAuthority;
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
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

/**
 * A filter that processes MTLS certificates from Athenz to authenticate a
 * caller.
 *
 * @since 3.0
 */
public class AthenzMTLSAuthFilter extends BaseAuthenticationPlugin {
  protected static final Logger LOG = LoggerFactory.getLogger(
          AthenzMTLSAuthFilter.class);

  public static final String TYPE = "AthenzMTLSAuthFilter";

  public static final String CA_BUNDLE_KEY = "athenz.ca.path";
  public static final String CA_BUNDLE_PASS_KEY = "athenz.ca.pass";
  public static final String JAVAX_CERT_ATTR =
          "javax.servlet.request.X509Certificate";
  public static final String TOKEN_TYPE = "X509";

  private CertificateAuthority ca;
  private List<Certificate> caCerts;

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    return super.initialize(tsdb, id).addCallback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        registerConfigs(tsdb);

        ca = new CertificateAuthority();
        ca.initialize();

        caCerts = loadCABundle(tsdb.getConfig().getString(CA_BUNDLE_KEY),
                tsdb.getConfig().getString(CA_BUNDLE_PASS_KEY));
        LOG.info("Successfully loaded Athenz MTLS authentication filter.");
        return null;
      }
    });
  }

  @Override
  protected void registerConfigs(final TSDB tsdb) {
    super.registerConfigs(tsdb);

    Configuration config = tsdb.getConfig();
    if (!config.hasProperty(CA_BUNDLE_KEY)) {
      config.register(CA_BUNDLE_KEY, null, false,
              "The path to a JKS bundle with the CA certs for Athenz signed certificates.");
    }
    if (!config.hasProperty(CA_BUNDLE_PASS_KEY)) {
      config.register(CA_BUNDLE_PASS_KEY, null, false,
              "The password for the CA certs bundle.");
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

    sendResponse((HttpServletResponse) servletResponse, 403,
            "Missing or invalid certificate.");
  }

  @Override
  public AuthState authenticate(final ServletRequest servletRequest) {
    X509Certificate[] certs = (X509Certificate[]) servletRequest.getAttribute(
            JAVAX_CERT_ATTR);
    StringBuilder errorMsg = new StringBuilder();
    Principal principal = ca.authenticate(certs, errorMsg);
    if (principal != null) {
      boolean matched = false;
      for (int i = 0; i < caCerts.size(); i++) {
        final Certificate caCert = caCerts.get(i);
        try {
          principal.getX509Certificate().verify(caCert.getPublicKey());
          principal.getX509Certificate().checkValidity();
          matched = true;
          break;
        } catch (Exception e) {
          // ignored
        }
      }

      if (!matched) {
        return null;
      }

      return new AthenzAuthState(principal);
    }
    return null;
  }

  /**
   * Attempts to load the ca bundle.
   * @param athensCABundle The path to the bunder.
   * @param athensCABundlePassword The password for the bundle.
   * @return A list of CA certs in the bundle if successfully read.
   * @throws Exception If something goes pear shaped.
   */
  protected List<Certificate> loadCABundle(final String athensCABundle,
                                           final String athensCABundlePassword)
          throws Exception {

    List<Certificate> caCerts = new ArrayList<Certificate>();
    KeyStore keyStore = KeyStoreUtil.loadKeyStore(athensCABundle, athensCABundlePassword);
    Enumeration<String> aliases = keyStore.aliases();
    while (aliases.hasMoreElements()) {
      String alias = aliases.nextElement();
      caCerts.add(keyStore.getCertificate(alias));
    }
    return caCerts;
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
  class AthenzAuthState implements AuthState {

    protected final Principal principal;

    AthenzAuthState(final Principal principal) {
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
