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

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import io.jsonwebtoken.security.SignatureException;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.auth.BaseAuthenticationPlugin;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.utils.Bytes;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.SharedHttpClient;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.crypto.SecretKey;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import java.util.Date;
import java.util.Map;
import java.util.Random;
import java.util.SecureRandom;
import java.util.concurrent.ExecutionException;

/**
 * A custom Oauth2 filter for working with Okta for authentication of user
 * accounts. Note that because we don't track the nonce in the written cookie
 * we can't validate it at all times, hence Okta's jwt-verifier fails when
 * validating the ID token. Useres are redirected to Okta's servers to supply
 * their credentials.
 * <p>
 * Currently, the user is identified by a "short_id" field in Okta's config.
 * TODO - we need some code to pick the Id field to parse and use as not everyone
 * may have the same Okta config.
 * <p>
 * Note that the calls out to Okta for configuration, keys and tokens are
 * blocking at this point so it will tie up the server thread for a bit. It also
 * delays or blocks startup of the TSD if we can't get to the configured Okta
 * endpoints.
 * <p>
 * This could all be slightly wrong so we should replace it at some point. And
 * besides, we're not using the Undertow authentication methods (partly because
 * we could be running under another type of server) and there may be a more
 * JAXy way to do this.
 *
 * TODO - May want more fields from Okta.
 * TODO - Better retries.
 * TODO - Async the remote calls possibly.
 *
 * @since 3.0
 */
public class OktaIDCookieAuthFilter extends BaseAuthenticationPlugin {
  protected static final Logger LOG = LoggerFactory.getLogger(
          OktaIDCookieAuthFilter.class);

  public static final String TYPE = "OktaIDCookieAuthFilter";

  public static final String CLIENT_KEY = "okta.shared.httpclient.id";
  public static final String ID_COOKIE_KEY = "okta.cookie.id.name";
  public static final String AT_COOKIE_KEY = "okta.cookie.at.name";
  public static final String CLIENT_ID_KEY = "okta.client.id";
  public static final String CLIENT_SECRET_KEY = "okta.client.secret.key";
  public static final String CALLBACK_URL_KEY = "okta.callback.url";
  public static final String DOMAIN_KEY = "okta.domain";
  public static final String EXPIRATION_KEY = "okta.cookie.expiration";
  public static final String OPENID_CONNECT_ENDPOINT_KEY =
          "okta.openid.connect.endpoint";
  public static final String TOKEN_TYPE = "JWT";

  // how often we redirect to the login page.
  public static final String LOGIN_REDIRECTS_METRIC = "auth.okta.login.redirects";
  // how often we get a callback
  public static final String AUTHZ_ATTEMPTS_METRIC = "auth.okta.authorization.attempts";
  // auth failed with Okta due to invalid creds/tokens, etc.
  public static final String AUTHZ_FAILED_METRIC = "auth.okta.authorization.failed";
  public static final String AUTHZ_SUCCESS_METRIC = "auth.okta.authorization.success";
  public static final String AUTHZ_ERROR_METRIC = "auth.okta.idtoken.error";
  public static final String PUBKEY_FETCH_METRIC = "auth.okta.publickey.fetches";
  public static final String EXPIRED_METRIC = "auth.okta.idtoken.expired";
  public static final String INVALID_METRIC = "auth.okta.idtoken.invalid";
  public static final String VALID_METRIC = "auth.okta.idtoken.valid";
  // something went wrong with our logic.
  public static final String PLUGIN_ERROR = "auth.okta.plugin.error";

  protected final Random random;
  protected String id_cookie;
  protected String at_cookie;
  protected String oidConnectEndpoint;
  protected CloseableHttpAsyncClient client;
  protected volatile JsonNode oidConfiguration;
  protected Map<String, Key> publicKeys;

  /**
   * Default ctor.
   */
  public OktaIDCookieAuthFilter() {
    super();
    random = new SecureRandom();
  }

  @Override
  public Deferred<Object> initialize(final TSDB tsdb, final String id) {
    return super.initialize(tsdb, id).addCallback(new Callback<Object, Object>() {
      @Override
      public Object call(Object arg) throws Exception {
        registerConfigs(tsdb);

        if (!tsdb.getConfig().hasProperty(CLIENT_KEY)) {
          tsdb.getConfig().register(CLIENT_KEY,
                  null, false,
                  "The ID of the SharedHttpClient plugin to use. " +
                          "Defaults to `null`.");
        }
        final String client_id = tsdb.getConfig().getString(CLIENT_KEY);
        final SharedHttpClient shared_client = tsdb.getRegistry().getPlugin(
                SharedHttpClient.class, client_id);
        if (shared_client == null) {
          throw new IllegalArgumentException("No shared HTTP client found "
                  + "for ID: " + (Strings.isNullOrEmpty(client_id) ?
                  "Default" : client_id));
        } else {
          client = shared_client.getClient();
        }

        id_cookie = tsdb.getConfig().getString(ID_COOKIE_KEY);
        at_cookie = tsdb.getConfig().getString(AT_COOKIE_KEY);
        oidConnectEndpoint = tsdb.getConfig().getString(OPENID_CONNECT_ENDPOINT_KEY);
        if (oidConnectEndpoint.endsWith("/")) {
          oidConnectEndpoint = oidConnectEndpoint.substring(0, oidConnectEndpoint.length() - 1);
        }

        fetchOIDConfiguration();
        if (oidConfiguration == null) {
          return Deferred.fromError(new RuntimeException("Unable to initialize the " +
                  "Okta plugin."));
        }

        publicKeys = Maps.newConcurrentMap();
        fetchPublicKeys();
        if (publicKeys.isEmpty()) {
          return Deferred.fromError(new RuntimeException("Unable to initialize the " +
                  "Okta plugin."));
        }
        return null;
      }
    });
  }

  @Override
  public void runFilter(final ServletRequest servletRequest,
                        final ServletResponse servletResponse,
                        final FilterChain chain) throws IOException, ServletException {
    final HttpServletRequest request = (HttpServletRequest) servletRequest;
    final HttpServletResponse response = (HttpServletResponse) servletResponse;

    final Cookie cookie = findCookie(request, tsdb.getConfig()
            .getString(ID_COOKIE_KEY));
    HttpServletRequestWrapper wrapper = null;
    if (cookie == null) {
      String requestedResource = request.getRequestURL().toString();
      if (requestedResource.equals(tsdb.getConfig().getString(CALLBACK_URL_KEY))) {
        stats.incrementCounter(AUTHZ_ATTEMPTS_METRIC, tags);
        handleOauthCallback(request, response);
      } else {
        stats.incrementCounter(LOGIN_REDIRECTS_METRIC, tags);
        response.sendRedirect(buildLoginRedirectURL(request));
      }
      return;
    } else {
      try {
        final String shortId = validateIdToken(cookie.getValue(), null);
        if (shortId == null) {
          // purge cookies
          expireCookie(id_cookie, response);
          expireCookie(at_cookie, response);
          stats.incrementCounter(LOGIN_REDIRECTS_METRIC, tags);
          response.sendRedirect(buildLoginRedirectURL(request));
          return;
        } else {
          final OktaAuthState state = new OktaAuthState(shortId);
          request.setAttribute(AUTH_STATE_KEY, state);
          wrapper = new HttpServletRequestWrapper((HttpServletRequest) request) {
            @Override
            public Principal getUserPrincipal() {
              return state.getPrincipal();
            }
          };
          stats.incrementCounter(VALID_METRIC, tags);
        }
      } catch (Throwable e) {
        // purge cookies
        expireCookie(id_cookie, response);
        expireCookie(at_cookie, response);
        LOG.error("Failed to validate Okta token", e);
        sendResponse(response, 401, "Cookie validation failed.");
        stats.incrementCounter(PLUGIN_ERROR, tags);
        return;
      }
    }

    chain.doFilter(wrapper != null ? wrapper : servletRequest, servletResponse);
  }

  @Override
  public AuthState authenticate(final ServletRequest servletRequest) {
    final HttpServletRequest request = (HttpServletRequest) servletRequest;
    final Cookie cookie = findCookie(request, tsdb.getConfig()
            .getString(ID_COOKIE_KEY));
    if (cookie == null) {
      return null;
    }

    final String shortId = validateIdToken(cookie.getValue(), null);
    if (shortId == null) {
      return null;
    }

    return new OktaAuthState(shortId);
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

    Configuration config = tsdb.getConfig();
    if (!config.hasProperty(ID_COOKIE_KEY)) {
      config.register(ID_COOKIE_KEY, "okta_it", false,
              "The ID of the ID Okta cookie.");
    }
    if (!config.hasProperty(AT_COOKIE_KEY)) {
      config.register(AT_COOKIE_KEY, "okta_at", false,
              "The ID of the Authorization Okta cookie.");
    }
    if (!config.hasProperty(CLIENT_ID_KEY)) {
      config.register(CLIENT_ID_KEY, null, false,
              "The Okta client ID for this application.");
    }
    if (!config.hasProperty(CLIENT_SECRET_KEY)) {
      config.register(CLIENT_SECRET_KEY, null, false,
              "The ID of a secret key that contains the protected " +
                      "Okta client signing key.");
    }
    if (!config.hasProperty(CALLBACK_URL_KEY)) {
      config.register(CALLBACK_URL_KEY, null, true,
              "The URL of the authentication callback that Okta should" +
                      " redirect to after the user supplies their credentials to" +
                      " Okta.");
    }
    if (!config.hasProperty(DOMAIN_KEY)) {
      config.register(DOMAIN_KEY, null, false,
              "The domain for the cookies.");
    }
    if (!config.hasProperty(EXPIRATION_KEY)) {
      config.register(EXPIRATION_KEY, 60 * 60, true,
              "The expiration time, in seconds, of the cookies.");
    }
    if (!config.hasProperty(OPENID_CONNECT_ENDPOINT_KEY)) {
      config.register(OPENID_CONNECT_ENDPOINT_KEY, null, true,
              "The Okta endpoint to hit for configuration.");
    }
  }

  /**
   * TSD Auth state for the user.
   */
  class OktaAuthState implements AuthState {

    protected final String username;

    OktaAuthState(final String username) {
      this.username = username;
    }

    @Override
    public String getUser() {
      return username;
    }

    @Override
    public Principal getPrincipal() {
      return () -> username;
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

  /**
   * Iterates through the cookies from the request and returns the cookie with
   * the given name if found.
   * @param request The non-null request to pull cookies from.
   * @param cookieName The non-null and non-empty cookie name to match.
   * @return The cookie if found, null if not.
   */
  protected Cookie findCookie(final HttpServletRequest request,
                              final String cookieName) {
    final Cookie[] cookies = request.getCookies();
    if (cookies == null || cookies.length < 1) {
      return null;
    }

    for (int i = 0; i < cookies.length; i++) {
      if (cookies[i].getName().equals(cookieName)) {
        return cookies[i];
      }
    }
    return null;
  }

  /**
   * Sets the cookie in the response.
   * @param cookieName The non-null and non-empty cookie name.
   * @param token The data to store in the cookie. May be null.
   * @param response The non-null response to set the cookie on.
   */
  protected void setCookie(final String cookieName,
                           final String token,
                           final HttpServletResponse response) {
    final Cookie cookie = new Cookie(cookieName, token);
    cookie.setSecure(true);
    cookie.setDomain(tsdb.getConfig().getString(DOMAIN_KEY));
    cookie.setHttpOnly(true);
    cookie.setMaxAge(tsdb.getConfig().getInt(EXPIRATION_KEY));
    cookie.setPath("/");
    response.addCookie(cookie);
  }

  /**
   * Sets a cookie on the response with a 0 age and empty data.
   * @param cookieName The non-null and non-empty cookie name.
   * @param response The non-null response to set the cookie on.
   */
  protected void expireCookie(final String cookieName,
                              final HttpServletResponse response) {
    Cookie cookie = new Cookie(cookieName, null);
    cookie.setSecure(true);
    cookie.setDomain(tsdb.getConfig().getString(DOMAIN_KEY));
    cookie.setHttpOnly(true);
    cookie.setMaxAge(0);
    cookie.setPath("/");
    response.addCookie(cookie);
  }

  /**
   * Processes the callback from Okta after a user supplies their credentials.
   * This will send an access request to the Okta service to get the ID token
   * and then validate said token.
   *
   * @param request The non-null request to pull the code and state from.
   * @param response The non-null response to answer to.
   */
  protected void handleOauthCallback(final HttpServletRequest request,
                                     final HttpServletResponse response) {
    // handle the callback
    final String code = request.getParameter("code");
    final String state = request.getParameter("state");
    if (null == code || code.isEmpty() || null == state || state.isEmpty()) {
      stats.incrementCounter(AUTHZ_ERROR_METRIC, tags);
      sendResponse(response, HttpServletResponse.SC_FORBIDDEN,
              "Authorization failed.");
      return;
    }

    final String token = sendAuthorizationToken(code, state);
    if (Strings.isNullOrEmpty(token)) {
      sendResponse(response, HttpServletResponse.SC_FORBIDDEN,
              "Unable to obtain token.");
      return;
    }

    try {
      final JsonNode root = JSON.getMapper().readTree(token);
      final String idToken = root.get("id_token").asText();
      final String accessToken = root.get("access_token").asText();

      String[] stateArray = state.split("_nonce_");
      final String nonce;
      final String redirect;
      if (stateArray != null && stateArray.length > 0) {
        redirect = stateArray[0];
        nonce = stateArray[1];
      } else {
        redirect = null;
        nonce = null;
      }

      String shortId = validateIdToken(idToken, nonce);
      if (shortId != null) {
        setCookie(id_cookie, idToken, response);
        setCookie(at_cookie, accessToken, response);
        stats.incrementCounter(AUTHZ_SUCCESS_METRIC, tags);
        response.sendRedirect(redirect);
      } else {
        expireCookie(id_cookie, response);
        expireCookie(at_cookie, response);
        stats.incrementCounter(AUTHZ_FAILED_METRIC, tags);
        sendResponse(response, HttpServletResponse.SC_FORBIDDEN,
                "Authorization failed.");
        return;
      }
    } catch (IOException e) {
      LOG.error("Unexpected exception", e);
      sendResponse(response, HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
              "Exception while authenticating.");
    }
  }

  /**
   * Validates and extracts the "short_id" field from the ID token if the
   * token is valid. Note that we need to change this for other Okta users as
   * their fields may be different.
   *
   * @param token The non-null and non-empty token to validate.
   * @param nonce The optional nonce to validate.
   * @return The user ID if valid, null if not.
   */
  protected String validateIdToken(final String token,
                                   final String nonce) {
    try {
      final String[] parts = token.split("\\.");
      if (parts.length != 3) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Invalid okta token. Only had {} parts instead of 3",
                  parts.length);
        }
        stats.incrementCounter(INVALID_METRIC, tags);
        return null;
      }

      final String decoded = new String(Base64.getDecoder()
              .decode(parts[0].getBytes()));
      final JsonNode idNodeRoot = JSON.getMapper().readTree(decoded);
      String keyId = idNodeRoot.get("kid").asText();
      if (keyId == null) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Invalid okta token. No key ID found in node");
        }
        stats.incrementCounter(INVALID_METRIC, tags);
        return null;
      }
      keyId = keyId.replace("\"", "");
      // retry with backoff
      for (int i = 0; i < 4; i++) {
        if (i > 0) {
          Thread.sleep(i * i * 100);
        }

        try {
          final Key key = publicKeys.get(keyId);
          if (key == null) {
            LOG.error("No key in the public keys list for the ID given in " +
                    "the token.");
            stats.incrementCounter(INVALID_METRIC, tags);
            continue;
          }
          final Claims claims = Jwts.parserBuilder()
                  .setSigningKey(key)
                  .build()
                  .parseClaimsJws(token)
                  .getBody();
          System.out.println(claims);
          if (nonce != null) {
            final String tokenNonce = claims.get("nonce", String.class);
            if (!nonce.equals(tokenNonce)) {
              if (LOG.isDebugEnabled()) {
                LOG.debug("Nonce missmatch");
              }
              stats.incrementCounter(INVALID_METRIC, tags);
              return null;
            }
          }

          if (!claims.getAudience().equals(
                  tsdb.getConfig().getString(CLIENT_ID_KEY))) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Audience/Client ID missmatch");
            }
            stats.incrementCounter(INVALID_METRIC, tags);
            return null;
          }

          if (!claims.getIssuer().equals(oidConnectEndpoint)) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Issuer missmatch");
            }
            stats.incrementCounter(INVALID_METRIC, tags);
            return null;
          }

          return claims.get("short_id", String.class);
        } catch (SignatureException e) {
          LOG.warn("Signature was invalid. Trying to refresh the public keys.");
          fetchPublicKeys();
        } catch (ExpiredJwtException e) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Token has expired");
          }
          stats.incrementCounter(EXPIRED_METRIC, tags);
          return null;
        }
      }

      return null;
    } catch (Exception e) {
      LOG.error("Failed to parse or validate token", e);
      stats.incrementCounter(AUTHZ_ERROR_METRIC, tags);
      return null;
    }
  }

  /**
   * Creates a signed authorization token request and sends it off to Okta in a
   * blocking fashion. An ID token is the result if successful.
   *
   * @param code The non-null and non-empty authorization code.
   * @param state The non-null and non-empty state.
   * @return The ID token if access was granted, null if not.
   */
  protected String sendAuthorizationToken(final String code, final String state) {
    try {
      final byte[] clientSecret = tsdb.getConfig()
              .getSecretBytes(CLIENT_SECRET_KEY);
      final SecretKey secretKey = Keys.hmacShaKeyFor(clientSecret);
      final String clientId = tsdb.getConfig().getString(CLIENT_ID_KEY);
      String clientAssertion = Jwts.builder()
              .setSubject(clientId)
              .setExpiration(new Date(System.currentTimeMillis() + 3600_000))
              .setAudience(oidConfiguration.get("token_endpoint").asText())
              .setIssuer(clientId)
              .signWith(secretKey)
              .compact();

      StringBuilder tokenBuilder = new StringBuilder();
      tokenBuilder.append("grant_type=")
              .append("authorization_code")
              .append("&code=")
              .append(URLEncoder.encode(code, "UTF-8"))
              .append("&state=")
              .append(URLEncoder.encode(code, "UTF-8"))
              .append("&state=")
              .append(URLEncoder.encode(code, "UTF-8"))
              .append("&scope=openid")
              .append("&state=")
              .append(URLEncoder.encode(state, "UTF-8"))
              .append("&client_assertion=")
              .append(URLEncoder.encode(clientAssertion, "UTF-8"))
              .append("&redirect_uri=")
              .append(tsdb.getConfig().getString(CALLBACK_URL_KEY))
              .append("&client_assertion_type=")
              .append(URLEncoder.encode(
                      "urn:ietf:params:oauth:client-assertion-type:jwt-bearer",
                      "UTF-8"));

      final HttpPost post = new HttpPost(oidConfiguration
              .get("token_endpoint").asText());
      post.setEntity(new StringEntity(tokenBuilder.toString()));
      post.setHeader("Accept", "application/json");
      post.setHeader("Content-Type", "application/x-www-form-urlencoded");


      final HttpResponse result = client.execute(post, null).get();
      try {
        if (result.getStatusLine().getStatusCode() != 200) {
          stats.incrementCounter(AUTHZ_FAILED_METRIC, tags);
          LOG.error("Failed to get authorization token from Okta. Status " +
                          "code {}. Response: {}",
                  result.getStatusLine().getStatusCode(),
                  EntityUtils.toString(result.getEntity()));
        } else {
          final String token = EntityUtils.toString(result.getEntity());
          if (token != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successfully got authorization token from Okta.");
            }
            return token;
          } else {
            stats.incrementCounter(AUTHZ_ERROR_METRIC, tags);
            LOG.error("Empty token validation from Okta.");
            return null;
          }
        }
      } catch (IOException e) {
        stats.incrementCounter(AUTHZ_ERROR_METRIC, tags);
        LOG.error("Failure validating token with Okta", e);
      }
      return null;
    } catch (Exception e) {
      stats.incrementCounter(AUTHZ_ERROR_METRIC, tags);
      LOG.error("Unexpected exception validating token from Okta", e);
      return null;
    }
  }

  /**
   * Helper to build the login redirect URL.
   *
   * @param request The non-null request to pull the redirect from.
   * @return The URL to send to Okta.
   */
  protected String buildLoginRedirectURL(final HttpServletRequest request) {
    try {
      StringBuilder authBuilder = new StringBuilder(
              oidConfiguration.get("authorization_endpoint").asText());
      String nonce = UniqueId.uidToString(Bytes.fromLong(random.nextLong()));
      String serverName = request.getServerName();
      int port = request.getServerPort();
      StringBuilder absoluteURL = new StringBuilder();
      absoluteURL.append(request.getScheme())
              .append("://")
              .append(serverName)
              .append(":")
              .append(port)
              .append(request.getRequestURI());
      String queryString = request.getQueryString();
      absoluteURL.append(URLEncoder.encode((queryString == null ? "" :
              "?" + queryString), "UTF-8"))
              .append("_nonce_").append(nonce);
      final String state = absoluteURL.toString();
      authBuilder.append("?")
              .append("response_type")
              .append("=code")
              .append("&response_mode=")
              .append("query")
              .append("&prompt=")
              .append("login")
              .append("&scope=")
              .append("openid")
              .append("&client_id=")
              .append(tsdb.getConfig().getString(CLIENT_ID_KEY))
              .append("&state=")
              .append(state)
              .append("&nonce=")
              .append(nonce)
              .append("&redirect_uri=")
              .append(tsdb.getConfig().getString(CALLBACK_URL_KEY));

      return authBuilder.toString();
    } catch (Exception e) {
      LOG.error("Unexpected exception setting up auth request for Okta", e);
      return null;
    }
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
   * Makes a blocking call to fetch the OID config from Okta.
   */
  protected void fetchOIDConfiguration() {
    final HttpGet get = new HttpGet(oidConnectEndpoint
            + "/.well-known/openid-configuration");
    get.setHeader("Accept", "application/json");
    get.setHeader("Content-Type", "application/x-www-form-urlencoded");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to fetch OID config from: {}", get.toString());
    }
    try {
      HttpResponse result = client.execute(get, null).get();
      try {
        if (result.getStatusLine().getStatusCode() != 200) {
          LOG.error("Failed to fetch OID configuration from Okta with status " +
                          "code {}. Response: {}",
                  result.getStatusLine().getStatusCode(),
                  EntityUtils.toString(result.getEntity()));
          oidConfiguration = null;
        } else {
          oidConfiguration = JSON.getMapper().readTree(
                  EntityUtils.toString(result.getEntity()));
          if (oidConfiguration != null) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Successfully fetched OID configuration from Okta.");
            }
          } else {
            LOG.error("Empty OID configuration from Okta.");
            oidConfiguration = null;
          }
        }
      } catch (IOException e) {
        LOG.error("Failure fetching OID configuration from Okta", e);
        oidConfiguration = null;
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to fetch OID configuration from Okta", e);
      oidConfiguration = null;
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch OID configuration from Okta", e);
      oidConfiguration = null;
    }
  }

  /**
   * Makes a blocking call to fetch the public signing keys from Okta.
   */
  protected void fetchPublicKeys() {
    stats.incrementCounter(PUBKEY_FETCH_METRIC, tags);
    final HttpGet get = new HttpGet(oidConfiguration.get("jwks_uri").asText());
    get.setHeader("Accept", "application/json");
    get.setHeader("Content-Type", "application/x-www-form-urlencoded");
    if (LOG.isDebugEnabled()) {
      LOG.debug("Attempting to fetch public keys from: {}", get.toString());
    }
    try {
      HttpResponse result = client.execute(get, null).get();
      try {
        if (result.getStatusLine().getStatusCode() != 200) {
          LOG.error("Failed to fetch public keys from Okta with status " +
                          "code {}. Response: {}",
                  result.getStatusLine().getStatusCode(),
                  EntityUtils.toString(result.getEntity()));
        } else {
          final JsonNode root = JSON.getMapper().readTree(
                  EntityUtils.toString(result.getEntity()));
          final JsonNode keys = root.findValue("keys");
          if (keys == null) {
            LOG.error("No keys node found in the JSON from Okta.");
            return;
          }

          for (JsonNode keyEntry : keys) {
            final String keyId = keyEntry.get("kid").asText();
            final String e = keyEntry.get("e").asText();
            final String n = keyEntry.get("n").asText();

            final byte[] eBytes = Base64.getUrlDecoder().decode(
                    e.getBytes(StandardCharsets.UTF_8));
            final byte[] nBytes = Base64.getUrlDecoder().decode(
                    n.getBytes(StandardCharsets.UTF_8));

            try {
              final RSAPublicKeySpec keySpec = new RSAPublicKeySpec(
                      new BigInteger(1, nBytes),
                      new BigInteger(1, eBytes));
              final KeyFactory factory = KeyFactory.getInstance("RSA");
              final Key key = factory.generatePublic(keySpec);
              publicKeys.put(keyId, key);
            } catch (NoSuchAlgorithmException ex) {
              LOG.error("No algorithm for public key.", ex);
            } catch (InvalidKeySpecException ex) {
              LOG.error("Invalid key spec for public key.", ex);
            }
          }
        }
      } catch (IOException e) {
        LOG.error("Failure fetching public keysn from Okta", e);
      }
    } catch (InterruptedException e) {
      LOG.error("Failed to fetch public keys from Okta", e);
    } catch (ExecutionException e) {
      LOG.error("Failed to fetch public keys from Okta", e);
    }
  }
}
