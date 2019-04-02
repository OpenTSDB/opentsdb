// This file is part of OpenTSDB.
// Copyright (C) 2017-2019  The OpenTSDB Authors.
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

import com.google.common.base.Strings;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.Undertow.Builder;
import io.undertow.server.HttpHandler;
import io.undertow.server.handlers.accesslog.AccessLogHandler;
import io.undertow.server.handlers.accesslog.AccessLogReceiver;
import io.undertow.server.handlers.encoding.EncodingHandler;
import io.undertow.server.handlers.resource.FileResourceManager;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import io.undertow.servlet.api.FilterInfo;
import io.undertow.servlet.api.InstanceFactory;
import io.undertow.servlet.api.InstanceHandle;
import net.opentsdb.auth.Authentication;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.configuration.ConfigurationEntrySchema;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.servlet.filter.AuthFilter;
import net.opentsdb.stats.BlackholeStatsCollector;
import net.opentsdb.utils.ArgP;
import net.opentsdb.utils.RefreshingSSLContext;
import net.opentsdb.utils.RefreshingSSLContext.SourceType;
import net.opentsdb.version.CoreVersion;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xnio.Options;
import org.xnio.Sequence;
import org.xnio.SslClientAuthMode;

import javax.net.ssl.SSLContext;
import javax.servlet.DispatcherType;
import javax.servlet.Filter;
import javax.servlet.ServletException;

import java.io.File;
import java.util.Map;
import java.util.Map.Entry;

/**
 * A simple main method that instantiates a TSD and the TSD servlet package
 * and serves them with the Undertow HTTP server.
 * 
 * @since 3.0
 */
public class TSDMain {
  private static Logger LOG = LoggerFactory.getLogger(TSDMain.class);
  
  /** Property keys */
  public static final String HTTP_PORT_KEY = "tsd.network.port";
  public static final String TLS_PORT_KEY = "tsd.network.ssl_port";
  public static final String BIND_KEY = "tsd.network.bind";
  public static final String ROOT_KEY = "tsd.http.root";
  public static final String LOAD_PLUGINS_KEY = "tsd.core.load_plugins";
  public static final String KEYSTORE_KEY = "tsd.network.keystore.location";
  public static final String KEYSTORE_PASS_KEY = "tsd.network.keystore.password";
  public static final String TLS_CERT_KEY = "tsd.network.tls.certificate";
  public static final String TLS_KEY_KEY = "tsd.network.tls.key";
  public static final String TLS_CA_KEY = "tsd.network.tls.ca";
  public static final String TLS_VERIFY_CLIENT_KEY = "tsd.network.tls.verify_client";
  public static final String TLS_SECRET_CERT_KEY = "tsd.network.tls.secrets.certificate";
  public static final String TLS_SECRET_KEY_KEY = "tsd.network.tls.secrets.key";
  public static final String TLS_PROTOCOLS_KEY = "tsd.network.tls.protocols";
  public static final String TLS_CIPHERS_KEY = "tsd.network.tls.ciphers";
  public static final String CORS_PATTERN_KEY = "tsd.http.request.cors.pattern";
  public static final String CORS_HEADERS_KEY = "tsd.http.request.cors.headers";
  public static final String DIRECTORY_KEY = "tsd.http.staticroot";
  
  public static final String READ_TO_KEY = "tsd.network.read_timeout";
  public static final String WRITE_TO_KEY = "tsd.network.write_timeout";
  
  /** Defaults */
  public static final String DEFAULT_PATH = "/";
  
  /** The TSD reference. Static so we can shutdown gracefully. */
  private static DefaultTSDB tsdb = null;
  
  /** The Undertow server reference. Static so we can shutdown gracefully. */
  private static Undertow server = null;
  
  /**
   * The main function that does all the fun stuff of instantiating the TSD
   * and passing it to the servlet.
   * @param args CLI args.
   */
  public static void main(final String[] args) {
    try {
      System.in.close();  // Release a FD we don't need.
    } catch (Exception e) {
      LOG.warn("Failed to close stdin", e);
    }
    
    final Configuration config = new Configuration(args);
    
    config.register(HTTP_PORT_KEY, 0, false, 
        "A port to listen on for HTTP requests.");
    config.register(TLS_PORT_KEY, 0, false, 
        "A port to listen on for HTTP over TLS requests");
    config.register(BIND_KEY, "0.0.0.0", false, 
        "The IP to bind listeners to.");
    config.register(ROOT_KEY, DEFAULT_PATH, false, 
        "The root path for HTTP requests.");
    config.register(LOAD_PLUGINS_KEY, true, false, 
        "Whether or not to load plugins on startup.");
    config.register(KEYSTORE_KEY, null, false,
        "The location to a Java keystore file containing the public "
        + "certificate, optional intermediaries and private key for TLS "
        + "server authentication. If set, takes precedence over '" 
        + TLS_CERT_KEY + "' and '" + TLS_KEY_KEY + "'");
    config.register(ConfigurationEntrySchema.newBuilder()
        .isNullable()
        .setSource(TSDMain.class.getCanonicalName())
        .setKey(KEYSTORE_PASS_KEY)
        .setDefaultValue(null)
        .setDescription("The secret used to unlock the keystore set in '" 
            + KEYSTORE_KEY + "'")
        .setType(String.class)
        .isSecret()
        .build());
    config.register(TLS_CERT_KEY, null, false,
        "The location to a PEM formatted file containing the public "
        + "certificate, optional intermediaries and optionally the "
        + "private key. If the private key is not in this file, make "
        + "sure that '" + TLS_KEY_KEY + "' is set.");
    config.register(TLS_KEY_KEY, null, false,
        "The location to a file containing the PKCS#1 or PKCS#8 encoded "
        + "private key. Make sure that '" + TLS_CERT_KEY 
        + "' is also configured.");
    config.register(TLS_SECRET_CERT_KEY, null, false,
        "The secret provider key to a PEM formatted value containing the public "
        + "certificate, optional intermediaries and optionally the "
        + "private key. If the private key is not present, make "
        + "sure that '" + TLS_SECRET_KEY_KEY + "' is set.");
    config.register(TLS_SECRET_KEY_KEY, null, false,
        "The secret provider key containing the PKCS#1 or PKCS#8 encoded "
        + "private key. Make sure that '" + TLS_SECRET_CERT_KEY 
        + "' is also configured.");
    config.register(TLS_CA_KEY, null, false,
        "An optional location to a PEM formatted file containing CA "
        + "certificates.");
    config.register(TLS_VERIFY_CLIENT_KEY, "NOT_REQUESTED", false,
        "Handling of client certificates. Can be 'NOT_REQUESTED', 'REQUESTED' "
        + "or 'REQUIRED'.");
    config.register(TLS_PROTOCOLS_KEY, null, false,
        "A comma separated list of TLS protocols that the server should accept. "
        + "If null or empty, then all TLS protocols are allowed.");
    config.register(TLS_CIPHERS_KEY, null, false,
        "A comma separated list of ciphers that the server should accept for "
        + "TLS connections. If null or empty then all ciphers are allowed.");
    config.register(CORS_PATTERN_KEY, null, false, "A comma separated list "
        + "of domain names to allow access to OpenTSDB when the Origin "
        + "header is specified by the client. If empty, CORS requests "
        + "are passed through without validation. The list may not "
        + "contain the public wildcard * and specific domains at the "
        + "same time.");
    config.register(CORS_HEADERS_KEY, "Authorization, Content-Type, "
        + "Accept, Origin, User-Agent, DNT, Cache-Control, "
        + "X-Mx-ReqToken, Keep-Alive, X-Requested-With, If-Modified-Since", 
        false, 
        "A comma separated list of headers sent to clients when "
        + "executing a CORs request. The literal value of this option "
        + "will be passed to clients.");
    config.register(DIRECTORY_KEY, null, false, 
        "The path to a directory to host at the root for hosting files.");
    config.register(READ_TO_KEY, 5 * 60 * 1000, false, 
        "A timeout in milliseconds for reading data from a client after which "
        + "Undertow will close the connection.");
    config.register(WRITE_TO_KEY, 5 * 60 * 1000, false, 
        "A timeout in milliseconds for writing data to a client after which "
        + "Undertow will close the connection.");
    
    int port = config.getInt(HTTP_PORT_KEY);
    int ssl_port = config.getInt(TLS_PORT_KEY);
    if (port < 1 && ssl_port < 1) {
      System.err.println("Must provide an HTTP or SSL port.");
      System.exit(1);
    }
  
    String bind = config.getString(BIND_KEY);
    String root = config.getString(ROOT_KEY);
    boolean load_plugins = config.getBoolean(LOAD_PLUGINS_KEY);
    
    tsdb = new DefaultTSDB(config);
    if (load_plugins) {
      try {
        // if the plugins don't load within 5 minutes, something is TERRIBLY
        // wrong.
        tsdb.initializeRegistry(true).join(300000);
      } catch (Throwable t) {
        LOG.error("Failed to initialize TSDB registry", t);
        System.exit(1);
      }
    }
    
    // make sure to shutdown gracefully.
    registerShutdownHook();
    
    DeploymentInfo servletBuilder = Servlets.deployment()
        .setClassLoader(TSDMain.class.getClassLoader())
        .setContextPath(root)
        .setDeploymentName("tsd.war") // just a name
        .addServletContextAttribute(OpenTSDBApplication.TSD_ATTRIBUTE, tsdb)
        .addServlets(
          Servlets.servlet("OpenTSDB", 
              org.glassfish.jersey.servlet.ServletContainer.class)
                  .setAsyncSupported(true)
                  .setLoadOnStartup(1)
                  .addInitParam("javax.ws.rs.Application", 
                      OpenTSDBApplication.class.getName())
                  .addMapping(root + "api/*"));
    
        if (tsdb.getConfig().hasProperty(DIRECTORY_KEY) &&
            !Strings.isNullOrEmpty(tsdb.getConfig().getString(DIRECTORY_KEY))) {
          servletBuilder.setResourceManager(new FileResourceManager(
                  new File(tsdb.getConfig().getString(DIRECTORY_KEY))));
        }

    // Load an authentication filter if so configured.
    if (tsdb.getConfig().getBoolean(Authentication.AUTH_ENABLED_KEY)) {
      LOG.info("Authentication is enabled, searching for an auth filter.");
      final AuthFilter auth_filter = tsdb.getRegistry()
          .getDefaultPlugin(AuthFilter.class);
      
      if (auth_filter != null) {
        class AuthFactory implements InstanceFactory<Filter> {

          @Override
          public InstanceHandle<Filter> createInstance()
              throws InstantiationException {
            return new InstanceHandle<Filter>() {

              @Override
              public Filter getInstance() {
                return auth_filter;
              }

              @Override
              public void release() { }
              
            };
          }
          
        };
        
        final FilterInfo filter_info = new FilterInfo(
            "tsdbAuthFilter", 
            auth_filter.getClass(),
            new AuthFactory());
        filter_info.setAsyncSupported(true);
        final  Map<String, String> conf = tsdb.getConfig().asUnsecuredMap();
        for (final Entry<String, String> entry : conf.entrySet()) {
          filter_info.addInitParam(entry.getKey(), entry.getValue());
        }
        servletBuilder = servletBuilder.addFilter(filter_info)
            .addFilterUrlMapping("tsdbAuthFilter", "/*", DispatcherType.REQUEST);
        LOG.info("Successfully loaded auth filter: " + auth_filter);
      } else {
        LOG.error("Failed to find an authentication filter. Not starting.");
        System.exit(1);
        return;
      }
    }
    
    final DeploymentManager manager = Servlets.defaultContainer()
        .addDeployment(servletBuilder);
    manager.deploy();
    
    try {
      HttpHandler handler = new AccessLogHandler(
          manager.start(),
          new Slf4jAccessLogReceiver(),
          "combined",
          TSDMain.class.getClassLoader());
      
      handler = Handlers.path(Handlers.redirect(root))
          .addPrefixPath(root, handler);
      
      if (!Strings.isNullOrEmpty(tsdb.getConfig().getString(CORS_PATTERN_KEY))) {
        // TODO - flesh out settings.
        handler = CORSHandler.newBuilder().setNext(handler)
            .setOriginPattern(tsdb.getConfig().getString(CORS_PATTERN_KEY))
            .build();
      }
      
      // support compression!
      handler = new EncodingHandler.Builder()
          .build(null)
          .wrap(handler);
      
      if (tsdb.getStatsCollector() != null && 
          !(tsdb.getStatsCollector() instanceof BlackholeStatsCollector)) {
        handler = new MetricsHandler(tsdb.getStatsCollector(), handler);
      }
      
      final Builder builder = Undertow.builder()
          .setHandler(handler);
      if (port > 0) {
        builder.addHttpListener(port, bind);
      }
      // SSL/TLS setup
      if (ssl_port > 0) {
        builder.addHttpsListener(ssl_port, bind, buildSSLContext(config));
        String temp = tsdb.getConfig().getString(TLS_PROTOCOLS_KEY);
        if (!Strings.isNullOrEmpty(temp)) {
          builder.setSocketOption(Options.SSL_ENABLED_PROTOCOLS, 
              Sequence.of(temp.split(",")));
        }
        temp = tsdb.getConfig().getString(TLS_CIPHERS_KEY);
        if (!Strings.isNullOrEmpty(temp)) {
          builder.setSocketOption(Options.SSL_ENABLED_CIPHER_SUITES, 
              Sequence.of(temp.split(",")));
        }
        builder.setSocketOption(Options.SSL_CLIENT_AUTH_MODE, 
            SslClientAuthMode.valueOf(config.getString(TLS_VERIFY_CLIENT_KEY)));
      }
      
      // https://issues.jboss.org/browse/UNDERTOW-991
      builder.setSocketOption(Options.READ_TIMEOUT, config.getInt(READ_TO_KEY));
      builder.setSocketOption(Options.WRITE_TIMEOUT, config.getInt(WRITE_TO_KEY));
      
      server = builder.build();
      server.start();
      LOG.info("Undertow server successfully started, listening on " + bind + ":" + 
          (port > 0 ? port : ssl_port) + ". Version " + CoreVersion.version() 
            + "@" + CoreVersion.gitCommitId());
      return;
    } catch (ServletException e) {
      LOG.error("Unable to start due to servlet exception", e);
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid configuration", e);
    } catch (Exception e) {
      LOG.error("WTF! Unexpected exception starting server", e);
    }
  }

  /**
   * Walks the config to figure out how to load either a key store or 
   * certificates from files for the server.
   * @param config A non-null config.
   * @return An SSL context. If a method threw an exception then we exit.
   */
  private static SSLContext buildSSLContext(final Configuration config) {
    final RefreshingSSLContext.Builder builder = RefreshingSSLContext.newBuilder()
        .setTsdb(tsdb)
        .setInterval(0);
    final String keystore_location = config.getString(KEYSTORE_KEY);
    if (!Strings.isNullOrEmpty(keystore_location)) {
      builder.setType(SourceType.KEYSTORE)
             .setKeystore(keystore_location)
             .setKeystorePass(config.getString(KEYSTORE_PASS_KEY));
    } else if (!Strings.isNullOrEmpty(config.getString(TLS_SECRET_CERT_KEY))) {
      builder.setType(SourceType.SECRET)
             .setSecret_cert(config.getString(TLS_SECRET_CERT_KEY))
             .setSecret_key(config.getString(TLS_SECRET_KEY_KEY))
             .setCa(config.getString(TLS_CA_KEY));
    } else {
      builder.setType(SourceType.FILES)
             .setCert(config.getString(TLS_CERT_KEY))
             .setKey(config.getString(TLS_KEY_KEY))
             .setCa(config.getString(TLS_CA_KEY));
    }
    try {
    return builder.build().context();
    } catch (Throwable t) {
      LOG.error("Failed to initialize SSLContext", t);
      t.printStackTrace();
      System.exit(1);
      return null;
    }
  }
  
  /** Prints usage and exits with the given retval. */
  static void usage(final ArgP argp, final String errmsg, final int retval) {
    System.err.println(errmsg);
    System.err.println("Usage: tsd --config=PATH\n"
      + "Starts the TSD, the Time Series Daemon");
    if (argp != null) {
      System.err.print(argp.usage());
    }
    System.exit(retval);
  }
  
  /**
   * Helper method that will attach a callback to the runtime shutdown so that
   * if we receive a SIGTERM then we can gracefully stop the web server and
   * the TSD with it's associated plugins.
   */
  private static void registerShutdownHook() {
    final class TSDBShutdown extends Thread {
      public TSDBShutdown() {
        super("TSDBShutdown");
      }
      public void run() {
        try {
          if (server != null) {
            LOG.info("Stopping Undertow server");
            server.stop();
          }
          if (tsdb != null) {
            LOG.info("Shutting down TSD");
            tsdb.shutdown().join();
          }
          
          LOG.info("Shutdown complete.");
        } catch (Exception e) {
          LoggerFactory.getLogger(TSDBShutdown.class)
            .error("Uncaught exception during shutdown", e);
        }
      }
    }
    Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
  }

  public static class Slf4jAccessLogReceiver implements AccessLogReceiver {
    private static Logger ACCESS_LOG = LoggerFactory.getLogger("AccessLog");
    
    @Override
    public void logMessage(final String message) {
      ACCESS_LOG.info(message);      
    }
    
  }
}
