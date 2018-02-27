// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.servlet.ServletException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.Undertow.Builder;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.utils.ArgP;

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
  
  /** Defaults */
  public static final String DEFAULT_PATH = "/";
  
  /** The TSD reference. Static so we can shutdown gracefully. */
  private static DefaultTSDB tsdb = null;
  
  /** The Undertwo server reference. Static so we can shutdown gracefully. */
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
    
    int port = config.getInt(HTTP_PORT_KEY);
    int ssl_port = config.getInt(TLS_PORT_KEY);
    if (port < 1 && ssl_port < 1) {
      System.err.println("Must provide an HTTP or SSL port.");
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
      } catch (Exception e) {
        LOG.error("Failed to initialize TSDB registry", e);
        System.exit(1);
      }
    }
    
    // make sure to shutdown gracefully.
    registerShutdownHook();
    
    final DeploymentInfo servletBuilder = Servlets.deployment()
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
                  .addMapping("/*"));

    final DeploymentManager manager = Servlets.defaultContainer()
        .addDeployment(servletBuilder);
    manager.deploy();
    
    String keystore_location = null;
    try {
      final PathHandler path = Handlers.path(Handlers.redirect(root))
              .addPrefixPath(root, manager.start());
      
      final Builder builder = Undertow.builder()
          .setHandler(path);
      if (port > 0) {
        builder.addHttpListener(port, bind);
      }
      
      // SSL/TLS setup
      if (ssl_port > 0) {
        keystore_location = config.getString("tsd.network.keystore.location");
        if (Strings.isNullOrEmpty(keystore_location)) {
          throw new IllegalArgumentException("Cannot enable SSL without a "
              + "keystore. Set 'tsd.network.keystore.location'");
        }
        // TODO - ugly ugly ugly! And not secure too!
        final String key = config.getString("tsd.network.keystore.password");
        if (Strings.isNullOrEmpty(key)) {
          throw new IllegalArgumentException("Cannot enable SSL without a "
              + "keystore password. Set 'tsd.network.keystore.password'");
        }
        
        // load an initialize the keystore.
        final FileInputStream file = new FileInputStream(keystore_location);
        final KeyStore keystore = KeyStore.getInstance(KeyStore.getDefaultType());
        keystore.load(file, key.toCharArray());
        
        // initialize a key manager to pass to the SSL context using the keystore.
        final KeyManagerFactory key_factory = KeyManagerFactory.getInstance(
            KeyManagerFactory.getDefaultAlgorithm());
        key_factory.init(keystore, key.toCharArray());

        // init a trust manager so we can use the public cert.
        final TrustManagerFactory trust_factory = 
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
          trust_factory.init(keystore);
        final TrustManager[] trustManagers = trust_factory.getTrustManagers();
        
        final SSLContext sslContext = SSLContext.getInstance("TLS"); 
        sslContext.init(key_factory.getKeyManagers(), trustManagers, null);
        builder.addHttpsListener(4443, bind, sslContext);
      }
      
      server = builder.build();
      server.start();
      LOG.info("Undertow server successfully started.");
      return;
    } catch (ServletException e) {
      LOG.error("Unable to start due to servlet exception", e);
    } catch (FileNotFoundException e) {
      LOG.error("Unable to open keystore file: " + keystore_location, e);
    } catch (NoSuchAlgorithmException e) {
      LOG.error("Missing a required algorithm for TLS?", e);
    } catch (CertificateException e) {
      LOG.error("Invalid certificate in keystore: " + keystore_location, e);
    } catch (IOException e) {
      LOG.error("WTF? Something went pear shaped unexpectedly", e);
    } catch (KeyManagementException e) {
      LOG.error("Something was wrong with the key in the keystore: " 
          + keystore_location, e);
    } catch (UnrecoverableKeyException e) {
      LOG.error("Possibly corrupted key in file: " + keystore_location, e);
    } catch (KeyStoreException e) {
      LOG.error("WTF! Unexpected exception in keystore: " + keystore_location, e);
    } catch (IllegalArgumentException e) {
      LOG.error("Invalid configuration", e);
    } catch (Exception e) {
      LOG.error("WTF! Unexpected exception starting server", e);
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
            LOG.info("Shuttingdown TSD");
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
}
