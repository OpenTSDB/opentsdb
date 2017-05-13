// This file is part of OpenTSDB.
// Copyright (C) 2017  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.tsd;

import java.io.IOException;

import javax.servlet.ServletException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.server.handlers.PathHandler;
import io.undertow.servlet.Servlets;
import io.undertow.servlet.api.DeploymentInfo;
import io.undertow.servlet.api.DeploymentManager;
import net.opentsdb.core.TSDB;
import net.opentsdb.servlet.applications.OpenTSDBApplication;
import net.opentsdb.utils.ArgP;
import net.opentsdb.utils.CliOptions;
import net.opentsdb.utils.Config;

/**
 * A simple main method that instantiates a TSD and the TSD servlet package
 * and serves them with the Undertow HTTP server.
 * 
 * @since 3.0
 */
public class TSDMain {
  private static Logger LOG = LoggerFactory.getLogger(TSDMain.class);
  
  /** The default HTTP path. */
  public static final String DEFAULT_PATH = "/";
  
  /** The TSD reference. Static so we can shutdown gracefully. */
  private static TSDB tsdb = null;
  
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
    
    final ArgP argp = new ArgP();
    CliOptions.addCommon(argp);
    argp.addOption("--port", "NUM", "TCP port to listen on.");
    argp.addOption("--bind", "ADDR", "Address to bind to (default: 0.0.0.0).");
    CliOptions.parse(argp, args);
    
    final Config config;
    try {
      config = CliOptions.getConfig(argp);
    } catch (IOException e) {
      LOG.error("Failed to load config file", e);
      usage(argp, "Failed to load config file", 1);
      return; // returns in usage but this makes IDEs happy.
    }
    
    int port = 4242;
    try {
      if (!config.hasProperty("tsd.network.port"))
        usage(argp, "Missing network port", 1);
      port = config.getInt("tsd.network.port");
    } catch (NumberFormatException nfe) {
      usage(argp, "Invalid network port setting", 1);
    }
    String bind = config.getString("tsd.network.bind");
    if (Strings.isNullOrEmpty(bind)) {
      bind = "0.0.0.0";
    }
    String root = config.getString("tsd.http.root");
    if (Strings.isNullOrEmpty(root)) {
      root = DEFAULT_PATH;
    }
    boolean load_plugins = config.hasProperty("tsd.core.load_plugins") ? 
        config.getBoolean("tsd.core.load_plugins") : true;
    
    tsdb = new TSDB(config);
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
        .setDeploymentName("tsd.war")
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
    
    try {
      final PathHandler path = Handlers.path(Handlers.redirect(root))
              .addPrefixPath(root, manager.start());

      server = Undertow.builder()
              .addHttpListener(port, bind)
              .setHandler(path)
              .build();
      server.start();
    } catch (ServletException e) {
      LOG.error("Unable to start due to servlet exception", e);
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
