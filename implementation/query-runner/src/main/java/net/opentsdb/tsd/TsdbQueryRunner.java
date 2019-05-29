// This file is part of OpenTSDB.
// Copyright (C) 2019  The OpenTSDB Authors.
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

import java.io.File;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.netty.util.Timeout;
import io.netty.util.TimerTask;
import net.opentsdb.configuration.Configuration;
import net.opentsdb.core.DefaultTSDB;
import net.opentsdb.data.TimeSeriesDataSourceFactory;
import net.opentsdb.storage.MockDataStoreFactory;
import net.opentsdb.utils.SharedHttpClient;

/**
 * A simple utility that scans a directory for configuration files (in YAML)
 * and executes the query found in each file against the configured endpoints,
 * tracking the responses times and sizes as metrics. This can be used for
 * health checking or performance testing.
 * 
 * @since 3.0
 */
public class TsdbQueryRunner implements TimerTask {
  private static Logger LOG = LoggerFactory.getLogger(TsdbQueryRunner.class);
  
  protected static final String CONFIG_DIR = "config.directory";
  protected static final String TIMEOUT_KEY = "client.timeout";
  protected static final String CLIENT_KEY = "client.id";
  protected static final String USE_CURL_KEY = "client.use_curl";
  protected static final String CURL_KEY = "client.curl";
  protected static final String CURL_TEMP_KEY = "client.curl.temp";
  protected static final String CURL_FLAGS_KEY = "client.curl.flags";
  protected static final String CURL_METRICS_KEY = "client.curl.metrics";
  protected static final String CURL_STATUS = "\\nstatus:\\t%{http_code}\\n";
  protected static final String CURL_METRICS_DEFAULT = 
      "download.size:\\t%{size_download}\\n"
      + "download.speed:\\t%{speed_download}\\n"
      + "upload.size:\\t%{size_upload}\\n"
      + "upload.speed:\\t%{speed_upload}\\n"
      + "namelookup.time:\\t%{time_namelookup}\\n"
      + "connect.time:\\t%{time_connect}\\n"
      + "pretransfer.time:\\t%{time_pretransfer}\\n"
      + "starttransfer.time:\\t%{time_starttransfer}\\n"
      + "appconnect.time:\\t%{time_appconnect}\\n"
      + "redirect.time:\\t%{time_redirect}\\n"
      + "total.time:\\t%{time_total}\n";
  
  protected static DefaultTSDB TSDB;
  
  /** The path we'll look for configs in. */
  protected final String dir;
  
  /** the queries we're running. */
  protected final Map<String, QueryConfig> queries;
  
  /** Client for health checks and queries. */
  protected CloseableHttpAsyncClient client;
  
  /** Whether or not to use CURL. */
  protected boolean use_curl;
  
  /** The curl executable. */
  protected String curl_exec;
  
  /** Temp dir for curl. */
  protected String curl_temp;
  
  /** Optional CURL flags. */
  protected String curl_flags;
  
  /** The metrics template. */
  protected String curl_metrics;
  
  /** Timeout in millis. */
  protected int timeout;
  
  TsdbQueryRunner(final String dir) {
    this.dir = dir;
    queries = Maps.newConcurrentMap();
    if (Strings.isNullOrEmpty(dir)) {
      LOG.error("No directory specified. Must provide a full path.");
      System.exit(1);
    }
    
    use_curl = TSDB.getConfig().getBoolean(USE_CURL_KEY);
    curl_exec = TSDB.getConfig().getString(CURL_KEY);
    curl_temp = TSDB.getConfig().getString(CURL_TEMP_KEY);
    if (!curl_temp.endsWith("/")) {
      curl_temp += "/";
    }
    curl_flags = TSDB.getConfig().getString(CURL_FLAGS_KEY);
    curl_metrics = TSDB.getConfig().getString(CURL_METRICS_KEY);
    timeout = TSDB.getConfig().getInt(TIMEOUT_KEY);
    
    final String client_id = TSDB.getConfig().getString(CLIENT_KEY);
    final SharedHttpClient shared_client = TSDB.getRegistry().getPlugin(
        SharedHttpClient.class, client_id);
    if (shared_client == null && !use_curl) {
      LOG.error("No shared HTTP client found "
          + "for ID: " + (Strings.isNullOrEmpty(client_id) ? 
              "Default" : client_id));
      System.exit(1);
    }
    if (shared_client != null) {
      client = shared_client.getClient();
      LOG.info("Running with the shared HttpClient");
    } else {
      LOG.info("Running in CURL mode.");
    }
    LOG.info("Looking for config files in: " + dir);
    TSDB.getMaintenanceTimer().newTimeout(this, 0, TimeUnit.SECONDS);
  }
  
  @Override
  public void run(final Timeout timeout) throws Exception {
    try {
      final File root = new File(dir);
      if (!root.exists()) {
        LOG.warn("Config directory " + dir + " does not exist yet.");
        queries.clear();
        return;
      }
      
      final Set<String> new_configs = Sets.newHashSet();
      for (final File file: Files.fileTreeTraverser().breadthFirstTraversal(root)) {
        if (file.isFile() && file.toString().toLowerCase().endsWith("yaml")) {
          try {
            final QueryConfig config = QueryConfig.parse(this, file);
            new_configs.add(config.id);
            
            QueryConfig extant = queries.get(config.id);
            if (extant != null) {
              if (!extant.equals(config)) {
                LOG.info("Updating config for: " + config.id);
                extant.cancel();
                queries.put(config.id, config);
                config.schedule();
              }
              // otherwise no change.
            } else {
              queries.put(config.id, config);
              config.schedule();
            }
          } catch (Exception e) {
            LOG.error("Failed to parse file: " + file, e);
          }
        }
      }
      
      final Iterator<Entry<String, QueryConfig>> iterator = queries.entrySet().iterator();
      while (iterator.hasNext()) {
        final Entry<String, QueryConfig> entry = iterator.next();
        if (!new_configs.contains(entry.getKey())) {
          LOG.info("Removing query: " + entry.getKey());
          entry.getValue().cancel();
          iterator.remove();
        }
      }
      LOG.debug("Finished loading of configs. Loaded: " + queries.size());
    } catch (Throwable t) {
      LOG.error("Failed to load files from directory: " + dir, t);
    } finally {
      TSDB.getMaintenanceTimer().newTimeout(this, 60, TimeUnit.SECONDS);
    }
  }
  
  public static void main(final String[] args) {
    final Configuration config = new Configuration(args);
    TSDB = new DefaultTSDB(config);
    try {
      // if the plugins don't load within 5 minutes, something is TERRIBLY
      // wrong.
      TSDB.initializeRegistry(true)
        .join(300000);
    } catch (Throwable t) {
      LOG.error("Failed to initialize TSDB registry", t);
      System.exit(1);
    }
    
    // we need this for parsing.
    final MockDataStoreFactory source_factory = new MockDataStoreFactory();
    source_factory.initialize(TSDB, null);
    TSDB.getRegistry().registerFactory(source_factory);
    TSDB.getRegistry().registerPlugin(TimeSeriesDataSourceFactory.class, 
        null, source_factory);
    
    // make sure to shutdown gracefully.
    registerShutdownHook();
    
    if (!config.hasProperty(CONFIG_DIR)) {
      config.register(CONFIG_DIR, "/usr/share/opentsdb/queryrunner", false, 
          "The directory where query configs are stored.");
    }
    if (!config.hasProperty(TIMEOUT_KEY)) {
      config.register(TIMEOUT_KEY, 30000, false, 
          "A request timeout in milliseconds.");
    }
    if (!config.hasProperty(CLIENT_KEY)) {
      config.register(CLIENT_KEY, null, false, 
          "The ID of a shared HTTP client.");
    }
    if (!config.hasProperty(CURL_KEY)) {
      config.register(CURL_KEY, "/usr/bin/curl", false, 
          "The full path to the CURL executable.");
    }
    if (!config.hasProperty(CURL_TEMP_KEY)) {
      config.register(CURL_TEMP_KEY, "/tmp", false, 
          "The path to a temp dir for CURL checks to use.");
    }
    if (!config.hasProperty(USE_CURL_KEY)) {
      config.register(USE_CURL_KEY, false, false, 
          "Whether or not to use CURL for detailed measurements.");
    }
    if (!config.hasProperty(CURL_FLAGS_KEY)) {
      config.register(CURL_FLAGS_KEY, null, false, 
          "Optional CLI flags for the CURL call.");
    }
    if (!config.hasProperty(CURL_METRICS_KEY)) {
      config.register(CURL_METRICS_KEY, CURL_METRICS_DEFAULT, false, 
          "The CURL metrics template.");
    }
    
    new TsdbQueryRunner(config.getString(CONFIG_DIR));
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
          if (TSDB != null) {
            LOG.info("Shutting down TSD");
            TSDB.shutdown().join();
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
