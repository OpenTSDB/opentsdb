// This file is part of OpenTSDB.
// Copyright (C) 2010-2014  The OpenTSDB Authors.
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
import java.lang.reflect.Method;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Atomics;
import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.handler.codec.http.HttpMethod;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.opentsdb.tools.BuildData;
import net.opentsdb.core.Aggregators;
import net.opentsdb.core.TSDB;
import net.opentsdb.core.TSDB.OperationMode;
import net.opentsdb.query.filter.TagVFilter;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.JSON;
import net.opentsdb.utils.PluginLoader;

/**
 * Manager for the lifecycle of <code>HttpRpc</code>s, <code>TelnetRpc</code>s,
 * <code>RpcPlugin</code>s, and <code>HttpRpcPlugin</code>.  This is a
 * singleton.  Its lifecycle must be managed by the "container".  If you are
 * launching via {@code TSDMain} then shutdown (and non-lazy initialization)
 * is taken care of. Outside of the use of {@code TSDMain}, you are responsible
 * for shutdown, at least.
 *
 * <p> Here's an example of how to correctly handle shutdown manually:
 *
 * <pre>
 * // Startup our TSDB instance...
 * TSDB tsdb_instance = ...;
 *
 * // ... later, during shtudown ..
 *
 * if (RpcManager.isInitialized()) {
 *   // Check that its actually been initialized.  We don't want to
 *   // create a new instance only to shutdown!
 *   RpcManager.instance(tsdb_instance).shutdown().join();
 * }
 * </pre>
 *
 * @since 2.2
 */
public final class RpcManager {
  private static final Logger LOG = LoggerFactory.getLogger(RpcManager.class);

  /** This is base path where {@link HttpRpcPlugin}s are rooted.  It's used
   * to match incoming requests. */
  @VisibleForTesting
  protected static final String PLUGIN_BASE_WEBPATH = "plugin";

  /** Splitter for web paths.  Removes empty strings to handle trailing or
   * leading slashes.  For instance, all of <code>/plugin/mytest</code>,
   * <code>plugin/mytest/</code>, and <code>plugin/mytest</code> will be
   * split to <code>[plugin, mytest]</code>. */
  private static final Splitter WEBPATH_SPLITTER = Splitter.on('/')
      .trimResults()
      .omitEmptyStrings();

  /** Matches paths declared by {@link HttpRpcPlugin}s that are rooted in
   * the system's plugins path. */
  private static final Pattern HAS_PLUGIN_BASE_WEBPATH = Pattern.compile(
      "^/?" + PLUGIN_BASE_WEBPATH + "/?.*",
      Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

  /** Reference to our singleton instance.  Set in {@link #initialize}. */
  private static final AtomicReference<RpcManager> INSTANCE = Atomics.newReference();

  /** Commands we can serve on the simple, telnet-style RPC interface. */
  private ImmutableMap<String, TelnetRpc> telnet_commands;
  /** Commands we serve on the HTTP interface. */
  private ImmutableMap<String, HttpRpc> http_commands;
  /** HTTP commands from user plugins. */
  private ImmutableMap<String, HttpRpcPlugin> http_plugin_commands;
  /** List of activated RPC plugins */
  private ImmutableList<RpcPlugin> rpc_plugins;

  /** The TSDB that owns us. */
  private TSDB tsdb;

  /**
   * Constructor used by singleton factory method.
   * @param tsdb the owning TSDB instance.
   */
  private RpcManager(final TSDB tsdb) {
    this.tsdb = tsdb;
  }

  /**
   * Get or create the singleton instance of the manager, loading all the
   * plugins enabled in the given TSDB's {@link Config}.
   * @return the shared instance of {@link RpcManager}. It's okay to
   * hold this reference once obtained.
   */
  public static synchronized RpcManager instance(final TSDB tsdb) {
    final RpcManager existing = INSTANCE.get();
    if (existing != null) {
      return existing;
    }

    final RpcManager manager = new RpcManager(tsdb);
    
    // Load any plugins that are enabled via Config.  Fail if any plugin cannot be loaded.

    final ImmutableList.Builder<RpcPlugin> rpcBuilder = ImmutableList.builder();
    if (tsdb.getConfig().hasProperty("tsd.rpc.plugins")) {
      final String[] plugins = tsdb.getConfig().getString("tsd.rpc.plugins").split(",");
      manager.initializeRpcPlugins(plugins, rpcBuilder);
    }
    manager.rpc_plugins = rpcBuilder.build();

    final ImmutableMap.Builder<String, TelnetRpc> telnetBuilder = ImmutableMap.builder();
    final ImmutableMap.Builder<String, HttpRpc> httpBuilder = ImmutableMap.builder();
    manager.initializeBuiltinRpcs(tsdb.getMode(), telnetBuilder, httpBuilder);
    manager.telnet_commands = telnetBuilder.build();
    manager.http_commands = httpBuilder.build();

    final ImmutableMap.Builder<String, HttpRpcPlugin> httpPluginsBuilder = ImmutableMap.builder();
    if (tsdb.getConfig().hasProperty("tsd.http.rpc.plugins")) {
      final String[] plugins = tsdb.getConfig().getString("tsd.http.rpc.plugins").split(",");
      manager.initializeHttpRpcPlugins(tsdb.getMode(), plugins, httpPluginsBuilder);
    }
    manager.http_plugin_commands = httpPluginsBuilder.build();

    INSTANCE.set(manager);
    return manager;
  }

  /**
   * @return {@code true} if the shared instance has been initialized;
   * {@code false} otherwise.
   */
  public static synchronized boolean isInitialized() {
    return INSTANCE.get() != null;
  }

  /**
   * @return list of loaded {@link RpcPlugin}s.  Possibly empty but
   * never {@code null}.
   */
  @VisibleForTesting
  protected ImmutableList<RpcPlugin> getRpcPlugins() {
    return rpc_plugins;
  }

  /**
   * Lookup a {@link TelnetRpc} based on given command name.  Note that this
   * lookup is case sensitive in that the {@code command} passed in must
   * match a registered RPC command exactly.
   * @param command a telnet API command name.
   * @return the {@link TelnetRpc} for the given {@code command} or {@code null}
   * if not found.
   */
  TelnetRpc lookupTelnetRpc(final String command) {
    return telnet_commands.get(command);
  }

  /**
   * Lookup a built-in {@link HttpRpc} based on the given {@code queryBaseRoute}.
   * The lookup is based on exact match of the input parameter and the registered
   * {@link HttpRpc}s.
   * @param queryBaseRoute the HTTP query's base route, with no trailing or
   * leading slashes.  For example: {@code api/query}
   * @return the {@link HttpRpc} for the given {@code queryBaseRoute} or
   * {@code null} if not found.
   */
  HttpRpc lookupHttpRpc(final String queryBaseRoute) {
    return http_commands.get(queryBaseRoute);
  }

  /**
   * Lookup a user-supplied {@link HttpRpcPlugin} for the given
   * {@code queryBaseRoute}. The lookup is based on exact match of the input
   * parameter and the registered {@link HttpRpcPlugin}s.
   * @param queryBaseRoute the value of {@link HttpRpcPlugin#getPath()} with no
   * trailing or leading slashes.
   * @return the {@link HttpRpcPlugin} for the given {@code queryBaseRoute} or
   * {@code null} if not found.
   */
  HttpRpcPlugin lookupHttpRpcPlugin(final String queryBaseRoute) {
    return http_plugin_commands.get(queryBaseRoute);
  }

  /**
   * @param uri HTTP request URI, with or without query parameters.
   * @return {@code true} if the URI represents a request for a
   * {@link HttpRpcPlugin}; {@code false} otherwise.  Note that this
   * method returning true <strong>says nothing</strong> about
   * whether or not there is a {@link HttpRpcPlugin} registered
   * at the given URI, only that it's a valid RPC plugin request.
   */
  boolean isHttpRpcPluginPath(final String uri) {
    if (Strings.isNullOrEmpty(uri) || uri.length() <= PLUGIN_BASE_WEBPATH.length()) {
      return false;
    } else {
      // Don't consider the query portion, if any.
      int qmark = uri.indexOf('?');
      String path = uri;
      if (qmark != -1) {
        path = uri.substring(0, qmark);
      }

      final List<String> parts = WEBPATH_SPLITTER.splitToList(path);
      return (parts.size() > 1 && parts.get(0).equals(PLUGIN_BASE_WEBPATH));
    }
  }

  /**
   * Load and init instances of {@link TelnetRpc}s and {@link HttpRpc}s.
   * These are not generally configurable via TSDB config.
   * @param mode is this TSD in read/write ("rw") or read-only ("ro")
   * mode?
   * @param telnet a map of telnet command names to {@link TelnetRpc}
   * instances.
   * @param http a map of API endpoints to {@link HttpRpc} instances.
   */
  private void initializeBuiltinRpcs(final OperationMode mode,
        final ImmutableMap.Builder<String, TelnetRpc> telnet,
        final ImmutableMap.Builder<String, HttpRpc> http) {

    final Boolean enableApi = tsdb.getConfig().getString("tsd.core.enable_api").equals("true");
    final Boolean enableUi = tsdb.getConfig().getString("tsd.core.enable_ui").equals("true");
    final Boolean enableDieDieDie = tsdb.getConfig().getString("tsd.no_diediedie").equals("false");

    LOG.info("Mode: {}, HTTP UI Enabled: {}, HTTP API Enabled: {}", mode, enableUi, enableApi);

    // defaults common to every mode
    final StatsRpc stats = new StatsRpc();
    final ListAggregators aggregators = new ListAggregators();
    final DropCachesRpc dropcaches = new DropCachesRpc();
    final Version version = new Version();
    
    telnet.put("stats", stats);
    telnet.put("dropcaches", dropcaches);
    telnet.put("version", version);
    telnet.put("exit", new Exit());
    telnet.put("help", new Help());
    
    if (enableUi) {
      http.put("aggregators", aggregators);
      http.put("logs", new LogsRpc());
      http.put("stats", stats);
      http.put("version", version);
    }
    
    if (enableApi) {
      http.put("api/aggregators", aggregators);
      http.put("api/config", new ShowConfig());
      http.put("api/dropcaches", dropcaches);
      http.put("api/stats", stats);
      http.put("api/version", version);
    }
    
    final PutDataPointRpc put = new PutDataPointRpc(tsdb.getConfig());
    final RollupDataPointRpc rollups = new RollupDataPointRpc(tsdb.getConfig());
    final HistogramDataPointRpc histos = new HistogramDataPointRpc(tsdb.getConfig());
    final SuggestRpc suggest_rpc = new SuggestRpc();
    final AnnotationRpc annotation_rpc = new AnnotationRpc();
    final StaticFileRpc staticfile = new StaticFileRpc();
    
    switch(mode) {
    case WRITEONLY:
      telnet.put("put", put);
      telnet.put("rollup", rollups);
      telnet.put("histogram", histos);
      
      if (enableApi) {
        http.put("api/annotation", annotation_rpc);
        http.put("api/annotations", annotation_rpc);
        http.put("api/put", put);
        http.put("api/rollup", rollups);
        http.put("api/histogram", histos);
        http.put("api/tree", new TreeRpc());
        http.put("api/uid", new UniqueIdRpc());
      }
      break;
    case READONLY:
      if (enableUi) {
        http.put("", new HomePage());
        http.put("s", staticfile);
        http.put("favicon.ico", staticfile);
        http.put("suggest", suggest_rpc);
        http.put("q", new GraphHandler());
      }
      
      if (enableApi) {
        http.put("api/query", new QueryRpc());
        http.put("api/search", new SearchRpc());
        http.put("api/suggest", suggest_rpc);
      }
      
      break;
    case READWRITE:
      telnet.put("put", put);
      telnet.put("rollup", rollups);
      telnet.put("histogram", histos);
      
      if (enableUi) {
        http.put("", new HomePage());
        http.put("s", staticfile);
        http.put("favicon.ico", staticfile);
        http.put("suggest", suggest_rpc);
        http.put("q", new GraphHandler());
      }
      
      if (enableApi) {
        http.put("api/query", new QueryRpc());
        http.put("api/search", new SearchRpc());
        http.put("api/annotation", annotation_rpc);
        http.put("api/annotations", annotation_rpc);
        http.put("api/suggest", suggest_rpc);
        http.put("api/put", put);
        http.put("api/rollup", rollups);
        http.put("api/histogram", histos);
        http.put("api/tree", new TreeRpc());
        http.put("api/uid", new UniqueIdRpc());
      }
    }
    
    

    if (enableDieDieDie) {
      final DieDieDie diediedie = new DieDieDie();
      telnet.put("diediedie", diediedie);
      if (enableUi) {
        http.put("diediedie", diediedie);
      }
    }
  }

  /**
   * Load and init the {@link HttpRpcPlugin}s provided as an array of
   * {@code pluginClassNames}.
   * @param mode is this TSD in read/write ("rw") or read-only ("ro")
   * mode?
   * @param pluginClassNames fully-qualified class names that are
   * instances of {@link HttpRpcPlugin}s
   * @param http a map of canonicalized paths
   * (obtained via {@link #canonicalizePluginPath(String)})
   * to {@link HttpRpcPlugin} instance.
   */
  @VisibleForTesting
  protected void initializeHttpRpcPlugins(final OperationMode mode,
        final String[] pluginClassNames,
        final ImmutableMap.Builder<String, HttpRpcPlugin> http) {
    for (final String plugin : pluginClassNames) {
      final HttpRpcPlugin rpc = createAndInitialize(plugin, HttpRpcPlugin.class);
      validateHttpRpcPluginPath(rpc.getPath());
      final String path = rpc.getPath().trim();
      final String canonicalized_path = canonicalizePluginPath(path);
      http.put(canonicalized_path, rpc);
      LOG.info("Mounted HttpRpcPlugin [{}] at path \"{}\"", rpc.getClass().getName(), canonicalized_path);
    }
  }

  /**
   * Ensure that the given path for an {@link HttpRpcPlugin} is valid.  This
   * method simply returns for valid inputs; throws and exception otherwise.
   * @param path a request path, no query parameters, etc.
   * @throws IllegalArgumentException on invalid paths.
   */
  @VisibleForTesting
  protected void validateHttpRpcPluginPath(final String path) {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(path),
        "Invalid HttpRpcPlugin path. Path is null or empty.");
    final String testPath = path.trim();
    Preconditions.checkArgument(!HAS_PLUGIN_BASE_WEBPATH.matcher(path).matches(),
        "Invalid HttpRpcPlugin path %s. Path contains system's plugin base path.",
        testPath);

    URI uri = URI.create(testPath);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(uri.getPath()),
        "Invalid HttpRpcPlugin path %s. Parsed path is null or empty.", testPath);
    Preconditions.checkArgument(!uri.getPath().equals("/"),
        "Invalid HttpRpcPlugin path %s. Path is equal to root.", testPath);
    Preconditions.checkArgument(Strings.isNullOrEmpty(uri.getQuery()),
        "Invalid HttpRpcPlugin path %s. Path contains query parameters.", testPath);
  }

  /**
   * @param origPath a request path, no query parameters, etc.
   * @return a canonical representation of the input, with trailing and leading
   * slashes removed.
   * @throws IllegalArgumentException if the given path is a root.
   */
  @VisibleForTesting
  protected String canonicalizePluginPath(final String origPath) {
    Preconditions.checkArgument(!(Strings.isNullOrEmpty(origPath) || origPath.equals("/")),
        "Path %s is a root.", origPath);
    String new_path = origPath;
    if (new_path.startsWith("/")) {
      new_path = new_path.substring(1);
    }
    if (new_path.endsWith("/")) {
      new_path = new_path.substring(0, new_path.length()-1);
    }
    return new_path;
  }

  /**
   * Load and init the {@link RpcPlugin}s provided as an array of
   * {@code pluginClassNames}.
   * @param pluginClassNames fully-qualified class names that are
   * instances of {@link RpcPlugin}s
   * @param rpcs a list of loaded and initialized plugins
   */
  private void initializeRpcPlugins(final String[] pluginClassNames,
        final ImmutableList.Builder<RpcPlugin> rpcs) {
    for (final String plugin : pluginClassNames) {
      final RpcPlugin rpc = createAndInitialize(plugin, RpcPlugin.class);
      rpcs.add(rpc);
    }
  }

  /**
   * Helper method to load and initialize a given plugin class. This uses reflection
   * because plugins share no common interfaces. (They could though!)
   * @param pluginClassName the class name of the plugin to load
   * @param pluginClass class of the plugin
   * @return loaded an initialized instance of {@code pluginClass}
   */
  @VisibleForTesting
  protected <T> T createAndInitialize(final String pluginClassName, final Class<T> pluginClass) {
    final T instance = PluginLoader.loadSpecificPlugin(pluginClassName, pluginClass);
    Preconditions.checkState(instance != null,
        "Unable to locate %s using name '%s", pluginClass, pluginClassName);
    try {
      final Method initMeth = instance.getClass().getMethod("initialize", TSDB.class);
      initMeth.invoke(instance, tsdb);
      final Method versionMeth = instance.getClass().getMethod("version");
      String version = (String) versionMeth.invoke(instance);
      LOG.info("Successfully initialized plugin [{}] version: {}",
          instance.getClass().getCanonicalName(),
          version);
      return instance;
    } catch (Exception e) {
      throw new RuntimeException("Failed to initialize " + instance.getClass(), e);
    }
  }

  /**
   * Called to gracefully shutdown the plugin. Implementations should close
   * any IO they have open
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null}
   * (think of it as {@code Deferred<Void>}).
   */
  public Deferred<ArrayList<Object>> shutdown() {
    // Clear shared instance.
    INSTANCE.set(null);

    final Collection<Deferred<Object>> deferreds = Lists.newArrayList();

    if (http_plugin_commands != null) {
      for (final Map.Entry<String, HttpRpcPlugin> entry : http_plugin_commands.entrySet()) {
        deferreds.add(entry.getValue().shutdown());
      }
    }

    if (rpc_plugins != null) {
      for (final RpcPlugin rpc : rpc_plugins) {
        deferreds.add(rpc.shutdown());
      }
    }

    return Deferred.groupInOrder(deferreds);
  }

  /**
   * Collect stats on the shared instance of {@link RpcManager}.
   */
  static void collectStats(final StatsCollector collector) {
    final RpcManager manager = INSTANCE.get();
    if (manager != null) {
      if (manager.rpc_plugins != null) {
        try {
          collector.addExtraTag("plugin", "rpc");
          for (final RpcPlugin rpc : manager.rpc_plugins) {
            rpc.collectStats(collector);
          }
        } finally {
          collector.clearExtraTag("plugin");
        }
      }

      if (manager.http_plugin_commands != null) {
        try {
          collector.addExtraTag("plugin", "httprpc");
          for (final Map.Entry<String, HttpRpcPlugin> entry
              : manager.http_plugin_commands.entrySet()) {
            entry.getValue().collectStats(collector);
          }
        } finally {
          collector.clearExtraTag("plugin");
        }
      }
    }
  }

  // ---------------------------- //
  // Individual command handlers. //
  // ---------------------------- //

  /** The "diediedie" command and "/diediedie" endpoint. */
  private final class DieDieDie implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      LOG.warn("{} {}", chan, "shutdown requested");
      chan.write("Cleaning up and exiting now.\n");
      return doShutdown(tsdb, chan);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) {
      LOG.warn("{} {}", query, "shutdown requested");
      query.sendReply(HttpQuery.makePage("TSD Exiting", "You killed me",
                                         "Cleaning up and exiting now."));
      doShutdown(tsdb, query.channel());
    }

    private Deferred<Object> doShutdown(final TSDB tsdb, final Channel chan) {
      ((GraphHandler) http_commands.get("q")).shutdown();
      ConnectionManager.closeAllConnections();
      // Netty gets stuck in an infinite loop if we shut it down from within a
      // NIO thread.  So do this from a newly created thread.
      final class ShutdownNetty extends Thread {
        ShutdownNetty() {
          super("ShutdownNetty");
        }
        public void run() {
          chan.getFactory().releaseExternalResources();
        }
      }
      new ShutdownNetty().start();  // Stop accepting new connections.

      // Log any error that might occur during shutdown.
      final class ShutdownTSDB implements Callback<Exception, Exception> {
        public Exception call(final Exception arg) {
          LOG.error("Unexpected exception while shutting down", arg);
          return arg;
        }
        public String toString() {
          return "shutdown callback";
        }
      }
      return tsdb.shutdown().addErrback(new ShutdownTSDB());
    }
  }

  /** The "exit" command. */
  private static final class Exit implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      chan.disconnect();
      return Deferred.fromResult(null);
    }
  }

  /** The "help" command. */
  private final class Help implements TelnetRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      final StringBuilder buf = new StringBuilder();
      buf.append("available commands: ");
      // TODO(tsuna): Maybe sort them?
      for (final String command : telnet_commands.keySet()) {
        buf.append(command).append(' ');
      }
      buf.append('\n');
      chan.write(buf.toString());
      return Deferred.fromResult(null);
    }
  }

  /** The home page ("GET /"). */
  private static final class HomePage implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query)
      throws IOException {
      final StringBuilder buf = new StringBuilder(2048);
      buf.append("<div id=queryuimain></div>"
                 + "<noscript>You must have JavaScript enabled.</noscript>"
                 + "<iframe src=javascript:'' id=__gwt_historyFrame tabIndex=-1"
                 + " style=position:absolute;width:0;height:0;border:0>"
                 + "</iframe>");
      query.sendReply(HttpQuery.makePage(
        "<script type=text/javascript language=javascript"
        + " src=s/queryui.nocache.js></script>",
        "OpenTSDB", "", buf.toString()));
    }
  }

  /** The "/aggregators" endpoint. */
  private static final class ListAggregators implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query)
      throws IOException {

      // only accept GET / POST
      RpcUtil.allowedMethods(query.method(), HttpMethod.GET.getName(), HttpMethod.POST.getName());

      if (query.apiVersion() > 0) {
        query.sendReply(
            query.serializer().formatAggregatorsV1(Aggregators.set()));
      } else {
        query.sendReply(JSON.serializeToBytes(Aggregators.set()));
      }
    }
  }

  /** The "version" command. */
  private static final class Version implements TelnetRpc, HttpRpc {
    public Deferred<Object> execute(final TSDB tsdb, final Channel chan,
                                    final String[] cmd) {
      if (chan.isConnected()) {
        chan.write(BuildData.revisionString() + '\n'
                   + BuildData.buildString() + '\n');
      }
      return Deferred.fromResult(null);
    }

    public void execute(final TSDB tsdb, final HttpQuery query) throws
      IOException {

      // only accept GET / POST
      RpcUtil.allowedMethods(query.method(), HttpMethod.GET.getName(), HttpMethod.POST.getName());

      final HashMap<String, String> version = new HashMap<String, String>();
      version.put("version", BuildData.version);
      version.put("short_revision", BuildData.short_revision);
      version.put("full_revision", BuildData.full_revision);
      version.put("timestamp", Long.toString(BuildData.timestamp));
      version.put("repo_status", BuildData.repo_status.toString());
      version.put("user", BuildData.user);
      version.put("host", BuildData.host);
      version.put("repo", BuildData.repo);
      version.put("branch", BuildData.branch);

      if (query.apiVersion() > 0) {
        query.sendReply(query.serializer().formatVersionV1(version));
      } else {
        final boolean json = query.request().getUri().endsWith("json");
        if (json) {
          query.sendReply(JSON.serializeToBytes(version));
        } else {
          final String revision = BuildData.revisionString();
          final String build = BuildData.buildString();
          StringBuilder buf;
          buf = new StringBuilder(2 // For the \n's
                                  + revision.length() + build.length());
          buf.append(revision).append('\n').append(build).append('\n');
          query.sendReply(buf);
        }
      }
    }
  }

  /** The /api/formatters endpoint
   * @since 2.0 */
  private static final class Serializers implements HttpRpc {
    public void execute(final TSDB tsdb, final HttpQuery query)
      throws IOException {
      // only accept GET / POST
      RpcUtil.allowedMethods(query.method(), HttpMethod.GET.getName(), HttpMethod.POST.getName());

      switch (query.apiVersion()) {
        case 0:
        case 1:
          query.sendReply(query.serializer().formatSerializersV1());
          break;
        default:
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
              "Requested API version not implemented", "Version " +
              query.apiVersion() + " is not implemented");
      }
    }
  }

  private static final class ShowConfig implements HttpRpc {
    @Override
    public void execute(TSDB tsdb, HttpQuery query) throws IOException {
      // only accept GET/POST
      RpcUtil.allowedMethods(query.method(), HttpMethod.GET.getName(), HttpMethod.POST.getName());

      final String[] uri = query.explodeAPIPath();
      final String endpoint = uri.length > 1 ? uri[1].toLowerCase() : "";

      if (endpoint.equals("filters")) {
        switch (query.apiVersion()) {
        case 0:
        case 1:
          query.sendReply(query.serializer().formatFilterConfigV1(
              TagVFilter.loadedFilters()));
          break;
        default:
          throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
              "Requested API version not implemented", "Version " +
              query.apiVersion() + " is not implemented");
        }
      } else {
        switch (query.apiVersion()) {
          case 0:
          case 1:
            query.sendReply(query.serializer().formatConfigV1(tsdb.getConfig()));
            break;
          default:
            throw new BadRequestException(HttpResponseStatus.NOT_IMPLEMENTED,
                "Requested API version not implemented", "Version " +
                query.apiVersion() + " is not implemented");
        }
      }
    }
  }

}
