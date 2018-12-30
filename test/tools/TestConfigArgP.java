// This file is part of OpenTSDB.
// Copyright (C) 2014  The OpenTSDB Authors.
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
package net.opentsdb.tools;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.Thread.UncaughtExceptionHandler;
import java.lang.management.ManagementFactory;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import net.opentsdb.tools.ConfigArgP.ConfigurationItem;
import net.opentsdb.utils.Config;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelUpstreamHandler;
import org.jboss.netty.channel.socket.ServerSocketChannel;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpChunkAggregator;
import org.jboss.netty.handler.codec.http.HttpHeaders;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * <p>Title: TestConfigArgP</p>
 * <p>Description: Test cases for the fat-jar launcher configuration manager</p> 
 */
public class TestConfigArgP {
  /** The number of cores available to this JVM */
  static final int CORES = ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors();
  /** Platform EOL */ 
  static final String EOL = System.getProperty("line.separator", "\n");
  /** The quickie web server */
  static QuickieWebServer webServer = null;
  /** The port the web server is listening on */
  static int port = -1;
  /** true/false string values */
  static final Set<String> trueFalseValues = Collections.unmodifiableSet(new HashSet<String>(Arrays.asList("true", "false")));
  
  static final boolean runningFatJar;
  
  static {
    boolean tmp = false;
    InputStream is = null;
    try {
      is = TestConfigArgP.class.getClassLoader().getResourceAsStream("opentsdb.conf.json");
      BufferedReader bin = new BufferedReader(new InputStreamReader(is));
      StringBuilder b = new StringBuilder();
      String line = null;
      while((line = bin.readLine())!=null) {
        b.append(line);
      }
      JSONObject jo = new JSONObject(b.toString());
      tmp = true;
    } catch (Exception x) {
      tmp = false;
    } finally {
      if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
    } 
    runningFatJar = tmp;
  }
  
  /**
   * Loads a fresh new JSONObject with the config 
   * from the classpath loaded <code>opentsdb.conf.json</code>.
   * @return A loaded JSONObject
   */
  static JSONObject newTSDBConfig() {
    InputStream is = null;
    try {
      is = TestConfigArgP.class.getClassLoader().getResourceAsStream("opentsdb.conf.json");
      BufferedReader bin = new BufferedReader(new InputStreamReader(is));
      StringBuilder b = new StringBuilder();
      String line = null;
      while((line = bin.readLine())!=null) {
        b.append(line);
      }
      return new JSONObject(b.toString());
    } catch (Exception ex) {
      throw new RuntimeException("Failed to load opentsdb.conf.json from the classpath", ex);
    } finally {
      if(is!=null) try { is.close(); } catch (Exception x) {/* No Op */}
    }   
  }
  
  
  
  /**
   * Creates a new QuickieResponder that will server the passed content
   * @param uri The uri to register the responder for
   * @param content The content to serve
   * @param contentType The optional content type
   * @return the URL to get the content with
   */
  public static String newResponderForContent(final String uri, final String content, final String contentType) {   
    final QuickieResponder qr = new QuickieResponder(){
      @Override
      public void writeResponse(final HttpResponse response) {
        final byte[] bytes = content.getBytes(Charset.defaultCharset());
        HttpHeaders.addHeader(response, Names.CONTENT_TYPE, contentType==null ? "text/plain" : contentType);
        HttpHeaders.addHeader(response, Names.CONTENT_LENGTH, bytes.length);
        response.setContent(ChannelBuffers.wrappedBuffer(bytes));       
      }
    };
    webServer.addResponder(uri, qr);
    InetSocketAddress isa = webServer.serverChannel.getLocalAddress();
    return String.format("http://%s:%s%s", isa.getAddress().getHostAddress(), isa.getPort(), uri);
  }
  

  
  
  
  /**
   * Starts the test web server
   */
  @BeforeClass
  public static void startHttpServer() {    
    webServer = new QuickieWebServer();
    port = webServer.getPort();   
    org.junit.Assume.assumeTrue(runningFatJar);
  }
  
  /**
   * Stops all the running servers
   */
  @AfterClass
  public static void stopHttpServer() {
    if(webServer!=null) {
      try { 
        webServer.stop();
        log("Stopped OIO HTTP Server on port [" + port + "]");
      } catch (Exception x) { /* No Op */ }
      webServer = null;
    }
  }
  
  
  
  /**
   * System out logger
   * @param format The message format
   * @param args The message arg tokens
   */
  public static void log(String format, Object...args) {
    System.out.println(String.format(format, args));
  }

  /**
   * Validates that a the config value for <code>tsd.network.worker_threads</code>
   * for which the default value is calced by a js scriptlet,
   * is two times the number of cores.
   * @throws Exception on any error
   */
  @Test
  public void testWorkerThreadDefault() throws Exception {
    ConfigArgP cap = new ConfigArgP();
    Config config = cap.getConfig();
    assertEquals("The value for key [tsd.network.worker_threads]", (CORES * 2), config.getInt("tsd.network.worker_threads"));
    // now test an override
    cap = new ConfigArgP("--worker-threads", "7");
    config = cap.getConfig();
    assertEquals("The overriden value for key [tsd.network.worker_threads]", 7, config.getInt("tsd.network.worker_threads"));
    // now test an override with an "="
    cap = new ConfigArgP("--worker-threads=7");
    config = cap.getConfig();
    assertEquals("The overriden value for key [tsd.network.worker_threads]", 7, config.getInt("tsd.network.worker_threads"));
    
  }
  
  /**
   * Validates that a ConfigArgP with no arguments creates a Config with the expected defaults
   * @throws Exception on any error
   */
  @Test
  public void testAllDefaults() throws Exception {
    ConfigArgP cap = new ConfigArgP();
    Config config = cap.getConfig();
    int validatedItems = 0;
    JSONArray configItems = newTSDBConfig().getJSONArray("config-items");
    for(int i = 0; i < configItems.length(); i++) {
      JSONObject configItem = configItems.getJSONObject(i);
      if(!configItem.has("defaultValue")) continue;
      String key = configItem.getString("key");
      String expectedValue = ConfigArgP
          // Converts any javascript evals or system property token decodes
          .processConfigValue(configItem.getString("defaultValue"));
      String value = config.getString(key);
      assertEquals("The value for key [" + key + "]", expectedValue, value);
      validatedItems++;
    }
    log("Validated %s Config Items", validatedItems);
  }
  
  /**
   * Validates that all {@link ConfigMetaType#BOOL} type config definitions have defaults, and vice-versa.
   * This is necessary to allow a command line specifier with no value (e.g. <code>--auto-metric</code>)
   * @throws Exception on any error
   */
  @Test
  public void testAllBoolMetaTypesHaveDefault() throws Exception {
    JSONArray configItems = newTSDBConfig().getJSONArray("config-items");
    for(int i = 0; i < configItems.length(); i++) {
      JSONObject configItem = configItems.getJSONObject(i);
      final String name = configItem.getString("key");
      String meta = configItem.has("meta") ? configItem.getString("meta") : null;
      if(!"BOOL".equals(meta)) continue;
      String defaultValue = configItem.has("defaultValue") ? configItem.getString("defaultValue") : null;
      assertNotNull("Config Item [" + name + "] of type BOOL has null default value", defaultValue);
      assertTrue("Config Item [" + name + "] of type BOOL has invalid default value", trueFalseValues.contains(defaultValue));
    }
    for(int i = 0; i < configItems.length(); i++) {
      JSONObject configItem = configItems.getJSONObject(i);
      final String name = configItem.getString("key");
      String defaultValue = configItem.has("defaultValue") ? configItem.getString("defaultValue") : null;
      if(defaultValue==null || !trueFalseValues.contains(defaultValue)) continue;     
      String meta = configItem.has("meta") ? configItem.getString("meta") : null;
      assertEquals("Config Item [" + name + "] with defaultValue of true/false meta", "BOOL", meta);
    }
    
  }
  
  /**
   * Validates that an HTTP URL defined config overrides the default values.
   * @throws Exception thrown on any error
   */ 
  @SuppressWarnings("unchecked")
  @Test
  public void httpExternalConfigTest() throws Exception {   
    try {
      final String url = newResponderForContent("/content", configToContent(
          new ConfigArgP().getConfig(), 
          Collections.singletonMap("tsd.network.worker_threads", "7")), 
          "plain/text");
      ConfigArgP cap = new ConfigArgP("--config", url); // include overwrites config
      Config config = cap.getConfig();
      assertEquals("The HTTP overriden value for key [tsd.network.worker_threads]", 7, config.getInt("tsd.network.worker_threads"));
    } finally {
      webServer.removeResponder("/content");
    }
  }
  
  /**
   * Validates that an HTTP URL defined include successfully overrides
   * the default values.
   * @throws Exception thrown on any error
   */ 
  @SuppressWarnings("unchecked")
  @Test
  public void httpIncludeOverrideTest() throws Exception {    
    try {
      final String url = newResponderForContent("/content", configToContent(
          new ConfigArgP().getConfig(), 
          Collections.singletonMap("tsd.network.worker_threads", "7")), 
          "plain/text");
      ConfigArgP cap = new ConfigArgP("--include", url); // include overwrites config
      Config config = cap.getConfig();
      assertEquals("The HTTP overriden value for key [tsd.network.worker_threads]", 7, config.getInt("tsd.network.worker_threads"));
    } finally {
      webServer.removeResponder("/content");
    }
  }
  
  
  /**
   * Validates that an HTTP URL defined command line config successfully loads from a URL
   * @throws Exception thrown on any error
   */ 
  @Test
  public void httpCommandLineURLConfigTest() throws Exception {   
    try {     
      final Properties p = contentToProps(ALL_NON_DEFAULTS);
      final String url = newResponderForContent("/content", ALL_NON_DEFAULTS, "plain/text");
      ConfigArgP cap = new ConfigArgP("--config", url);
      Config config = cap.getConfig();
      assertEquals("The HTTP overriden value for key [tsd.network.worker_threads]", (CORES * 3 + 1), config.getInt("tsd.network.worker_threads"));
      for(String key: p.stringPropertyNames()) {
        String value = p.getProperty(key);
        String cvalue = config.getString(key);
        assertEquals("The value for config item [" + key + "]", value, cvalue);
      }
    } finally {
      webServer.removeResponder("/content");
    }
  }
  
  /**
   * Validates that an HTTP URL defined command line config is overriden by an include config
   * @throws Exception thrown on any error
   */ 
  @Test
  public void httpCommandLineOverridenByIncludeTest() throws Exception {    
    try {     
      final Properties p = contentToProps(ALL_DEFAULTS);
      p.remove("tsd.network.worker_threads");
      final String url = newResponderForContent("/content", ALL_DEFAULTS, "plain/text");
      final String url2 = newResponderForContent("/content2", "tsd.network.worker_threads=5", "plain/text");
      ConfigArgP cap = new ConfigArgP("--config", url, "--include", url2);
      Config config = cap.getConfig();
      
      assertEquals("The HTTP overriden value for key [tsd.network.worker_threads]", 5, config.getInt("tsd.network.worker_threads"));
      // this is a bit of a hack (and redundant ?), but we're trying to test the rest of the values.
      for(String key: p.stringPropertyNames()) {
        ConfigurationItem di = cap.getDefaultItem(key);
        String value = di.isBool() ? // For a bool, presence means true
            cap.isClArg(di.getClOption()) ? "true" : di.getDefaultValue() 
            : p.getProperty(key);         
        String cvalue = config.getString(key);
        assertEquals("The value for config item [" + key + "]", value, cvalue);
      }
    } finally {
      webServer.removeResponder("/content");
    }
  }
  
  /**
   * Validates that BOOL type items with a default value of false 
   * eval as true if configured on the command line
   * @throws Exception thrown on any error
   */ 
  @Test
  public void validateCLEnablesFalseBools() throws Exception {
    // find all the applicable items, which are bools with a default value of false
    Set<String> keysToEnable = new HashSet<String>();
    Set<String> clsToEnable = new HashSet<String>();
    JSONArray configItems = newTSDBConfig().getJSONArray("config-items");
    for(int i = 0; i < configItems.length(); i++) {
      JSONObject configItem = configItems.getJSONObject(i);
      if(!configItem.has("meta")) continue;
      if("BOOL".equals(configItem.getString("meta")) && "false".equals(configItem.getString("defaultValue")) ) {
        keysToEnable.add(configItem.getString("key"));
        clsToEnable.add(configItem.getString("cl-option"));
      }
    }   
    ConfigArgP cap = new ConfigArgP(clsToEnable.toArray(new String[clsToEnable.size()]));
    Config cfg = cap.getConfig();
    for(String key: keysToEnable) {
      assertEquals("The bool value of [" + key + "]", "true", cfg.getString(key));
    }
    
  }
  
  
  /**
   * Converts the passed stringy to a properties instance
   * @param content The content to format
   * @return the properties instance
   */
  protected Properties contentToProps(final CharSequence content) {
    Properties p = new Properties();
    try {
      p.load(new StringReader(content.toString()));
      Properties converted = new Properties();
      for(String key: p.stringPropertyNames()) {
        converted.put(key, ConfigArgP.processConfigValue(p.getProperty(key)));        
      }
      return converted;
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
  
  /**
   * Converts the passed config to a readable properties file, applying the passed overrides
   * @param config The config to write
   * @param overrides The overrides to apply
   * @return The rendered content
   */
  protected String configToContent(final Config config, Map<String, String>...overrides) {
    Properties p = new Properties();
    for(Map.Entry<String, String> entry: config.getMap().entrySet()) {
      String vl = entry.getValue();
      if(vl==null || vl.trim().isEmpty()) continue;
      p.put(entry.getKey(), vl);
    }
    for(Map<String, String> map: overrides) {
      p.putAll(map);
    }
    StringBuilder b = new StringBuilder();
    for(String key: p.stringPropertyNames()) {
      b.append(key).append("=")
        .append(p.getProperty(key))
        .append(EOL);
    }
    return b.toString();
  }
  
  
  static interface QuickieResponder {
    public void writeResponse(final HttpResponse response);
  }
  
  static class QuickieWebServer extends SimpleChannelUpstreamHandler implements ChannelPipelineFactory {
    final OioServerSocketChannelFactory scf; 
    final ServerBootstrap bootstrap;
    final ServerSocketChannel serverChannel;
    final int port;
    /** Thread serial for the http server handler threads */
    final AtomicInteger serial = new AtomicInteger(0);
    /** The http server thread factory */
    final ThreadFactory threadFactory = new ThreadFactory() {
      @Override
      public Thread newThread(final Runnable r) {
        Thread t = new Thread(r, "HttpServerHandlerThread#" + serial.incrementAndGet());
        t.setDaemon(true);
        t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
          @Override
          public void uncaughtException(Thread t, Throwable e) {
            System.err.println("Uncaught exception on [" + t + "]");
            e.printStackTrace(System.err);          
          }
        });
        return t;
      }   
    };
    
    final ConcurrentHashMap<String, QuickieResponder> responders = new ConcurrentHashMap<String, QuickieResponder>();
    static final DefaultHttpResponse response404 = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NOT_FOUND);
    public QuickieWebServer() {
      scf = new OioServerSocketChannelFactory(Executors.newCachedThreadPool(threadFactory), Executors.newCachedThreadPool(threadFactory));
      bootstrap = new ServerBootstrap(scf);
      bootstrap.setOption("child.tcpNoDelay", true);
      bootstrap.setPipelineFactory(this);
      serverChannel = (ServerSocketChannel) bootstrap.bind(new InetSocketAddress("127.0.0.1", 0));
      port = serverChannel.getLocalAddress().getPort();
      log("Started OIO HTTP Server on port [" + port + "]");
    }
    
    public int getPort() {
      return port;
    }
    
    public void addResponder(final String uri, final QuickieResponder responder) {
      if(responders.putIfAbsent(uri, responder)!=null) throw new RuntimeException("Handler for URI [" + uri + "] exists");
    }
    
    public void removeResponder(final String uri) {
      responders.remove(uri);
    }

    public void stop() {
      scf.releaseExternalResources();
    }
    /**
     * {@inheritDoc}
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#messageReceived(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.MessageEvent)
     */
    @Override
    public void messageReceived(final ChannelHandlerContext ctx, final MessageEvent e) {
      Object msg = e.getMessage();      
      if (msg instanceof HttpRequest) {
        HttpRequest req = (HttpRequest)msg;
        QuickieResponder responder = responders.get(req.getUri());
        if(responder==null) {
          ctx.getChannel().write(response404).addListener(ChannelFutureListener.CLOSE);
        } else {
          DefaultHttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
          responder.writeResponse(response);
          ctx.getChannel().write(response).addListener(ChannelFutureListener.CLOSE);
        }
      }
    }

    /**
     * {@inheritDoc}
     * @see org.jboss.netty.channel.ChannelPipelineFactory#getPipeline()
     */
    @Override
    public ChannelPipeline getPipeline() throws Exception {
      ChannelPipeline pipeline = Channels.pipeline();
      pipeline.addLast("decoder", new HttpRequestDecoder());
      pipeline.addLast("aggregator", new HttpChunkAggregator(1048576));
      pipeline.addLast("encoder", new HttpResponseEncoder());
      pipeline.addLast("handler", this);
      return pipeline;
    }
    
     /**
     * {@inheritDoc}
     * @see org.jboss.netty.channel.SimpleChannelUpstreamHandler#exceptionCaught(org.jboss.netty.channel.ChannelHandlerContext, org.jboss.netty.channel.ExceptionEvent)
     */
    @Override
     public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) {
       e.getCause().printStackTrace();
       e.getChannel().close();
     }    
  }
  
  private static String ALL_DEFAULTS = "tsd.core.auto_create_tagvs=true\n" +
      "tsd.http.request.enable_chunked=false\n" +
      "tsd.network.tcp_no_delay=true\n" +
      "tsd.ui.noexport=false\n" +
      "tsd.core.preload_uid_cache.max_entries=300000\n" +
      "tsd.http.staticroot=${java.io.tmpdir}/.tsdb/static-content\n" +
      "tsd.storage.hbase.zk_basedir=/hbase\n" +
      "tsd.storage.flush_interval=1000\n" +
      "tsd.core.meta.enable_tsuid_tracking=false\n" +
      "tsd.core.auto_create_metrics=false\n" +
      "tsd.http.request.max_chunk=4096\n" +
      "tsd.search.enable=false\n" +
      "tsd.network.backlog=3072\n" +
      "tsd.logback.rollpattern=_%d{yyyy-MM-dd}.%i\n" +
      "tsd.http.request.cors_headers=Authorization, Content-Type, Accept, Origin, User-Agent, DNT, Cache-Control, X-Mx-ReqToken, Keep-Alive, X-Requested-With, If-Modified-Since\n" +
      "tsd.storage.hbase.data_table=tsdb\n" +
      "tsd.network.keep_alive=true\n" +
      "tsd.core.timezone=${user.timezone}\n" +
      "tsd.network.port=4242\n" +
      "tsd.core.auto_create_tagks=true\n" +
      "tsd.mode=rw\n" +
      "tsd.network.reuse_address=true\n" +
      "tsd.http.cachedir=${java.io.tmpdir}/.tsdb/http-cache/\n" +
      "tsd.core.meta.enable_realtime_ts=false\n" +
      "tsd.rtpublisher.enable=false\n" +
      "tsd.stats.canonical=false\n" +
      "tsd.process.pid.ignore.existing=false\n" +
      "tsd.core.meta.enable_tsuid_incrementing=false\n" +
      "tsd.core.socket.timeout=0\n" +
      "tsd.core.tree.enable_processing=false\n" +
      "tsd.storage.hbase.uid_table=tsdb-uid\n" +
      "tsd.storage.hbase.tree_table=tsdb-tree\n" +
      "tsd.process.pid.file=${java.io.tmpdir}/.tsdb/opentsdb.pid\n" +
      "tsd.core.preload_uid_cache=false\n" +
      "tsd.network.async_io=true\n" +
      "tsd.storage.fix_duplicates=false\n" +
      "tsd.network.bind=0.0.0.0\n" +
      "tsd.storage.hbase.zk_quorum=localhost\n" +
      "tsd.storage.enable_compaction=true\n" +
      "tsd.no_diediedie=false\n" +
      "tsd.network.worker_threads=$[new Integer(cores * 2)]\n" +
      "tsd.http.show_stack_trace=true\n" +
      "tsd.logback.console=false\n" +
      "tsd.core.meta.enable_realtime_uid=false\n" +
      "tsd.storage.hbase.meta_table=tsdb-meta\n";

  private static String ALL_NON_DEFAULTS = "tsd.core.auto_create_tagvs=true\n" +
      "tsd.http.request.enable_chunked=true\n" +
      "tsd.network.tcp_no_delay=false\n" +
      "tsd.ui.noexport=true\n" +
      "tsd.core.preload_uid_cache.max_entries=3\n" +
      "tsd.http.staticroot=${user.home}/.tsdb/static-content\n" +
      "tsd.storage.hbase.zk_basedir=/hbasex\n" +
      "tsd.storage.flush_interval=10\n" +
      "tsd.core.meta.enable_tsuid_tracking=true\n" +
      "tsd.core.auto_create_metrics=true\n" +
      "tsd.http.request.max_chunk=9203\n" +
      "tsd.search.enable=true\n" +
      "tsd.network.backlog=1\n" +
      "tsd.logback.rollpattern=_%d{yyyy}.%i\n" +
      "tsd.http.request.cors_headers=Authorization\n" +
      "tsd.storage.hbase.data_table=tsdbx\n" +
      "tsd.network.keep_alive=false\n" +
      "tsd.core.timezone=Pacific/Kiritimati\n" +
      "tsd.network.port=7272\n" +
      "tsd.core.auto_create_tagks=false\n" +
      "tsd.mode=ro\n" +
      "tsd.network.reuse_address=false\n" +
      "tsd.http.cachedir=${user.home}/.tsdb/http-cache/\n" +
      "tsd.core.meta.enable_realtime_ts=true\n" +
      "tsd.rtpublisher.enable=true\n" +
      "tsd.stats.canonical=true\n" +
      "tsd.process.pid.ignore.existing=true\n" +
      "tsd.core.meta.enable_tsuid_incrementing=true\n" +
      "tsd.core.socket.timeout=9024\n" +
      "tsd.core.tree.enable_processing=true\n" +
      "tsd.storage.hbase.uid_table=tsdb-uidx\n" +
      "tsd.storage.hbase.tree_table=tsdb-treex\n" +
      "tsd.process.pid.file=${user.home}/.tsdb/opentsdb.pid\n" +
      "tsd.core.preload_uid_cache=true\n" +
      "tsd.network.async_io=false\n" +
      "tsd.storage.fix_duplicates=true\n" +
      "tsd.network.bind=127.0.0.1\n" +
      "tsd.storage.hbase.zk_quorum=127.0.0.1\n" +
      "tsd.storage.enable_compaction=false\n" +
      "tsd.no_diediedie=true\n" +
      "tsd.network.worker_threads=$[new Integer(cores * 3 + 1)]\n" +
      "tsd.http.show_stack_trace=false\n" +
      "tsd.logback.console=true\n" +
      "tsd.core.meta.enable_realtime_uid=true\n" +
      "tsd.storage.hbase.meta_table=tsdb-metax\n";
  
}


