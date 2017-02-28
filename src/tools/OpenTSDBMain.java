// This file is part of OpenTSDB.
// Copyright (C) 2016  The OpenTSDB Authors.
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Method;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.net.URLConnection;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import javax.management.ObjectName;

import net.opentsdb.core.TSDB;
import net.opentsdb.tools.ConfigArgP.ConfigurationItem;
import net.opentsdb.tsd.PipelineFactory;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.Threads;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.socket.ServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioServerBossPool;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.channel.socket.oio.OioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.status.StatusListener;

/**
 * <p>Title: Main</p>
 * <p>Description: OpenTSDB fat-jar main entry point</p> 
 */

public class OpenTSDBMain {
  /** The platform EOL string */
  public static final String EOL = System.getProperty("line.separator", "\n");
  /** Static class logger */
  private static final Logger log = LoggerFactory.getLogger(OpenTSDBMain.class);
  /** The content prefix  */
  public static final String CONTENT_PREFIX = "queryui";

  /** The default pid file directory which is <b><code>${user.home}/.tsdb/</code></b> */
  public static final File DEFAULT_PID_DIR = new File(System.getProperty("user.home") + File.separator + ".tsdb");
  /** The default pid file which is <b><code>${user.home}/.tsdb/opentsdb.pid</code></b> */
  public static final File DEFAULT_PID_FILE = new File(DEFAULT_PID_DIR, "opentsdb.pid");

  /** The default flush interval */
  public static final short DEFAULT_FLUSH_INTERVAL = 1000;

  /** Boot classes keyed by the commands we recognize */
  public static final Map<String, Class<?>> COMMANDS;
  
  static {
    Map<String, Class<?>> tmp = new HashMap<String, Class<?>>();
    tmp.put("fsck", Fsck.class);
    tmp.put("import", TextImporter.class);
    tmp.put("mkmetric", UidManager.class); // -> shift --> set uid assign metrics "$@"
    tmp.put("query", CliQuery.class);
    tmp.put("tsd", OpenTSDBMain.class);
    tmp.put("scan", DumpSeries.class);
    tmp.put("uid", UidManager.class);
    tmp.put("exportui", UIContentExporter.class);
    tmp.put("help", HelpProcessor.class);
    COMMANDS = Collections.unmodifiableMap(tmp);
  }
  
  /**
   * Prints the main usage banner
   */
  public static void mainUsage(PrintStream ps) {
    StringBuilder b = new StringBuilder("\nUsage: java -jar [opentsdb.jar] [command] [args]\nValid commands:")
      .append("\n\ttsd: Starts a new TSDB instance")
      .append("\n\tfsck: Searches for and optionally fixes corrupted data in a TSDB")
      .append("\n\timport: Imports data from a file into HBase through a TSDB")
      .append("\n\tmkmetric: Creates a new metric")
      .append("\n\tquery: Queries time series data from a TSDB ")
      .append("\n\tscan: Dumps data straight from HBase")
      .append("\n\tuid: Provides various functions to search or modify information in the tsdb-uid table. ")
      .append("\n\texportui: Exports the OpenTSDB UI static content")
      .append("\n\n\tUse help <command> for details on a command\n");
    ps.println(b);
  }
  
  
  /**
   * The OpenTSDB fat-jar main entry point
   * @param args See usage banner {@link OpenTSDBMain#mainUsage(PrintStream)}
   */
  public static void main(String[] args) {
      log.info("Starting.");
      log.info(BuildData.revisionString());
      log.info(BuildData.buildString());
      try {
        System.in.close();  // Release a FD we don't need.
      } catch (Exception e) {
        log.warn("Failed to close stdin", e);
      }
      if(args.length==0) {
        log.error("No command supplied");
        mainUsage(System.err);
        System.exit(-1);
      }
      // This is not normally needed since values passed on the CL are auto-trimmed,
      // but since the Main may be called programatically in some embedded scenarios,
      // let's save us some time and trim the values here.
    for(int i = 0; i < args.length; i++) {
      args[i] = args[i].trim();
    }     
      String targetTool = args[0].toLowerCase();
      if(!COMMANDS.containsKey(targetTool)) {
        log.error("Command not recognized: [" + targetTool + "]");
        mainUsage(System.err);
        System.exit(-1);        
      }     
      process(targetTool, shift(args));     
  }
  
  /**
   * Executes the target tool
   * @param targetTool the name of the target tool to execute
   * @param args The command line arguments minus the tool name
   */
  private static void process(String targetTool, String[] args) {
    if("mkmetric".equals(targetTool)) {
      shift(args);
    } 
    if(!"tsd".equals(targetTool)) {     
      try {
        COMMANDS.get(targetTool).getDeclaredMethod("main", String[].class).invoke(null, new Object[] {args});
      } catch(Exception x) {
          log.error("Failed to call [" + targetTool + "].", x);
          System.exit(-1);
      }     
    } else {
      launchTSD(args);
    }
  }
  
  
  /**
   * Applies and processes the pre-tsd command line
   * @param cap The main configuration wrapper
   * @param argp The preped command line argument handler 
   */
  protected static void applyCommandLine(ConfigArgP cap, ArgP argp) {
    // --config, --include-config, --help
    if(argp.has("--help")) {
      if(cap.hasNonOption("extended")) {
        System.out.println(cap.getExtendedUsage("tsd extended usage:"));
      } else {
        System.out.println(cap.getDefaultUsage("tsd usage:"));
      }     
      System.exit(0);
    }
    if(argp.has("--config")) {
      loadConfigSource(cap, argp.get("--config").trim());
    }
    if(argp.has("--include")) {
      String[] sources = argp.get("--include").split(",");
      for(String s: sources) {
        loadConfigSource(cap, s.trim());
      }
    }
  }
  
  /**
   * Applies the properties from the named source to the main configuration
   * @param config the main configuration to apply to 
   * @param source the name of the source to apply properties from
   */
  protected static void loadConfigSource(ConfigArgP config, String source) {
    Properties p = loadConfig(source);
    Config c = config.getConfig();
    for(String key: p.stringPropertyNames()) {
      String value = p.getProperty(key);
      ConfigurationItem ci = config.getConfigurationItem(key);      
      if(ci!=null) { // if we recognize the key, validate it
        ci.setValue(value);
      }
      c.overrideConfig(key, value);     
    }
  }
  
  /**
   * Loads properties from a file or url with the passed name
   * @param name The name of the file or URL
   * @return the loaded properties
   */
  protected static Properties loadConfig(String name) {
    try {
      URL url = new URL(name);
      return loadConfig(url);
    } catch (Exception ex) {
      return loadConfig(new File(name));
    }
  }
  
  /**
   * Loads properties from the passed input stream
   * @param source The name of the source the properties are being loaded from
   * @param is The input stream to load from
   * @return the loaded properties
   */
  protected static Properties loadConfig(String source, InputStream is) {
    try {
      Properties p = new Properties();
      p.load(is);
      // trim the value as it may have trailing white-space
      Set<String> keys = p.stringPropertyNames();
      for(String key: keys) {
        p.setProperty(key, p.getProperty(key).trim());
      }
      return p;
    } catch (IllegalArgumentException iae) {
      throw iae;
    } catch (Exception ex) {
      throw new IllegalArgumentException("Failed to load configuration from [" + source + "]");
    }
  }
  
  /**
   * Loads properties from the passed file
   * @param file The file to load from
   * @return the loaded properties
   */
  protected static Properties loadConfig(File file) {
    InputStream is = null;
    try {
      is = new FileInputStream(file);
      return loadConfig(file.getAbsolutePath(), is);     
    } catch (Exception ex) {
      throw new IllegalArgumentException("Failed to load configuration from [" + file.getAbsolutePath() + "]");
    }finally {
      if(is!=null) try { is.close(); } catch (Exception ex) { /* No Op */ }
    }
  }
  
  /**
   * Loads properties from the passed URL
   * @param url The url to load from
   * @return the loaded properties
   */
  protected static Properties loadConfig(URL url) {
    InputStream is = null;
    try {
      URLConnection connection = url.openConnection();
      if(connection instanceof HttpURLConnection) {
        ((HttpURLConnection)connection).setConnectTimeout(2000);
      }
      is = connection.getInputStream();
      return loadConfig(url.toString(), is);     
    } catch (Exception ex) {
      throw new IllegalArgumentException("Failed to load configuration from [" + url + "]");
    }finally {
      if(is!=null) try { is.close(); } catch (Exception ex) { /* No Op */ }
    }
  }
  
    /** Prints usage and exits with the given retval. */
    static void usage(final ArgP argp, final String errmsg, final int retval) {
      System.err.println(errmsg);
      System.err.println(new ConfigArgP().getDefaultUsage());
      if (argp != null) {
        System.err.print(argp.usage());
      }
      System.exit(retval);
    }
  
  
  
  /**
   * Starts the TSD.
   * @param args The command line arguments
   */
  private static void launchTSD(String[] args) {
    ConfigArgP cap = new ConfigArgP(args);
    Config config = cap.getConfig();
    ArgP argp = cap.getArgp();
    applyCommandLine(cap, argp);
    config.loadStaticVariables();   
    // All options are now correctly set in config
    setJVMName(config.getInt("tsd.network.port"), config.getString("tsd.network.bind"));
    // Configure the logging
    if(config.hasProperty("tsd.logback.file")) {
      final String logBackFile = config.getString("tsd.logback.file");
      final String rollPattern = config.hasProperty("tsd.logback.rollpattern") ? config.getString("tsd.logback.rollpattern") : null;
      final boolean keepConsoleOpen = config.hasProperty("tsd.logback.console") ? config.getBoolean("tsd.logback.console") : false;       
      log.info("\n\t===================================\n\tReconfiguring logback. Logging to file:\n\t{}\n\t===================================\n", logBackFile);
      setLogbackInternal(logBackFile, rollPattern, keepConsoleOpen);
    } else {      
      final String logBackConfig;
      if(config.hasProperty("tsd.logback.config")) {
        logBackConfig = config.getString("tsd.logback.config");
      } else {
        logBackConfig = System.getProperty("tsd.logback.config", null);
      }
      if(logBackConfig!=null && !logBackConfig.trim().isEmpty() && new File(logBackConfig.trim()).canRead()) {
        setLogbackExternal(logBackConfig.trim());
      }
    }   
    if(config.auto_metric()) {
      log.info("\n\t==========================================\n\tAuto-Metric Enabled\n\t==========================================\n");
    } else {
      log.warn("\n\t==========================================\n\tAuto-Metric Disabled\n\t==========================================\n");
    }
    try {
        // Write the PID file
        writePid(config.getString("tsd.process.pid.file"), config.getBoolean("tsd.process.pid.ignore.existing"));       
        // Export the UI content
        if(!config.getBoolean("tsd.ui.noexport")) {
          loadContent(config.getString("tsd.http.staticroot"));
        }
        // Create the cache dir if it does not exist
        File cacheDir = new File(config.getString("tsd.http.cachedir"));
        if(cacheDir.exists()) {
          if(!cacheDir.isDirectory()) {
            throw new IllegalArgumentException("The http cache directory [" + cacheDir + "] is not a directory, but a file, which is bad");
          }
        } else {
          if(!cacheDir.mkdirs()) {
            throw new IllegalArgumentException("Failed to create the http cache directory [" + cacheDir + "]");
          }
        }
    } catch (Exception ex) {
      log.error("Failed to process tsd configuration", ex);
      System.exit(-1);
    }
    
    //  =====================================================================    
    //  Command line processing complete, ready to start TSD.
    //  The code from here to the end of the method is an exact duplicate
    //  of {@link TSDMain#main(String[])} once configuration is complete.
    //  At the time of this writing, this is at line 123 starting with the
    //  code:  final ServerSocketChannelFactory factory;
    // =====================================================================     
     
    log.info("Configuration complete. Starting TSDB");
      final ServerSocketChannelFactory factory;
      if (config.getBoolean("tsd.network.async_io")) {
        int workers = Runtime.getRuntime().availableProcessors() * 2;
        if (config.hasProperty("tsd.network.worker_threads")) {
          try {
          workers = config.getInt("tsd.network.worker_threads");
          } catch (NumberFormatException nfe) {
            usage(argp, "Invalid worker thread count", 1);
          }
        }
        final Executor executor = Executors.newCachedThreadPool();
        final NioServerBossPool boss_pool = 
            new NioServerBossPool(executor, 1, new Threads.BossThreadNamer());
        final NioWorkerPool worker_pool = new NioWorkerPool(executor, 
            workers, new Threads.WorkerThreadNamer());
        factory = new NioServerSocketChannelFactory(boss_pool, worker_pool);
      } else {
        factory = new OioServerSocketChannelFactory(
            Executors.newCachedThreadPool(), Executors.newCachedThreadPool());
      }
      
      TSDB tsdb = null;
      try {
        tsdb = new TSDB(config);
        tsdb.initializePlugins(true);
        
        // Make sure we don't even start if we can't find our tables.
        tsdb.checkNecessaryTablesExist().joinUninterruptibly();

        registerShutdownHook(tsdb);
        final ServerBootstrap server = new ServerBootstrap(factory);

        server.setPipelineFactory(new PipelineFactory(tsdb));
        if (config.hasProperty("tsd.network.backlog")) {
          server.setOption("backlog", config.getInt("tsd.network.backlog")); 
        }
        server.setOption("child.tcpNoDelay", 
            config.getBoolean("tsd.network.tcp_no_delay"));
        server.setOption("child.keepAlive", 
            config.getBoolean("tsd.network.keep_alive"));
        server.setOption("reuseAddress", 
            config.getBoolean("tsd.network.reuse_address"));

        // null is interpreted as the wildcard address.
        InetAddress bindAddress = null;
        if (config.hasProperty("tsd.network.bind")) {
          bindAddress = InetAddress.getByName(config.getString("tsd.network.bind"));
        }

        // we validated the network port config earlier
        final InetSocketAddress addr = new InetSocketAddress(bindAddress,
            config.getInt("tsd.network.port"));
        server.bind(addr);
        log.info("Ready to serve on " + addr);
      } catch (Throwable e) {
        factory.releaseExternalResources();
        try {
          if (tsdb != null)
            tsdb.shutdown().joinUninterruptibly();
        } catch (Exception e2) {
          log.error("Failed to shutdown HBase client", e2);
        }
        throw new RuntimeException("Initialization failed", e);
      }
      // The server is now running in separate threads, we can exit main.
  }
  
  
  /**
   * Attempts to set the vm agent property that identifies the vm's display name.
   * This is the name displayed for tools such as jconsole and jps when using auto-dicsovery.
   * When using a fat-jar, this provides a much more identifiable name
   * @param port The listening port
   * @param iface The bound interface
   */
  protected static void setJVMName(final int port, final String iface) {
    final Properties p = getAgentProperties();
    if(p!=null) {
      final String ifc = (iface==null || iface.trim().isEmpty()) ? "" : (iface.trim() + ":");
      final String name = "opentsdb[" + ifc + port + "]";
      p.setProperty("sun.java.command", name);
      p.setProperty("sun.rt.javaCommand", name);
      System.setProperty("sun.java.command", name);
      System.setProperty("sun.rt.javaCommand", name);     
    }
  }
  
  /**
   * Returns the agent properties
   * @return the agent properties or null if reflective call failed
   */
  protected static Properties getAgentProperties() {
    try {
      Class<?> clazz = Class.forName("sun.misc.VMSupport");
      Method m = clazz.getDeclaredMethod("getAgentProperties");
      m.setAccessible(true);
      Properties p = (Properties)m.invoke(null);
      return p;
    } catch (Throwable t) {
      return null;
    }   
  }
  
  /**
   * Sets the logback system property, <b><code>tsdb.logback.file</code></b> and then
   * reloads the internal file based logging configuration. The property will 
   * not be set if it was already set, allowing the value to be set by a standard 
   * command line set system property.
   * @param logFileName The name of the file logback will log to.
   * @param rollPattern The pattern specifying the rolling file pattern, 
   * inserted between the file name base and extension. So for a log file name of 
   * <b><code>/var/log/opentsdb.log</code></b> and a pattern of <b><code>_%d{yyyy-MM-dd}.%i</code></b>,
   * the configured fileNamePattern would be <b><code>/var/log/opentsdb_%d{yyyy-MM-dd}.%i.log</code></b> 
   * @param keepConsoleOpen If true, will keep the console appender open, 
   * otherwise closes it and logs exclusively to the file.
   */
  protected static void setLogbackInternal(final String logFileName, final String rollPattern, final boolean keepConsoleOpen) {
    if(System.getProperty("tsdb.logback.file", null) == null) {
      System.setProperty("tsdb.logback.file", logFileName); 
    }
    System.setProperty("tsd.logback.rollpattern", insertPattern(logFileName, rollPattern));
    BasicStatusManager bsm = new BasicStatusManager();
    for(StatusListener listener: bsm.getCopyOfStatusListenerList()) {
      log.info("Status Listener: {}", listener);
    }
    try {
      final URL url = OpenTSDBMain.class.getClassLoader().getResource("file-logback.xml");
      final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
      try {
        final JoranConfigurator configurator = new JoranConfigurator();
        configurator.setContext(context);
        configurator.doConfigure(url);
        if(!keepConsoleOpen) {
          final ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
          root.detachAppender("STDOUT");
        }
        log.info("Set internal logback config with file [{}] and roll pattern [{}]", logFileName, rollPattern);
      } catch (JoranException je) {
        System.err.println("Failed to configure internal logback");
        je.printStackTrace(System.err);
      }                       
    } catch (Exception ex) {
      log.warn("Failed to set internal logback config with file [{}]", logFileName, ex);
    }
  }
  
  /**
   * Merges the roll pattern name into the file name 
   * @param logFileName The log file name
   * @param rollPattern The rolling log file appender roll pattern
   * @return the merged file pattern
   */
  protected static String insertPattern(final String logFileName, final String rollPattern) {   
    int index = logFileName.lastIndexOf('.');
    if(index==-1) return logFileName + rollPattern;
    return logFileName.substring(0, index) + rollPattern + logFileName.substring(index); 
  }
  
  
  /**
   * Reloads the logback configuration to an external file
   * @param fileName The logback configuration file
   */
  protected static void setLogbackExternal(final String fileName) {
    try {
      final ObjectName logbackObjectName = new ObjectName("ch.qos.logback.classic:Name=default,Type=ch.qos.logback.classic.jmx.JMXConfigurator");
      ManagementFactory.getPlatformMBeanServer().invoke(logbackObjectName, "reloadByFileName", new Object[]{fileName}, new String[]{String.class.getName()});
      log.info("Set external logback config to [{}]", fileName);
    } catch (Exception ex) {
      log.warn("Failed to set external logback config to [{}]", fileName, ex);
    }
  }
  

  /**
   * Drops the first array item in the passed array.
   * If the passed array is null or empty, returns an empty array
   * @param args The array to shift
   * @return the shifted array
   */
  private static String[] shift(String[] args) {
    if(args==null || args.length==0 | args.length==1) return new String[0];
    String[] newArgs = new String[args.length-1];
    System.arraycopy(args, 1, newArgs, 0, newArgs.length);
    return newArgs;
  }
  
    
    private static void registerShutdownHook(final TSDB tsdb) {
        final class TSDBShutdown extends Thread {
          public TSDBShutdown() {
            super("TSDBShutdown");
          }
          public void run() {
            try {
              tsdb.shutdown().join();
            } catch (Exception e) {
              LoggerFactory.getLogger(TSDBShutdown.class)
                .error("Uncaught exception during shutdown", e);
            }
          }
        }
        Runtime.getRuntime().addShutdownHook(new TSDBShutdown());
      }
    
  
    /**
   * <p>Title: HelpProcessor</p>
   * <p>Description: Command line help processor</p> 
   */
  public static class HelpProcessor {
      /**
       * Entry point for invoking the ui content exporter
       * @param args see the ArgP
       */
      public static void main(String[] args) {
        if(args==null || args.length==0) {
          mainUsage(System.out);
        } else {
          Class<?> command = COMMANDS.get(args[0].trim());
          if(command==null) {
            System.err.println("\nUnrecognized command [" + args[0] + "]\n");
            mainUsage(System.err);
          } else {
            try {
              if(args[0].equals("tsd")) {
                ConfigArgP cap = new ConfigArgP(); 
                if(args.length>1 && args[1].trim().equals("extended")) {
                  System.out.println(cap.getExtendedUsage("tsd extended usage:"));
                } else {
                  System.out.println(cap.getExtendedUsage("tsd usage:"));
                }
              } else {
                Method m = null;
                ArgP fake = new ArgP();
                try {               
                  m = command.getDeclaredMethod("usage", ArgP.class, String.class);
                  m.setAccessible(true);
                  m.invoke(null, new Object[] {fake, "\nHelp for [" + args[0] + "] command\n"});
                } catch (NoSuchMethodException ne) {
                  try {
                    m = command.getDeclaredMethod("usage", ArgP.class, String.class, int.class);
                    m.setAccessible(true);
                    m.invoke(null, new Object[] {fake, "\nHelp for [" + args[0] + "] command\n", 1});
                  } catch (NoSuchMethodException ne2) {
                    m = command.getDeclaredMethod("usage", ArgP.class, int.class);
                    m.setAccessible(true);
                    m.invoke(null, new Object[] {fake, 1});                 
                  }
                }
              }
            } catch(Exception x) {
                log.error("Failed to invoke help for [" + args[0] + "].", x);
                System.exit(-1);
            }     
          }
        }
        System.exit(0);
      }
      
      /**
       * Prints help usage
       * @param argp Ignored
       * @param errmsg Ignored
       */
      static void usage(final ArgP argp, final String errmsg) {
          System.out.println("help: Prints the main command line help");
          System.out.println("help: <command> Prints help for the specified command");
      }
      
    }

  /**
   * <p>Title: UIContentExporter</p>
   * <p>Description: Exports the queryui content from the jar to the specified directory</p> 
   */
  public static class UIContentExporter {
    private static final ArgP uiexOptions = new ArgP();
    
    static {
      uiexOptions.addOption("--d", "DIR", "The directory to export the UI content to");
      uiexOptions.addOption("--p", "Create the directory if it does not exist");      
    }
    
    
    /**
     * Usage banner, args are not used, just approximating a consistent signature
     * @param ignored ignored
     * @param alsoIgnored ignored
     */
    public static void usage(ArgP ignored, int alsoIgnored) {
      System.out.println("Usage:  java -jar <opentsdb.jar> exportui --d <destination> [--p]\n" + 
          uiexOptions.usage() );
      
    }
    
    
    /**
     * Entry point for invoking the ui content exporter
     * @param args see the ArgP
     */
    public static void main(String[] args) {
      uiexOptions.parse(args);
      String dirName = uiexOptions.get("--d");
      boolean createIfNotExists = uiexOptions.has("--p");
      if(dirName==null) {
        log.error("Missing argument for target directory. Usage:  java -jar <opentsdb.jar> exportui --d <destination> [--p]");
        System.exit(1);
      }
      File f = new File(dirName);
      if(!f.exists()) {
        if(createIfNotExists) {
          if(f.mkdirs()) {
            log.info("Created exportui directory [{}]", f);
          } else {
            log.error("Failed to create target directory [{}]", f);
            System.exit(1);
          }
        } else {
          log.error("Specified target directory [{}] does not exist. You could use the --p option, or create the directory", f);
          System.exit(1);
        }
      } else {
        if(!f.isDirectory()) {
          log.error("Specified target [{}] is not a directory, but is a file. exportui cannot contine", f);
          System.exit(1);
        }
      }
      loadContent(f.getAbsolutePath());
      System.exit(0);     
    }
  }
  /**
   * Loads the Static UI content files from the classpath JAR to the configured static root directory
   * @param the name of the content directory to write the content to
   */
  private static void loadContent(String contentDirectory) {    
    File gpDir = new File(contentDirectory);
    final long startTime = System.currentTimeMillis();
    int filesLoaded = 0;
    int fileFailures = 0;
    int fileNewer = 0;
    long bytesLoaded = 0;
    String codeSourcePath = TSDMain.class.getProtectionDomain().getCodeSource().getLocation().getPath();
    File file = new File(codeSourcePath);
    if( codeSourcePath.endsWith(".jar") && file.exists() && file.canRead() ) {
      JarFile jar = null;
      ChannelBuffer contentBuffer = ChannelBuffers.dynamicBuffer(300000);
      try {
        jar = new JarFile(file);
        final Enumeration<JarEntry> entries = jar.entries(); 
        while(entries.hasMoreElements()) {
          JarEntry entry = entries.nextElement();
          final String name = entry.getName();
          if (name.startsWith(CONTENT_PREFIX + "/")) { 
            final int contentSize = (int)entry.getSize();
            final long contentTime = entry.getTime();
            if(entry.isDirectory()) {
              new File(gpDir, name).mkdirs();
              continue;
            }
            File contentFile = new File(gpDir, name.replace(CONTENT_PREFIX + "/", ""));
            if( !contentFile.getParentFile().exists() ) {
              contentFile.getParentFile().mkdirs();
            }
            if( contentFile.exists() ) {
              if( contentFile.lastModified() >= contentTime ) {
                log.debug("File in directory was newer [{}]", name);
                fileNewer++;
                continue;
              }
              contentFile.delete();
            }
            log.debug("Writing content file [{}]", contentFile );
            contentFile.createNewFile();
            if( !contentFile.canWrite() ) {
              log.warn("Content file [{}] not writable", contentFile);
              fileFailures++;
              continue;
            }
            FileOutputStream fos = null;
            InputStream jis = null;
            try {
              fos = new FileOutputStream(contentFile);
              jis = jar.getInputStream(entry);
              contentBuffer.writeBytes(jis, contentSize);
              contentBuffer.readBytes(fos, contentSize);
              fos.flush();
              jis.close(); jis = null;
              fos.close(); fos = null;
              filesLoaded++;
              bytesLoaded += contentSize;
              log.debug("Wrote content file [{}] + with size [{}]", contentFile, contentSize );
            } finally {
              if( jis!=null ) try { jis.close(); } catch (Exception ex) {}
              if( fos!=null ) try { fos.close(); } catch (Exception ex) {}
            }
          }  // not content
        } // end of while loop
        final long elapsed = System.currentTimeMillis()-startTime;
        StringBuilder b = new StringBuilder("\n\n\t===================================================\n\tStatic Root Directory:[").append(contentDirectory).append("]");
        b.append("\n\tTotal Files Written:").append(filesLoaded);
        b.append("\n\tTotal Bytes Written:").append(bytesLoaded);
        b.append("\n\tFile Write Failures:").append(fileFailures);
        b.append("\n\tExisting File Newer Than Content:").append(fileNewer);
        b.append("\n\tElapsed (ms):").append(elapsed);
        b.append("\n\t===================================================\n");
        log.info(b.toString());
      } catch (Exception ex) {
        log.error("Failed to export ui content", ex);       
      } finally {
        if( jar!=null ) try { jar.close(); } catch (Exception x) { /* No Op */}
      }
    }  else { // end of was-not-a-jar
      log.warn("\n\tThe OpenTSDB classpath is not a jar file, so there is no content to unload.\n\tBuild the OpenTSDB jar and run 'java -jar <jar> --d <target>'.");
    }
  }
  
    /**
     * Writes the PID to the file at the passed location
     * @param file The fully qualified pid file name
     * @param ignorePidFile If true, an existing pid file will be ignored after a warning log
     */
    private static void writePid(String file, boolean ignorePidFile) {
      File pidFile = new File(file);
      if(pidFile.exists()) {
        Long oldPid = getPid(pidFile);
        if(oldPid==null) {
          pidFile.delete();
        } else {
          log.warn("\n\t==================================\n\tThe OpenTSDB PID file [" + file + "] already exists for PID [" + oldPid + "]. \n\tOpenTSDB might already be running.\n\t==================================\n");
          if(!ignorePidFile) {
            log.warn("Exiting due to existing pid file. Start with option --ignore-existing-pid to overwrite"); 
            System.exit(-1);
          } else {
            log.warn("Deleting existing pid file [" + file + "]");
            pidFile.delete();
          }
        }
      }
      pidFile.deleteOnExit();
      File pidDir = pidFile.getParentFile();
      FileOutputStream fos = null;
      try {
        if(!pidDir.exists()) {
          if(!pidDir.mkdirs()) {
            throw new Exception("Failed to create PID directory [" + file + "]");
          }
        }     
        fos = new FileOutputStream(pidFile);      
        String PID = ManagementFactory.getRuntimeMXBean().getName().split("@")[0];
        fos.write(String.format("%s%s", PID, EOL).getBytes());
        fos.flush();
        fos.close();
        fos = null;
        log.info("PID [" + PID + "] written to pid file [" + file + "]");
      } catch (Exception ex) {
        log.error("Failed to write PID file to [" + file + "]", ex);
        throw new IllegalArgumentException("Failed to write PID file to [" + file + "]", ex);
      } finally {
        if(fos!=null) try { fos.close(); } catch (Exception ex) { /* No Op */ }
      }
    }

    /**
     * Reads the pid from the specified pid file
     * @param pidFile The pid file to read from
     * @return The read pid or possibly null / blank if failed to read
     */
    private static Long getPid(File pidFile) {
      FileReader reader = null;
      BufferedReader lineReader = null;
      String pidLine  = null;
      try {
        reader = new FileReader(pidFile);
        lineReader = new BufferedReader(reader);
        pidLine = lineReader.readLine();
        if(pidLine!=null) {
          pidLine = pidLine.trim();     
        }   
      } catch (Exception ex) {
        log.error("Failed to read PID from file  [" + pidFile.getAbsolutePath() + "]", ex);
      } finally {
        if(reader!=null) try { reader.close(); } catch (Exception ex) { /* No Op */ }
      }
      try {
        return Long.parseLong(pidLine);
      } catch (Exception ex) {
        return null;
      }     
    }

}