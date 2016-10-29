// This file is part of OpenTSDB.
// Copyright (C) 2010-2016  The OpenTSDB Authors.
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

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.management.ManagementFactory;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Bindings;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import net.opentsdb.utils.Config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * <p>Title: ConfigArgP</p>
 * <p>Description: Wraps {@link Config} and {@link ArgP} instances for a consolidated configuration and command line handler</p> 
 */

public class ConfigArgP {
  /** Static class logger */
  protected static final Logger LOG = LoggerFactory.getLogger(ConfigArgP.class);
  /** The command line argument holder for all (default and extended) options */
  protected final ArgP argp = new ArgP();
  /** The command line argument holder for default options */
  protected final ArgP dargp = new ArgP();
  /** The non config option arguments */
  protected String[] nonOptionArgs = {};
  /** The base configuration */
  protected Config config;
  /** The default configuration items keyed by the item key and cl-option */
  protected final Map<String, ConfigurationItem> defaultConfItems;
  /** The command line args */
  protected final Set<String> commandLineArgs;
  
  /** The raw configuration items loaded from the json file */
  protected final TreeSet<ConfigurationItem> configItemsByKey = new TreeSet<ConfigurationItem>();
  /** The raw configuration items loaded from the json file  */
  protected final TreeSet<ConfigurationItem> configItemsByCl = new TreeSet<ConfigurationItem>();
  
  /** The regex pattern to perform a substitution for <b><pre><code>${&lt;sysprop&gt;:&lt;default&gt;}</code></pre></b> patterns in strings */
  public static final Pattern SYS_PROP_PATTERN = Pattern.compile("\\$\\{(.*?)(?::(.*?))??\\}");
  /** The regex pattern to perform a substitution for <b><pre><code>$[&lt;javascript snippet&gt;]</code></pre></b> patterns in strings */
  public static final Pattern JS_PATTERN = Pattern.compile("\\$\\[(.*?)\\]", Pattern.MULTILINE);
  
  /** The config key for the TSD RPC addin classes */
  public static final String TSD_RPC_ADDIN_KEY = "tsd.addins.rpcs";
  /** The config key for the TSD RPC addin classpath */
  public static final String TSD_RPC_ADDIN_CP_KEY = "tsd.addins.rpcs.classpath";

  /** Indicates if we're on Windows, in which case the SysProp handling needs a few tweaks */
  public static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
  /** The JavaScript Engine to interpret <b><code>$[&lt;javascript snippet&gt;]</code></b> values */
  protected static final ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByExtension("js");
  /** The JavaScript bindings */
  protected static final Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE); 
  
  

  static {
    // Initialize js bindings
    Map<String, Object> map = new HashMap<String, Object>();
    for(String key: System.getProperties().stringPropertyNames()) {
      map.put(key, System.getProperty(key));
    }
    map.put("cores", ManagementFactory.getOperatingSystemMXBean().getAvailableProcessors());
    map.put("maxheap", Runtime.getRuntime().maxMemory());
    bindings.putAll(map);
    bindings.putAll(System.getenv());
    bindings.put("bindings", bindings);
  }
  
  /**
   * Creates a new ConfigArgP
   * @param args The command line arguments
   */
  public ConfigArgP(String...args) {
    InputStream is = null;
    commandLineArgs = Collections.unmodifiableSet(new LinkedHashSet<String>(Arrays.asList(args)));
    try {
      config = new NoLoadConfig();
      is = ConfigArgP.class.getClassLoader().getResourceAsStream("opentsdb.conf.json");
      ObjectMapper jsonMapper = new ObjectMapper();
      JsonNode root = jsonMapper.reader().readTree(is);
      JsonNode configRoot = root.get("config-items");
      scriptEngine.eval("var config = " + configRoot.toString() + ";");
      processBindings(jsonMapper, root);
      // Contains all the defaults
      final ConfigurationItem[] loadedItems = jsonMapper.reader(ConfigurationItem[].class).readValue(configRoot);
      // Contains all the defaults
      final TreeSet<ConfigurationItem> items = new TreeSet<ConfigurationItem>(Arrays.asList(loadedItems));
      
      Map<String, ConfigurationItem> tmpItems = new HashMap<String, ConfigurationItem>(items.size());
      for(Iterator<ConfigurationItem> iter = items.iterator(); iter.hasNext();) {
        ConfigurationItem item = iter.next();
        if(tmpItems.containsKey(item.getKey())) throw new RuntimeException("opentsdb.conf.json contains duplicate key: [" + item.getKey() + "]");
        if(tmpItems.containsKey(item.getClOption())) throw new RuntimeException("opentsdb.conf.json contains duplicate clOption: [" + item.getClOption() + "]");
        tmpItems.put(item.getKey(), item);
        tmpItems.put(item.getClOption(), item);
//        if("BOOL".equals(item.getMeta())) iter.remove();
      }
      defaultConfItems = Collections.unmodifiableMap(tmpItems);
      LOG.debug("Loaded [{}] Configuration Items from opentsdb.conf.json", items.size());
      if(LOG.isDebugEnabled()) {
        StringBuilder b = new StringBuilder("Configs:");
        for(ConfigurationItem ci: items) {
          b.append("\n\t").append(ci.toString());
        }
        b.append("\n");
        LOG.debug(b.toString());
      }
      for(ConfigurationItem ci: items) {
        LOG.debug("Processing CI [{}]", ci.getKey());
        if(ci.meta!=null) {
          argp.addOption(ci.clOption, ci.meta, ci.description);
          LOG.debug("Registered Meta ArgP cl:[{}], meta:[{}]", ci.clOption, ci.meta); 
          if("default".equals(ci.help)) dargp.addOption(ci.clOption, ci.meta, ci.description);
        } else {
          argp.addOption(ci.clOption, ci.description);
          LOG.debug("Registered No Meta ArgP cl:[{}]", ci.clOption);
          if("default".equals(ci.help)) dargp.addOption(ci.clOption, ci.description);
        }
        if(!configItemsByKey.add(ci)) {
          throw new RuntimeException("Duplicate configuration key [" + ci.key + "] in opentsdb.conf.json. Programmer Error.");
        }
        if(!configItemsByCl.add(ci)) {
          throw new RuntimeException("Duplicate configuration command line option [" + ci.clOption + "] in opentsdb.conf.json. Programmer Error.");
        }       
        if(ci.getDefaultValue()!=null && !ci.getDefaultValue().trim().isEmpty()) {
          ci.setValue(processConfigValue(ci.getDefaultValue()));                
          config.overrideConfig(ci.key, processConfigValue(ci.getValue()));
        }
      }
      nonOptionArgs = applyArgs(args);
      loadExternalConfigs(items);
      config = new Config(config);
      
    } catch (Exception ex) {
      if(ex instanceof IllegalArgumentException) {
        throw (IllegalArgumentException)ex;
      }
      throw new RuntimeException("Failed to read opentsdb.conf.json", ex);
    } finally {
      if(is!=null) try { is.close(); } catch (Exception x) { /* No Op */ }
    }
  }
  
  /**
   * Loads an externally defined configuration and/or a configuration include 
   * @param source The existing configuration items
   */
  protected void loadExternalConfigs(final Set<ConfigurationItem> source) {
    ConfigurationItem include = getConfigurationItemByKey(source, "tsd.core.config");
    if(include!=null && include.getValue()!=null) {
      loadConfigSource(this, include.getValue());
    }
    include = getConfigurationItemByKey(source, "tsd.core.config.include");
    if(include!=null && include.getValue()!=null) {
      loadConfigSource(this, include.getValue());
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
        ci.setValue(processConfigValue(value));
      }
      c.overrideConfig(key, processConfigValue(value));     
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
        HttpURLConnection conn = (HttpURLConnection)connection;
        conn.setConnectTimeout(5000);
        conn.setReadTimeout(5000);        
      }
      is = connection.getInputStream();
      return loadConfig(url.toString(), is);     
    } catch (Exception ex) {
      ex.printStackTrace(System.err);
      throw new IllegalArgumentException("Failed to load configuration from [" + url + "]", ex);
    }finally {
      if(is!=null) try { is.close(); } catch (Exception ex) { /* No Op */ }
    }
  }
  
  
  /**
   * <p>Title: NoLoadConfig</p>
   * <p>Description: A {@link Config} override that does not trigger {@link Config#loadStaticVariables()} when {@link Config#overrideConfig(String, String)} is called.</p> 
   */
  private static class NoLoadConfig extends Config {
    /**
     * {@inheritDoc}
     * @see net.opentsdb.utils.Config#overrideConfig(java.lang.String, java.lang.String)
     */
    @Override
    public void overrideConfig(final String property, final String value) {
      this.properties.put(property, value);
    }
  }

  
  /**
   * Parses the command line arguments, and where the options are recognized config items, the value is validated, then applied to the config
   * @param clargs The command line arguments
   * @return The un-applied command line arguments
   */
  public String[] applyArgs(String[] clargs) {
    LOG.debug("Applying Command Line Args {}", Arrays.toString(clargs));
    final List<String> nonFlagArgs = new ArrayList<String>(Arrays.asList(clargs));
    extractAndApplyFlags(nonFlagArgs);
    
    String[] args = nonFlagArgs.toArray(new String[0]);
    String[] nonArgs = argp.parse(args);
    LOG.debug("Applying Command Line ArgP {}", argp);
    LOG.debug("configItemsByCl Keys: [{}]", configItemsByCl.toString());
    for(Map.Entry<String, String> entry: argp.getParsed().entrySet()) {
      String key = entry.getKey(), value = entry.getValue();
      ConfigurationItem citem = getConfigurationItemByClOpt(key);
      LOG.debug("Loaded CI for command line option [{}]: Found:{}", key, citem!=null);
      if("BOOL".equals(citem.getMeta())) {        
        citem.setValue(value!=null ? value : "true");
      } else {
        if(value!=null) {
          citem.setValue(processConfigValue(value));              
        }
      }
//      log("CL Override [%s] --> [%s]", citem.getKey(), citem.getValue());
      config.overrideConfig(citem.getKey(), citem.getValue());                    
    }
    return nonArgs;
  }
  
  private void extractAndApplyFlags(final List<String> args) {
    for(Iterator<String> iter = args.iterator(); iter.hasNext();) {
      String arg = iter.next();
      if(!arg.startsWith("--")) continue;
      ConfigurationItem ci = getConfigurationItemByClOpt(arg);
      if(ci==null) continue;
      if(ci.isBool()) {
        LOG.debug("Enabling flag [{}]", ci.getClOption());
        config.overrideConfig(ci.getKey(), "true");
        iter.remove();
      }
    }
  }
  
  private ConfigurationItem getConfigurationItemByKey(final Set<ConfigurationItem> source, final String key) {
    for(final ConfigurationItem ci: source) {
      if(ci.getKey().equals(key)) return ci;
    }
    return null;
  }
  
  /**
   * Returns the {@link ConfigurationItem} with the passed key
   * @param key The key of the item to fetch
   * @return The matching ConfigurationItem or null if one was not found
   */
  public ConfigurationItem getConfigurationItem(final String key) {
    if(key==null || key.trim().isEmpty()) throw new IllegalArgumentException("The passed key was null or empty");
    return getConfigurationItemByKey(configItemsByKey, key);
  }
  
  /**
   * Returns the {@link ConfigurationItem} with the passed cl-option
   * @param clopt The cl-option of the item to fetch
   * @return The matching ConfigurationItem or null if one was not found
   */
  public ConfigurationItem getConfigurationItemByClOpt(final String clopt) {
    if(clopt==null || clopt.trim().isEmpty()) throw new IllegalArgumentException("The passed cl-opt was null or empty");
    for(final ConfigurationItem ci: configItemsByCl) {
      if(ci.getClOption().equals(clopt)) return ci;
    }
    return null;

  }
  
  
  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuilder b = new StringBuilder();
    for(ConfigurationItem ci: configItemsByKey) {
      b.append(ci.toString()).append("\n");
    }
    return b.toString();
  }
  
  /**
   * Returns a default usage banner with optional prefixed messages, one per line.
   * @param msgs The optional message
   * @return the formatted usage banner
   */
  public String getDefaultUsage(String...msgs) {
    StringBuilder b = new StringBuilder("\n");
    for(String msg: msgs) {
      b.append(msg).append("\n");
    }
    b.append(dargp.usage());
    return b.toString();
  }

  /**
   * Returns an extended usage banner with optional prefixed messages, one per line.
   * @param msgs The optional message
   * @return the formatted usage banner
   */
  public String getExtendedUsage(String...msgs) {
    StringBuilder b = new StringBuilder("\n");
    for(String msg: msgs) {
      b.append(msg).append("\n");
    }
    b.append(argp.usage());
    return b.toString();
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
   * Performs sys-prop and js evals on the passed value
   * @param text The value to process
   * @return the processed value
   */
  public static String processConfigValue(CharSequence text) {
    final String pv = evaluate(tokenReplaceSysProps(text));
    return (pv==null || pv.trim().isEmpty()) ? null : pv;
  }
  
  /**
   * Replaces all matched tokens with the matching system property value or a configured default
   * @param text The text to process
   * @return The substituted string
   */
  public static String tokenReplaceSysProps(CharSequence text) {
    if(text==null) return null;
    Matcher m = SYS_PROP_PATTERN.matcher(text);
    StringBuffer ret = new StringBuffer();
    while(m.find()) {
      String replacement = decodeToken(m.group(1), m.group(2)==null ? "<null>" : m.group(2));
      if(replacement==null) {
        throw new IllegalArgumentException("Failed to fill in SystemProperties for expression with no default [" + text + "]");
      }
      if(IS_WINDOWS) {
        replacement = replacement.replace(File.separatorChar , '/');
      }
      m.appendReplacement(ret, replacement);
    }
    m.appendTail(ret);
    return ret.toString();
  }
  
  /**
   * Evaluates JS expressions defines as configuration values
   * @param text The value of a configuration item to evaluate for JS expressions
   * @return The passed value with any embedded JS expressions evaluated and replaced
   */
  public static String evaluate(CharSequence text) {
    if(text==null) return null;
    Matcher m = JS_PATTERN.matcher(text);
    StringBuffer ret = new StringBuffer();
    final boolean isNas = scriptEngine.getFactory().getEngineName().toLowerCase().contains("nashorn");
    while(m.find()) {
      String source = (isNas ? 
          "load(\"nashorn:mozilla_compat.js\");\nimportPackage(java.lang); "
          : 
          "\nimportPackage(java.lang); "
          )
          +  m.group(1);
      try {
        Object obj = scriptEngine.eval(source, bindings);
        if(obj!=null) {
          //log("Evaled [%s] --> [%s]", source, obj);
          m.appendReplacement(ret, obj.toString());
        } else {
          m.appendReplacement(ret, "");
        }
      } catch (Exception ex) {
        ex.printStackTrace(System.err);
        throw new IllegalArgumentException("Failed to evaluate expression [" + text + "]");
      }
    }
    m.appendTail(ret);
    return ret.toString();
  }
  
  /**
   * Attempts to decode the passed dot delimited as a system property, and if not found, attempts a decode as an 
   * environmental variable, replacing the dots with underscores. e.g. for the key: <b><code>buffer.size.max</b></code>,
   * a system property named <b><code>buffer.size.max</b></code> will be looked up, and then an environmental variable
   * named <b><code>buffer.size.max</b></code> will be looked up.
   * @param key The dot delimited key to decode
   * @param defaultValue The default value returned if neither source can decode the key
   * @return the decoded value or the default value if neither source can decode the key
   */
  public static String decodeToken(String key, String defaultValue) {
    String value = System.getProperty(key, System.getenv(key.replace('.', '_')));
    return value!=null ? value : defaultValue;
  }
  
  
  /**
   * <p>Title: ConfigurationItem</p>
   * <p>Description: A container class for deserialized configuration items from <b><code>opentsdb.conf.json</code></b>.</p> 
   */
  public static class ConfigurationItem implements Comparable<ConfigurationItem> {
    /** The internal configuration key */
    @JsonProperty("key")
    protected String key;
    /** The command line option key that maps to this item */
    @JsonProperty("cl-option")
    protected String clOption;
    /** The original value, loaded from opentsdb.conf.json, and never overwritten */
    @JsonProperty("defaultValue")
    protected String defaultValue;
    /** A description of the configuration item */
    @JsonProperty("description")
    protected String description;
    /** The command line help level at which this item will be displayed ('default' or 'extended') */
    @JsonProperty("help")
    protected String help;
    /** The meta symbol representing the type of value expected for a parameterized command line arg */
    @JsonProperty("meta")
    protected String meta;
    
    /** The decoded or overriden value */   
    protected String value;
    
    /**
     * Creates a new ConfigurationItem
     */
    public ConfigurationItem() {}
    
    /**
     * Creates a new ConfigurationItem
     * @param key The internal configuration key
     * @param clOption The command line option key that maps to this item
     * @param defaultValue The original value, loaded from opentsdb.conf.json, and never overwritten 
     * @param description A description of the configuration item
     * @param help The command line help level at which this item will be displayed ('default' or 'extended')
     * @param meta The meta symbol representing the type of value expected for a parameterized command line arg
     */
    public ConfigurationItem(String key, String clOption,
        String defaultValue, String description, String help,
        String meta) {
      this.key = key;
      this.clOption = clOption;
      this.defaultValue = defaultValue;
      this.description = description;
      this.help = help;
      this.meta = meta;
    }
    
    /**
     * Validates the value
     */
    public void validate() {
      if(meta!=null && value!=null) {
        ConfigMetaType.byName(meta, key).validate(this); 
      }
    }
    
    
    
    /**
     * Returns a descriptive name with the cl option and key
     * @return a descriptive name
     */
    public String getName() {
      return String.format("cl: %s, key: %s", clOption, key);
    }
    
    

    /**
     * Returns the item key name
     * @return the itemName
     */
    public String getKey() {
      return key;
    }

    /**
     * Returns the command line option mapping to this item
     * @return the clOption
     */
    public String getClOption() {
      return clOption;
    }

    /**
     * Returns the item current value
     * @return the value
     */
    public String getValue() {
      return value!=null ? value : defaultValue;
    }
    
    /**
     * Indicates if this ConfigurationItem is a <code>BOOL</code> meta type
     * @return true if this ConfigurationItem is a <code>BOOL</code> meta type, false otherwise
     */
    public boolean isBool() {
      return "BOOL".equals(this.meta);
    }

    
    /**
     * Sets a new value for this item
     * @param newValue The new value
     */
    public void setValue(final String newValue) {
      final String currValue = newValue;
      value = newValue.trim();
      try {
        validate();
      } catch (IllegalArgumentException ex) {
        value = currValue;
        throw ex;
      }     
    }

    /**
     * Returns the original raw value loaded from opentsdb.conf.json
     * @return the original raw value
     */
    public String getDefaultValue() {
      return defaultValue;
    }
    
    /**
     * Returns the item description
     * @return the description
     */
    public String getDescription() {
      return description;
    }

    /**
     * Returns the help level for this option
     * @return the help
     */
    public String getHelp() {
      return help;
    }

    /**
     * Returns the meta symbol
     * @return the meta
     */
    public String getMeta() {
      return meta;
    }

    /**
     * {@inheritDoc}
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
      return String
          .format("ConfigurationItem [key=%s, clOption=%s, value=%s, description=%s, help=%s, meta=%s, defaultValue=%s]",
              key, clOption, value, description, help,
              meta, defaultValue);
    }

    /**
     * {@inheritDoc}
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((key == null) ? 0 : key.hashCode());
      return result;
    }

    /**
     * {@inheritDoc}
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null)
        return false;
      if (getClass() != obj.getClass())
        return false;
      ConfigurationItem other = (ConfigurationItem) obj;
      if (key == null) {
        if (other.key != null)
          return false;
      } else if (!key.equals(other.key))
        return false;
      return true;
    }

    /**
     * <p>Sorts {@link ConfigurationItem}s by the underlying {@link ConfigMetaType}</p>
     * {@inheritDoc}
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(final ConfigurationItem other) {
      if(this==other) return 0;
      if((other.meta==null || other.meta.isEmpty()) && (meta==null || meta.isEmpty())) {
        return this.key.compareTo(other.key);
      }
      if(other.meta==null || other.meta.isEmpty()) {
        return 1;
      }
      if(meta==null || meta.isEmpty()) {
        return -1;
      }
      
      final ConfigMetaType otherType = ConfigMetaType.byName(other.meta, other.key); 
      final ConfigMetaType thisType = ConfigMetaType.byName(meta, key);
      int c = thisType.compareTo(otherType);
      if(c==0) {
        c = this.key.compareTo(other.key);
      }
      return c;
    }
  }


  /**
   * Returns the command line argument processor
   * @return the argp
   */
  public ArgP getArgp() {
    return argp;
  }

  /**
   * Returns the command line argument holder for default options
   * @return the command line argument holder 
   */
  public ArgP getDargp() {
    return dargp;
  }

  /**
   * Returns the TSDB config instance
   * @return the config
   */
  public Config getConfig() {
    return config;
  }

  /**
   * Returns the non config option arguments
   * @return the non config option arguments
   */
  public String[] getNonOptionArgs() {
    return nonOptionArgs;
  } 
  
  /**
   * Returns the default {@link ConfigurationItem} for the passed name
   * which can be the item's key or cl-option
   * @param name The item's key or cl-option
   * @return The named ConfigurationItem or null if one was not found
   */
  public ConfigurationItem getDefaultItem(final String name) {
    if(name==null) throw new IllegalArgumentException("The passed name was null");
    return defaultConfItems.get(name);
  }
  
  
  /**
   * Determines if the passed key is contained in the non option args
   * @param nonOptionKey The non option key to check for
   * @return true if the passed key is present, false otherwise
   */
  public boolean hasNonOption(String nonOptionKey) {
    if(nonOptionArgs==null || nonOptionArgs.length==0 || nonOptionKey==null || nonOptionKey.trim().isEmpty()) return false;
    return Arrays.binarySearch(nonOptionArgs, nonOptionKey) >= 0;
  }
  
  /**
   * Indicates if the passed arg was a command line option
   * @param arg The arg to test for
   * @return true if the passed arg was a command line option, false otherwise
   */
  public boolean isClArg(final String arg) {
    return commandLineArgs.contains(arg);
  }

  /**
   * Checks the <b><source>opentsdb.conf.json</source></b> document to see if it has a <b><source>bindings</source></b> segment
   * which contains JS statements to evaluate which will prime variables used by the configuration. 
   * @param jsonMapper The JSON mapper
   * @param root The root <b><source>opentsdb.conf.json</source></b> document 
   */
  protected void processBindings(ObjectMapper jsonMapper, JsonNode root) {
    String script = null;
    try {
      if(root.has("bindings")) {
        JsonNode bindingsNode = root.get("bindings");
        if(bindingsNode.isArray()) {
          String[] jsLines = jsonMapper.reader(String[].class).readValue(bindingsNode);
          StringBuilder b = new StringBuilder();
          for(String s: jsLines) {
            b.append(s).append("\n");
          }
          script = b.toString();
          scriptEngine.eval(script);
          LOG.debug("Successfully evaluated [{}] lines of JS in bindings", jsLines.length);
        }
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException("Failed to evaluate opentsdb.conf.json javascript binding [" + script + "]", ex);
    }
  }
  
  
}