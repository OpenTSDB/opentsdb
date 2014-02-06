// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
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
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
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
 * @author Whitehead (nwhitehead AT heliosdev DOT org)
 * <p><code>net.opentsdb.tools.ConfigArgP</code></p>
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
	protected final Config config;
	/** The raw configuration items loaded from the json file keyed by the item key */
	protected final Map<String, ConfigurationItem> configItemsByKey = new HashMap<String, ConfigurationItem>();
	/** The raw configuration items loaded from the json file keyed by the cl-option */
	protected final Map<String, ConfigurationItem> configItemsByCl = new HashMap<String, ConfigurationItem>();
	
	/** The regex pattern to perform a substitution for <b><pre><code>${&lt;sysprop&gt;:&lt;default&gt;}</code></pre></b> patterns in strings */
	public static final Pattern SYS_PROP_PATTERN = Pattern.compile("\\$\\{(.*?)(?::(.*?))??\\}");
	/** The regex pattern to perform a substitution for <b><pre><code>$[&lt;javascript snippet&gt;]</code></pre></b> patterns in strings */
	public static final Pattern JS_PATTERN = Pattern.compile("\\$\\[(.*?)\\]", Pattern.MULTILINE);

	/** Indicates if we're on Windows, in which case the SysProp handling needs a few tweaks */
	public static final boolean IS_WINDOWS = System.getProperty("os.name").toLowerCase().contains("windows");
	/** The JavaScript Engine to interpret <b><code>$[&lt;javascript snippet&gt;]</code></b> values */
	protected static final ScriptEngine scriptEngine = new ScriptEngineManager().getEngineByExtension("js");
	
	

	static {
		// Initialize js bindings
		Bindings bindings = scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE);
		bindings.put("bindings", bindings);
	}
	
	/**
	 * Creates a new ConfigArgP
	 * @param args The command line arguments
	 */
	public ConfigArgP(String...args) {
		InputStream is = null;
		
		try {
			config = new Config(false);
			is = ConfigArgP.class.getClassLoader().getResourceAsStream("opentsdb.conf.json");
			ObjectMapper jsonMapper = new ObjectMapper();
			JsonNode root = jsonMapper.reader().readTree(is);
			JsonNode configRoot = root.get("config-items");
			scriptEngine.eval("var config = " + configRoot.toString() + ";");
			processBindings(jsonMapper, root);
			ConfigurationItem[] items = jsonMapper.reader(ConfigurationItem[].class).readValue(configRoot);
			LOG.info("Loaded [{}] Configuration Items from opentsdb.conf.json", items.length);
			for(ConfigurationItem ci: items) {
				LOG.debug("Processing CI [{}]", ci.getKey());
				if(ci.meta!=null) {
					argp.addOption(ci.clOption, ci.meta, ci.description);
					if("default".equals(ci.help)) dargp.addOption(ci.clOption, ci.meta, ci.description);
				} else {
					argp.addOption(ci.clOption, ci.description);
					if("default".equals(ci.help)) dargp.addOption(ci.clOption, ci.description);
				}
				if(configItemsByKey.put(ci.key, ci)!=null) {
					throw new RuntimeException("Duplicate configuration key [" + ci.key + "] in opentsdb.conf.json. Programmer Error.");
				}
				if(configItemsByCl.put(ci.clOption, ci)!=null) {
					throw new RuntimeException("Duplicate configuration command line option [" + ci.clOption + "] in opentsdb.conf.json. Programmer Error.");
				}				
				if(ci.getDefaultValue()!=null) {
					ci.setValue(processConfigValue(ci.getDefaultValue()));								
					config.overrideConfig(ci.key, processConfigValue(ci.getValue()));
				}
			}
			// find --config and --include-config in argp and load into config 
			//		validate
			//argp.parse(args);
			nonOptionArgs = applyArgs(args);
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
	 * Parses the command line arguments, and where the options are recognized config items, the value is validated, then applied to the config
	 * @param args The command line arguments
	 * @return The un-applied command line arguments
	 */
	public String[] applyArgs(String[] args) {
		LOG.debug("Applying Command Line Args {}", Arrays.toString(args));
		String[] nonArgs = argp.parse(args);
		LOG.debug("Applying Command Line ArgP {}", argp);
		LOG.debug("configItemsByCl Keys: [{}]", configItemsByCl.keySet());
		for(Map.Entry<String, String> entry: argp.getParsed().entrySet()) {
			String key = entry.getKey(), value = entry.getValue();
			ConfigurationItem citem = configItemsByCl.get(key);
			LOG.debug("Loaded CI for command line option [{}]: Found:{}", key, citem!=null);
			if(citem.getMeta()==null) {				
				citem.setValue(value!=null ? value : "true");
			} else {
				if(value!=null) {
					citem.setValue(processConfigValue(value));							
				}
			}
//			log("CL Override [%s] --> [%s]", citem.getKey(), citem.getValue());
			config.overrideConfig(citem.getKey(), citem.getValue());										
		}
		return nonArgs;
	}
	
	/**
	 * {@inheritDoc}
	 */
	public String toString() {
		StringBuilder b = new StringBuilder();
		for(ConfigurationItem ci: configItemsByKey.values()) {
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
	
	public static void main(String args[]) {
		log("JSON Config Test");
		
		ConfigArgP cp = new ConfigArgP();
		log(cp.toString());
		log("=======");
		log(cp.getDefaultUsage());
	}
	
	public static void log(String format, Object...args) {
		System.out.println(String.format(format, args));
	}
	
	/**
	 * Performs sys-prop and js evals on the passed value
	 * @param text The value to process
	 * @return the processed value
	 */
	public static String processConfigValue(CharSequence text) {
		return evaluate(tokenReplaceSysProps(text));
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
		while(m.find()) {
			String source = "importPackage(java.lang); " +  m.group(1);
			try {
				
				Object obj = scriptEngine.eval(source);
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
	 * @author Whitehead (nwhitehead AT heliosdev DOT org)
	 * <p><code>net.opentsdb.tools.ConfigArp.ConfigurationItem</code></p>
	 */
	public static class ConfigurationItem {
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
			super();
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
				ConfigMetaType.byName(meta).validate(this); 
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
		 * Sets a new value for this item
		 * @param newValue The new value
		 */
		public void setValue(String newValue) {
			final String currValue = newValue;
			value = newValue;
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
	}


	/**
	 * Returns the
	 * @return the argp
	 */
	public ArgP getArgp() {
		return argp;
	}

	/**
	 * Returns the
	 * @return the dargp
	 */
	public ArgP getDargp() {
		return dargp;
	}

	/**
	 * Returns the
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
	 * Determines if the passed key is contained in the non option args
	 * @param nonOptionKey The non option key to check for
	 * @return true if the passed key is present, false otherwise
	 */
	public boolean hasNonOption(String nonOptionKey) {
		if(nonOptionArgs==null || nonOptionArgs.length==0 || nonOptionKey==null || nonOptionKey.trim().isEmpty()) return false;
		return Arrays.binarySearch(nonOptionArgs, nonOptionKey) >= 0;
	}

	/**
	 * Checks the <b><source>opentsdb.conf.json</source></b> document to see if it has a <b><source>bindings</source></b> segment
	 * which contains JS statements to evaluate which will prime variables used by the configuration. 
	 * @param jsonMapper The JSON mapper
	 * @param root The root <b><source>opentsdb.conf.json</source></b> document 
	 */
	protected void processBindings(ObjectMapper jsonMapper, JsonNode root) {
		try {
			if(root.has("bindings")) {
				JsonNode bindingsNode = root.get("bindings");
				if(bindingsNode.isArray()) {
					String[] jsLines = jsonMapper.reader(String[].class).readValue(bindingsNode);
					StringBuilder b = new StringBuilder();
					for(String s: jsLines) {
						b.append(s).append("\n");
					}
					scriptEngine.eval(b.toString());
					LOG.info("Successfully evaluated [{}] lines of JS in bindings", jsLines.length);
				}
			}
		} catch (Exception ex) {
			throw new IllegalArgumentException("Failed to evaluate opentsdb.conf.json bindings", ex);
		}
	}
	
	
}
