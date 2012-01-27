package net.opentsdb.core;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A simple configuration class consisting of key/value pairs. It's loosely
 * based off of the Hadoop Configuration class and Tsuna's ArgP classes in that
 * it will cast results with a provided default value. All application settings
 * should use this interface to store and retrieve values.
 * <p>
 * loadConfig() should be called at the start of the application and it will
 * search default locations for config files or it will try to load a user
 * supplied file. The config file format is a standard java.properties file
 * with key=value pairs and the pound sign signifying comments.
 */
public final class Configuration {
  private static final Logger LOG = LoggerFactory
      .getLogger(Configuration.class);

  /** A list of key/values loaded from the configuration file */
  private volatile static Properties props = new Properties();

  /** A list of file locations, provided by the user or loads defaults */
  private static ArrayList<String> file_locations = null;

  /**
   * Attempts to load a configuration file. If the user called Load(String file)
   * with a file, it will try to load only that file. Otherwise it will attempt
   * to load the file from default locations.
   * @return True if a config file was loaded, false if no file was loaded
   */
  public static final boolean loadConfig() {
    if (file_locations == null) {
      file_locations = new ArrayList<String>();

      // search locally first
      file_locations.add("opentsdb.conf");

      // add default locations based on OS
      if (System.getProperty("os.name").contains("Windows")) {
        file_locations.add("C:\\program files\\opentsdb\\opentsdb.conf");
      } else {
        file_locations.add("/etc/opentsdb/opentsdb.conf");
      }
    }

    // loop and load until we get something
    for (String file : file_locations) {
      try {
        props.load(new FileInputStream(file));
        LOG.info("Loaded configuration file [" + file + "]");

        // clear the file_locations so we can load again if necessary
        file_locations.clear();
        file_locations = null;

        return true;
      } catch (FileNotFoundException e) {
        LOG.debug("Unable to load configuration file [" + file + "]");
      } catch (IOException e) {
        LOG.debug("Unable to load configuration file [" + file + "]");
      }
    }

    // couldn't load anything
    LOG.warn("Unable to load any of the configuration files");
    return false;
  }

  /**
   * Attempts to load a configuration file specified by the user
   * @param file The full or relative path to a configuration file
   * @return True if a config file was loaded, false if no file was loaded
   */
  public static final boolean loadConfig(String file) {
    if (file.length() > 0) {
      file_locations = new ArrayList<String>();
      file_locations.add(file);
    }

    return Configuration.loadConfig();
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as a string
   */
  public static final String getString(final String key,
      final String defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    return val;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as an int
   */
  public static final int getInt(final String key, final int defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      return Integer.parseInt(val);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to an int");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an int
   */
  public static final int getInt(final String key, final int defaultValue,
      final int min, final int max) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      final int temp = Integer.parseInt(val);

      // check min/max
      if (temp < min) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
        return defaultValue;
      }
      if (temp > max) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
        return defaultValue;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to an int");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as an short
   */
  public static final short getShort(final String key, final short defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      return Short.parseShort(val);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to a short");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an short
   */
  public static final short getShort(final String key,
      final short defaultValue, final short min, final short max) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      final short temp = Short.parseShort(val);

      // check min/max
      if (temp < min) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
        return defaultValue;
      }
      if (temp > max) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
        return defaultValue;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to an short");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as a long
   */
  public static final long getLong(final String key, final long defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      return Long.parseLong(val);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to a long");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an long
   */
  public static final long getLong(final String key, final long defaultValue,
      final long min, final long max) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      final long temp = Long.parseLong(val);

      // check min/max
      if (temp < min) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
        return defaultValue;
      }
      if (temp > max) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
        return defaultValue;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to an long");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as a float
   */
  public static final float getFloat(final String key, final float defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      return Float.parseFloat(val);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to a float");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @param min A minimum value for the parameter
   * @param max A maximum value for the parameter
   * @return The configuration value as an float
   */
  public static final float getFloat(final String key,
      final float defaultValue, final float min, final float max) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      final float temp = Float.parseFloat(val);

      // check min/max
      if (temp < min) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was less than the minimum [" + min + "]");
        return defaultValue;
      }
      if (temp > max) {
        LOG.warn("Key [" + key + "] value [" + temp
            + "] was greater than the max [" + max + "]");
        return defaultValue;
      }
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to an float");
    }
    return defaultValue;
  }

  /**
   * Attempts to retrieve a configuration value given the key
   * @param key Key to search for
   * @param defaultValue The default value to return if the key could not be
   *          found
   * @return The configuration value as a boolean
   */
  public static final boolean getBoolean(final String key,
      final boolean defaultValue) {
    String val = props.getProperty(key);
    if (val == null)
      return defaultValue;
    try {
      return Boolean.parseBoolean(val);
    } catch (NumberFormatException e) {
      LOG.warn("Unable to convert key [" + key + "] value [" + val
          + "] to a boolean");
    }
    return defaultValue;
  }

  /**
   * Sets the value of a key in the configuration properties list
   * @param key The name of the configuration key to modify
   * @param value The value to store for the key
   */
  public static final void setConfig(final String key, final String value) {
    if (key.isEmpty()) {
      LOG.warn("Key provided is empty");
      return;
    }
    props.setProperty(key, value);
  }
}
