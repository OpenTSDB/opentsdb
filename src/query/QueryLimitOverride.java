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
package net.opentsdb.query;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timeout;
import org.jboss.netty.util.TimerTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.common.io.Files;

import net.opentsdb.core.Const;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.JSON;

/**
 * A class used for numeric query overrides using regular expression matching.
 * The class will attempt to load from a locally cached file on construction if
 * a file is given. Each time one or more items are added or removed to the list
 * the file will be updated if it was configured.
 * This class is thread safe.
 * 
 * @since 2.4
 */
public class QueryLimitOverride implements TimerTask {
  private static final Logger LOG = LoggerFactory.getLogger(QueryLimitOverride.class);
  
  /** Used for deserialization the proper object. Without this the hash codes
  /* won't function properly as Jackson may just create a hash map for our simple
  /* object. */
  public static TypeReference<HashSet<QueryLimitOverrideItem>> TR_OVERRIDES =
      new TypeReference<HashSet<QueryLimitOverrideItem>>() {};
  
  /** The list of overrides */
  
  /** Keyed on the raw regex so we can update objects properly. */    
  private final Map<String, QueryLimitOverrideItem> overrides;
  
  /** The default byte limit to use if the string didn't match. */
  private long default_byte_limit;
  
  /** The default data points limit to use if the string didn't match. */
  private long default_data_points_limit;
  
  /** The optional file location to read/write */
  private String file_location;
  
  /** How often, in seconds, to reload from the file */
  private int reload_interval;
  
  /** A timer for refreshing data from the config */
  private HashedWheelTimer timer;
  
  /**
   * Default ctor. If a file location is given, it will be read on construction
   * and reloaded every interval seconds. Note that if there is a problem reading
   * or parsing the config file (when set) the ctor will continue loading with 
   * the defaults and if an interval is set, will attempt to reload at a later
   * time.
   * @param tsdb The TSDB to which we belong.
   * @throws IllegalArgumentException if the default limits are less than zero.
   */
  public QueryLimitOverride(final TSDB tsdb) {
    overrides = Maps.newConcurrentMap();
    default_byte_limit = tsdb.getConfig().getLong("tsd.query.limits.bytes.default");
    default_data_points_limit = tsdb.getConfig()
        .getLong("tsd.query.limits.data_points.default");
    if (tsdb.getConfig().hasProperty("tsd.query.limits.overrides.interval")) {
      reload_interval = tsdb.getConfig().getInt("tsd.query.limits.overrides.interval");
    } else {
      reload_interval = 0;
    }
    file_location = tsdb.getConfig().getString("tsd.query.limits.overrides.config");
    timer = (HashedWheelTimer) tsdb.getTimer();
    if (default_byte_limit < 0) {
      throw new IllegalArgumentException("The default byte limit cannot be negative");
    }
    if (default_data_points_limit < 0) {
      throw new IllegalArgumentException("The default data points limit cannot"
          + " be negative");
    }
    
    if (!Strings.isNullOrEmpty(file_location)) {
      loadFromFile();
      if (reload_interval > 0) {
        timer.newTimeout(this, reload_interval, TimeUnit.SECONDS);
      }
    }
  }
  
  /** @return The default byte limit used when a match fails */
  public long getDefaultByteLimit() {
    return default_byte_limit;
  }
  
  /** @return The default data points limit used when a match fails */
  public long getDefaultDataPointsLimit() {
    return default_data_points_limit;
  }
  
  /**
   * Iterates over the list of overrides and return the first that matches or
   * the default if no match is found.
   * NOTE: The set of expressions is not sorted so if more than one regex
   * matches the string, the result is indeterministic. 
   * If the metric is null or empty, the default limit is returned.
   * @param metric The string to match
   * @return The matched or default limit.
   */
  public synchronized long getByteLimit(final String metric) {
    if (metric == null || metric.isEmpty()) {
      return default_byte_limit;
    }
    for (final QueryLimitOverrideItem item : overrides.values()) {
      if (item.matches(metric)) {
        return item.getByteLimit();
      }
    }
    return default_byte_limit;
  }
  
  /**
   * Iterates over the list of overrides and return the first that matches or
   * the default if no match is found.
   * NOTE: The set of expressions is not sorted so if more than one regex
   * matches the string, the result is indeterministic. 
   * If the metric is null or empty, the default limit is returned.
   * @param metric The string to match
   * @return The matched or default limit.
   */
  public synchronized long getDataPointLimit(final String metric) {
    if (metric == null || metric.isEmpty()) {
      return default_data_points_limit;
    }
    for (final QueryLimitOverrideItem item : overrides.values()) {
      if (item.matches(metric)) {
        return item.getDataPointsLimit();
      }
    }
    return default_data_points_limit;
  }
  
  /** @return An unmodifiable collection of the items. WARNING: Don't modify the items! 
   * They are not duplicates (right now)*/
  public Collection<QueryLimitOverrideItem> getLimits() {
    return Collections.unmodifiableCollection(overrides.values());
  }
  
  @Override
  public String toString() {
    return JSON.serializeToString(this);
  }
  
  /** @param timeout The timeout reference. */
  @Override
  public void run(final Timeout timeout) {
    try {
      loadFromFile();
    } catch (RuntimeException e) {
      LOG.error("Failed to read cache file on auto reload: " + this, e);
    } finally {
      timer.newTimeout(this, reload_interval, TimeUnit.SECONDS);
    }
  }
  
  /**
   * Attempts to load the file from disk
   */
  private void loadFromFile() {
    // load from disk if the caller gave us a file
    if (file_location != null && !file_location.isEmpty()) {
      final File file = new File(file_location);
      if (!file.exists()) {
        LOG.warn("Query override file " + file_location + " does not exist");
        return;
      }
      try {
        final String raw_json = Files.toString(file, Const.UTF8_CHARSET);
        if (raw_json != null && !raw_json.isEmpty()) {
          final Set<QueryLimitOverrideItem> cached_items = 
              JSON.parseToObject(raw_json, TR_OVERRIDES);
          
          // iterate so we only change bits that are different.
          for (final QueryLimitOverrideItem override : cached_items) {
            QueryLimitOverrideItem existing = overrides.get(override.getRegex());
            if (existing == null || !existing.equals(override)) {
              overrides.put(override.getRegex(), override);
            }
          }
          
          // reverse quadratic, woot! Ugly but if the limit file is so big that
          // this takes over 60 seconds or starts blocking queries on modifications
          // to the map then something is really wrong.
          final Iterator<Entry<String, QueryLimitOverrideItem>> iterator = 
              overrides.entrySet().iterator();
          while (iterator.hasNext()) {
            final Entry<String, QueryLimitOverrideItem> entry = iterator.next();
            boolean matched = false;
            for (final QueryLimitOverrideItem override : cached_items) {
              if (override.getRegex().equals(entry.getKey())) {
                matched = true;
                break;
              }
            }
            if (!matched) {
              iterator.remove();
            }
          }
        }
        LOG.info("Successfully loaded query overrides: " + this);
      } catch (Exception e) {
        LOG.error("Failed to read cache file for query limit override: " + 
            this, e);
      }
    }
  }
  
  /** A simple class for ser/des of the items along with some validation */
  public static class QueryLimitOverrideItem {
   
    /** The regular expression provided by the user */
    private String regex;
    
    /** A compiled pattern for speedier operation */
    private Pattern pattern;
    
    /** The byte limit to use when matched */
    private long byte_limit = 0;
    
    /** The data points limit to use when matched */
    private long data_points_limit = 0;
    
    @Override
    public int hashCode() {
      return Objects.hashCode(regex, byte_limit, data_points_limit);
    }
    
    @Override
    public boolean equals(final Object item) {
      if (item == this) {
        return true;
      }
      if (item == null) {
        return false;
      }
      return regex.equals(((QueryLimitOverrideItem)item).regex) &&
          byte_limit == ((QueryLimitOverrideItem)item).byte_limit &&
          data_points_limit == ((QueryLimitOverrideItem)item).data_points_limit;
    }
  
    /** @return The regular expression */
    public String getRegex() {
      return regex;
    }

    /** @param regex The regular expression
     * @throws PatternSyntaxException if the regex can't be compiled */
    public void setRegex(final String regex) {
      this.regex = regex;
      pattern = Pattern.compile(regex);
    }

    /** @return The byte limit to use for this override */
    public long getByteLimit() {
      return byte_limit;
    }

    /** @param byte_limit The byte limit to use for this override */
    public void setByteLimit(final long byte_limit) {
      this.byte_limit = byte_limit;
    }
    
    /** @return The data points limit to use for this override. */
    public long getDataPointsLimit() {
      return data_points_limit;
    }
    
    /** @param data_points_limit The data points limit to use for this override. */
    public void setDataPointsLimit(final long data_points_limit) {
      this.data_points_limit = data_points_limit;
    }
    
    /** @param string The string to match. 
     * @return true if the string matches this regex pattern, false if not.
     * If the string is null or empty we return false. */
    public boolean matches(final String string) {
      if (string == null || string.isEmpty()) {
        return false;
      }
      if (pattern == null || regex == null || regex.isEmpty()) {
        return false;
      }
      return pattern.matcher(string).find();
    }
  
    /** @throws IllegalArgumentException if the limit is less than zero or
     * the regular expression is null or empty */
    public void validate() {
      if (byte_limit < 0) {
        throw new IllegalArgumentException("The byte limit must be 0 or greater");
      }
      if (data_points_limit < 0) {
        throw new IllegalArgumentException("The data points limit must be 0 or greater");
      }
      if (regex == null || regex.isEmpty()) {
        throw new IllegalArgumentException("The regex cannot be empty or null");
      }
    }
  }
}
