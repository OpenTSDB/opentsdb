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
package net.opentsdb.uid;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import com.google.common.annotations.VisibleForTesting;
import com.stumbleupon.async.Deferred;

import net.opentsdb.core.TSDB;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.UniqueId.UniqueIdType;
import net.opentsdb.utils.Config;

/**
 * A UID filter implementation using regular expression based whitelists.
 * Multiple regular expressions can be provided in the configuration file with
 * a configurable delimiter. Each expression is compiled into a list per UID
 * type and when a new UID passes through the filter, each expression in the
 * list is compared to make sure the name satisfies all expressions.
 */
public class UniqueIdWhitelistFilter extends UniqueIdFilterPlugin {

  /** Default delimiter */
  public static final String DEFAULT_REGEX_DELIMITER = ",";
  
  /** Lists of patterns for each type. */
  private List<Pattern> metric_patterns;
  private List<Pattern> tagk_patterns;
  private List<Pattern> tagv_patterns;
  
  /** Counters for tracking stats */
  private final AtomicLong metrics_rejected = new AtomicLong();
  private final AtomicLong metrics_allowed = new AtomicLong();
  private final AtomicLong tagks_rejected = new AtomicLong();
  private final AtomicLong tagks_allowed = new AtomicLong();
  private final AtomicLong tagvs_rejected = new AtomicLong();
  private final AtomicLong tagvs_allowed = new AtomicLong();
  
  @Override
  public void initialize(final TSDB tsdb) {
    final Config config = tsdb.getConfig();
    String delimiter = config.getString("tsd.uidfilter.whitelist.delimiter");
    if (delimiter == null) {
      delimiter = DEFAULT_REGEX_DELIMITER;
    }
    
    String raw = config.getString("tsd.uidfilter.whitelist.metric_patterns");
    if (raw != null) {
      final String[] splits = raw.split(delimiter);
      metric_patterns = new ArrayList<Pattern>(splits.length);
      for (final String pattern : splits) {
        try {
          metric_patterns.add(Pattern.compile(pattern));
        } catch (PatternSyntaxException e) {
          throw new IllegalArgumentException("The metric whitelist pattern [" + 
              pattern + "] does not compile.", e);
        }
      }
    }
    
    raw = config.getString("tsd.uidfilter.whitelist.tagk_patterns");
    if (raw != null) {
      final String[] splits = raw.split(delimiter);
      tagk_patterns = new ArrayList<Pattern>(splits.length);
      for (final String pattern : splits) {
        try {
          tagk_patterns.add(Pattern.compile(pattern));
        } catch (PatternSyntaxException e) {
          throw new IllegalArgumentException("The tagk whitelist pattern [" + 
              pattern + "] does not compile.", e);
        }
      }
    }
    
    raw = config.getString("tsd.uidfilter.whitelist.tagv_patterns");
    if (raw != null) {
      final String[] splits = raw.split(delimiter);
      tagv_patterns = new ArrayList<Pattern>(splits.length);
      for (final String pattern : splits) {
        try {
          tagv_patterns.add(Pattern.compile(pattern));
        } catch (PatternSyntaxException e) {
          throw new IllegalArgumentException("The tagv whitelist pattern [" + 
              pattern + "] does not compile.", e);
        }
      }
    }
  }

  @Override
  public Deferred<Object> shutdown() {
    return Deferred.fromResult(null);
  }

  @Override
  public String version() {
    return "2.3.0";
  }

  @Override
  public void collectStats(final StatsCollector collector) {
    collector.record("uid.filter.whitelist.accepted", metrics_allowed.get(), 
        "type=metrics");
    collector.record("uid.filter.whitelist.accepted", tagks_allowed.get(), 
        "type=tagk");
    collector.record("uid.filter.whitelist.accepted", tagvs_allowed.get(), 
        "type=tagv");
    collector.record("uid.filter.whitelist.rejected", metrics_rejected.get(), 
        "type=metrics");
    collector.record("uid.filter.whitelist.rejected", tagks_rejected.get(), 
        "type=tagk");
    collector.record("uid.filter.whitelist.rejected", tagvs_rejected.get(), 
        "type=tagv");
  }

  @Override
  public Deferred<Boolean> allowUIDAssignment(
      final UniqueIdType type, 
      final String value,
      final String metric, 
      final Map<String, String> tags) {
    
    switch (type) {
      case METRIC:
        if (metric_patterns != null) {
          for (final Pattern pattern : metric_patterns) {
            if (!pattern.matcher(value).find()) {
              metrics_rejected.incrementAndGet();
              return Deferred.fromResult(false);
            }
          }
        }
        metrics_allowed.incrementAndGet();
        break;
        
      case TAGK:
        if (tagk_patterns != null) {
          for (final Pattern pattern : tagk_patterns) {
            if (!pattern.matcher(value).find()) {
              tagks_rejected.incrementAndGet();
              return Deferred.fromResult(false);
            }
          }
        }
        tagks_allowed.incrementAndGet();
        break;
        
      case TAGV:
        if (tagv_patterns != null) {
          for (final Pattern pattern : tagv_patterns) {
            if (!pattern.matcher(value).find()) {
              tagvs_rejected.incrementAndGet();
              return Deferred.fromResult(false);
            }
          }
        }
        tagvs_allowed.incrementAndGet();
        break;
    }
    
    // all patterns passed, yay!
    return Deferred.fromResult(true);
  }

  @Override
  public boolean fillterUIDAssignments() {
    return true;
  }

  @VisibleForTesting
  List<Pattern> metricPatterns() {
    return metric_patterns;
  }
  
  @VisibleForTesting
  List<Pattern> tagkPatterns() {
    return tagk_patterns;
  }
  
  @VisibleForTesting
  List<Pattern> tagvPatterns() {
    return tagv_patterns;
  }
}
