package net.opentsdb.utils;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Created by santhosh.r on 25/04/16.
 *
 * Based on the number of rows scanned by a given query blocks any further calls to that same metric/tag combination
 * for a specified amount of time. Currently blacklisting is supported only for query based on metric and tag name
 * and not on direct querying using TSUID, which is not a common use case for cosmos.
 */
public class BlacklistManager {
  private static final Logger logger = LoggerFactory.getLogger(BlacklistManager.class);

  private static boolean isReactiveBlacklistingEnabled = false;

  private static int rowCountThresholdForBlackList = 100000;

  private static int blockingTimeInSeconds = 600;

  private static Cache<String, Boolean> metricNameCache = CacheBuilder.newBuilder().expireAfterWrite(blockingTimeInSeconds, TimeUnit.SECONDS).build();

  public static void initBlockListConfiguration(boolean isReactiveBlacklistingEnabled, int rowCountThreshold, int blockingTimeInSeconds) {
    BlacklistManager.isReactiveBlacklistingEnabled = isReactiveBlacklistingEnabled;
    if (isReactiveBlacklistingEnabled) {
      BlacklistManager.rowCountThresholdForBlackList = rowCountThreshold;
      BlacklistManager.blockingTimeInSeconds = blockingTimeInSeconds;
      metricNameCache = CacheBuilder.newBuilder().expireAfterWrite(blockingTimeInSeconds, TimeUnit.SECONDS).build();
    }
  }

  public static void checkAndAddToBlacklist(String metricName, Map<String, String> tags, int nRowsScanned) {
    if (isReactiveBlacklistingEnabled && (metricName != null) && (nRowsScanned > rowCountThresholdForBlackList)) {
      String metricNameKey = getMetricNameKey(metricName, tags);
      metricNameCache.put(metricNameKey, true);
      logger.info("Metric " + metricNameKey + " is blacklisted for " + blockingTimeInSeconds + " seconds");
    }
  }

  public static List<String> getAllBlacklistedMetrics() {
    List<String> metricsNameList = new ArrayList<String>();
    if (isReactiveBlacklistingEnabled) {
      metricsNameList.addAll(metricNameCache.asMap().keySet());
    }
    return metricsNameList;
  }

  public static boolean isBlacklisted(String metricName, Map<String, String> tags) {
    if (isReactiveBlacklistingEnabled && (metricName != null)) {
      String metricNameKey = getMetricNameKey(metricName, tags);
      if (metricNameCache.getIfPresent(metricNameKey) != null) {
        return true;
      }
    }
    return false;
  }

  private static String getMetricNameKey(String metricName, Map<String, String> tags) {
    SortedMap<String, String> sortedTags = new TreeMap<String, String>();
    sortedTags.putAll(tags);
    StringBuilder blacklistingKey = new StringBuilder();
    blacklistingKey.append(metricName);
    blacklistingKey.append("[");
    int index = 0;
    for (Map.Entry<String, String> pair : tags.entrySet()) {
      if(index != 0) {
        blacklistingKey.append(",");
      }
      blacklistingKey.append(pair.getKey());
      blacklistingKey.append("=");
      blacklistingKey.append(pair.getValue());
      index++;
    }
    blacklistingKey.append("]");
    return blacklistingKey.toString();
  }
}
