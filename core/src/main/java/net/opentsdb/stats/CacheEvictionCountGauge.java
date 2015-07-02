package net.opentsdb.stats;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.Gauge;
import com.google.common.cache.Cache;

/**
 * A metrics gauge that measures the eviction count on a Guava cache.
 */
public class CacheEvictionCountGauge implements Gauge<Long> {
  private final Cache<?, ?> cache;

  public CacheEvictionCountGauge(final Cache<?, ?> cache) {
    this.cache = checkNotNull(cache);
  }

  @Override
  public Long getValue() {
    return cache.stats().evictionCount();
  }
}
