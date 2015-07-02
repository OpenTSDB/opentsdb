package net.opentsdb.stats;

import static com.google.common.base.Preconditions.checkNotNull;

import com.codahale.metrics.Gauge;
import com.google.common.cache.Cache;

/**
 * A metrics gauge that measures the hit rate on a Guava cache.
 */
public class CacheHitRateGauge implements Gauge<Double> {
  private final Cache<?, ?> cache;

  public CacheHitRateGauge(final Cache<?, ?> cache) {
    this.cache = checkNotNull(cache);
  }

  @Override
  public Double getValue() {
    return cache.stats().hitRate();
  }
}
