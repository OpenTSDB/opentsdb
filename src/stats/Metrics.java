package net.opentsdb.stats;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Joiner;

import javax.inject.Inject;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Instead of juggling a registry around directly we use this class. It might be
 * really useful in the future to store {@link com.codahale.metrics.Metric}s on.
 */
public class Metrics {
  /**
   * The registry that is associated with this Metrics instance.
   */
  private final MetricRegistry registry;

  /**
   * Create a new name for the metrics library with the given metric and tags.
   */
  public static String name(String metric, Tag... tags) {
    StringBuilder sb = new StringBuilder()
        .append(metric);

    if (tags.length > 0) {
      sb.append(":");
      Joiner.on(",").appendTo(sb, tags);
    }

    return sb.toString();
  }

  /**
   * Create a new tag with the given tag key and tag value. You should only rely
   * on this method for anything else other than for use with the name method
   * above.
   */
  public static Tag tag(final String key, final String value) {
    return new Tag(key, value);
  }

  /**
   * Inner class that describes tags for use with the name method above. You
   * should not rely on this class for anything else.
   */
  public static class Tag {
    public final String key;
    public final String value;

    public Tag(final String key, final String value) {
      this.key = checkNotNull(key);
      this.value = checkNotNull(value);
    }

    @Override
    public String toString() {
      return key + "=" + value;
    }
  }

  /**
   * Create a new metrics instance with the given registry.
   */
  @Inject
  public Metrics(MetricRegistry registry) {
    this.registry = checkNotNull(registry);
  }

  /**
   * Get the registry that is associated with this metrics instance.
   */
  public MetricRegistry getRegistry() {
    return registry;
  }
}
