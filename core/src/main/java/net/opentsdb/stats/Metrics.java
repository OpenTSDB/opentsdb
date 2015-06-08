package net.opentsdb.stats;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Joiner;

/**
 * Instead of juggling a registry around directly we use this class. It might be
 * really useful in the future to store {@link com.codahale.metrics.Metric}s on.
 */
public class Metrics {
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

  private Metrics() {}
}
