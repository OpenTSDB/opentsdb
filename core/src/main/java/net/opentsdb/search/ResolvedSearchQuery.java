package net.opentsdb.search;

import net.opentsdb.utils.ByteArrayPair;

import java.util.SortedSet;

public class ResolvedSearchQuery {
  private final byte[] metric;
  private final SortedSet<ByteArrayPair> tags;

  public ResolvedSearchQuery(final byte[] metric,
                             final SortedSet<ByteArrayPair> tags) {
    this.metric = metric;
    this.tags = tags;
  }

  public byte[] getMetric() {
    return metric;
  }

  public SortedSet<ByteArrayPair> getTags() {
    return tags;
  }
}
