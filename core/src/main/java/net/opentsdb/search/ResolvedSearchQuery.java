package net.opentsdb.search;

import net.opentsdb.uid.LabelId;
import net.opentsdb.utils.Pair;

import java.util.SortedSet;

public class ResolvedSearchQuery {
  private final LabelId metric;
  private final SortedSet<Pair<LabelId,LabelId>> tags;

  public ResolvedSearchQuery(final LabelId metric,
                             final SortedSet<Pair<LabelId,LabelId>> tags) {
    this.metric = metric;
    this.tags = tags;
  }

  public LabelId getMetric() {
    return metric;
  }

  public SortedSet<Pair<LabelId,LabelId>> getTags() {
    return tags;
  }
}
