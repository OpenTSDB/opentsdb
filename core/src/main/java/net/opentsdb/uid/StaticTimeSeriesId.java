package net.opentsdb.uid;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * An implementation of {@link TimeSeriesId} that is built up by the plain metric
 * and tags.
 *
 * <p>Note: This is here for legacy reasons and to aid migration. Everything that accepts instances
 * of this as an argument should be refactored to accept the metric and tags separately instead
 */
@Deprecated
public class StaticTimeSeriesId extends TimeSeriesId {
  private final LabelId metric;
  private final List<LabelId> tags;

  public StaticTimeSeriesId(final LabelId metric,
                            final List<LabelId> tags) {
    this.metric = metric;
    this.tags = tags;
  }

  @Nonnull
  @Override
  public LabelId metric() {
    return metric;
  }

  @Nonnull
  @Override
  public List<LabelId> tags() {
    return tags;
  }
}
