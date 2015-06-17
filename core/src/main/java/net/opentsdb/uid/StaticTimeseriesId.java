package net.opentsdb.uid;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * An implementation of {@link net.opentsdb.uid.TimeseriesId} that is built up by the plain metric
 * and tags.
 *
 * <p>Note: This is here for legacy reasons and to aid migration. Everything that accepts instances
 * of this as an argument should be refactored to accept the metric and tags separately instead
 */
@Deprecated
public class StaticTimeseriesId extends TimeseriesId {
  private final LabelId metric;
  private final List<LabelId> tags;

  public StaticTimeseriesId(final LabelId metric,
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
