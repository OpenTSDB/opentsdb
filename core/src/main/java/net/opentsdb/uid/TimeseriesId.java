package net.opentsdb.uid;

import javax.annotation.Nonnull;
import java.util.List;

/**
 * A general representation of timeseries IDs.
 *
 * Note: You should not use implementations of this interface as an argument,
 * rather you should use the components that make them up (metric and tags).
 * This interface is inteded to be used by implementations of {@link
 * net.opentsdb.storage.TsdbStore TsdbStores} to hide their representation of
 * timeseries IDs and expose them in a general interface. The canonical use-case
 * is for return values on store implementations.
 */
public abstract class TimeseriesId {
  /**
   * The metric behind this timeseries ID.
   */
  @Nonnull
  public abstract LabelId metric();

  /**
   * The tags behind this timeseries ID.
   */
  @Nonnull
  public abstract List<LabelId> tags();
}
