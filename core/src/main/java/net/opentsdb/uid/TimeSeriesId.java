package net.opentsdb.uid;

import java.util.List;
import javax.annotation.Nonnull;

/**
 * A general representation of time series IDs.
 *
 * <p>Note: You should not use implementations of this interface as an argument, rather you should
 * use the components that make them up (metric and tags). This interface is intended to be used by
 * implementations of {@link net.opentsdb.storage.TsdbStore TsdbStores} to hide their representation
 * of time series IDs and expose them in a general interface. The canonical use-case is for return
 * values on store implementations.
 */
public abstract class TimeSeriesId {
  /**
   * The metric behind this time series ID.
   */
  @Nonnull
  public abstract LabelId metric();

  /**
   * The tags behind this time series ID.
   */
  @Nonnull
  public abstract List<LabelId> tags();
}
