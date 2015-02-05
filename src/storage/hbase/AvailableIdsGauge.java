package net.opentsdb.storage.hbase;

import com.codahale.metrics.DerivativeGauge;

/**
 * A {@link com.codahale.metrics.DerivativeGauge} that measures the number of
 * available IDs. It uses the {@link UsedIdsGauge} and the ID width information
 * to calculate it.
 */
public class AvailableIdsGauge extends DerivativeGauge<Long,Long> {
  private final int idWidth;

  protected AvailableIdsGauge(final UsedIdsGauge usedIdsGauge,
                              final int idWidth) {
    super(usedIdsGauge);
    this.idWidth = idWidth;
  }

  @Override
  protected Long transform(final Long usedIds) {
    return maxPossibleId(idWidth) - usedIds;
  }

  /**
   * The largest possible ID given the number of bytes the IDs are represented
   * on.
   */
  static long maxPossibleId(final int idWidth) {
    return (1 << idWidth * Byte.SIZE) - 1;
  }
}
